# Replicated Cache RPC

A typed JSON-RPC service for an ordered, replicated key/value cache. It replaces the original fixed-sequencer experiment's serving path with quorum-committed writes, leader-gated reads, persistent recovery and bounded request handling. The original experiment remains available for comparison.

## Architecture

```text
Go client / JSON-RPC caller
        |
        v
authenticated HTTP/TLS API -> elected Raft leader -> majority log commit
        |                         |                     |
        |                    deterministic state     other voters
        |                         |
        +--- Get -> quorum check + apply barrier -> authoritative value
                                      |
                            optional Memcached read cache
```

- **RPC:** `Cache.Get`, `Cache.Set`, `Cache.Delete`; JSON-RPC 2.0 profile with mandatory string IDs, named parameters, no notifications or batch requests. The Go client reuses HTTP connections, validates response IDs and tries the configured endpoints with the same request ID.
- **Replication:** HashiCorp Raft supplies election, ordered log replication and majority commit. Followers reject reads/writes instead of silently returning stale results. A failed leader can be replaced when a majority remains. Without quorum, requests fail rather than claiming success.
- **Recovery:** BoltDB stores Raft logs and stable metadata. File snapshots persist values and retry receipts; restore and log replay recover state. Only a fresh cluster is bootstrapped, on one node, with its entire static membership.
- **Retries:** identical write IDs return their previous result. Reusing an ID for different input is rejected. Receipts survive snapshots/restart, but only the most recent 10,000 unique writes are retained. This is a bounded idempotency window, not unlimited exactly-once execution. A timeout may mean the write committed: retry the same ID promptly, not a new one.
- **Cache correctness:** the Raft state machine is authoritative. Memcached is optional acceleration, never the only copy of acknowledged data. Cache keys include a digest of the key and current value; returned bytes are checked. Eviction, invalidation, cache restart or cache outage cannot substitute a stale value or turn a committed key into a miss. Reads still require a reachable elected leader.
- **Resource controls:** 32 KiB RPC bodies, 16 KiB values, 200-byte keys, 10,000 live keys, 128 in-flight operations, bounded HTTP lifetimes and backend cache deadlines. Detached operations keep their admission slot until completion, so client disconnects cannot create unlimited workers. Capacity errors and overload are explicit.

## Run a local cluster

Requirements: Docker Desktop/Engine and Compose. Build/test without Docker requires Go 1.27.3 (minimum module language version is in `go.mod`).

Set a **random secret**, then start the three voters and an isolated Memcached service:

```ps
$env:RPCACHE_TOKEN = 'replace-with-a-random-secret-at-least-24-characters'
docker compose up -d --build
```

The RPC ports are published **only on loopback**: 8001, 8002, 8003. Raft and Memcached ports are private to the Compose network. Bootstrap runs only on n1; existing storage is reused after restart. Separate volumes keep each voter's logs and snapshots independent.

```ps
go run ./cmd/rpcctl -method Cache.Set -key example -value hello -id write-001
go run ./cmd/rpcctl -method Cache.Get -key example -id read-001
go run ./cmd/rpcctl -method Cache.Delete -key example -id delete-001
docker compose stop n1
go run ./cmd/rpcctl -method Cache.Get -key example -id read-002
docker compose up -d n1
docker compose down
```

Do not add `--volumes` to shutdown unless intentionally destroying all stored state. A majority must retain its durable data for normal recovery; do not bootstrap a new cluster on top of surviving members.

Direct development without containers:

```ps
$env:RPCACHE_TOKEN = 'replace-with-a-random-secret-at-least-24-characters'
go run ./cmd/rpcd -id n1 -raft 127.0.0.1:9001 -http 127.0.0.1:8001 -data data/n1 -bootstrap
```

This is a single-voter development deployment with no failover. Omit `-memcached` to use authoritative state alone; add `-memcached 127.0.0.1:11211` to enable the read cache.

## Wire contract

POST `/rpc` with `Authorization: Bearer <RPCACHE_TOKEN>` and JSON:

```json
{"jsonrpc":"2.0","id":"unique-write-id","method":"Cache.Set","params":{"key":"example","value":"hello"}}
```

```json
{"jsonrpc":"2.0","id":"unique-write-id","result":{"found":true,"revision":3}}
```

Revisions are Raft log indexes, not a contiguous write count. Get returns `found:false` for a missing key; an unavailable leader is an error, never a cache miss. Application errors use the JSON-RPC `error` field. HTTP 401 means invalid credentials; HTTP 429 means admission capacity is exhausted. Unknown methods, malformed schemas and invalid key/value limits are rejected before entering the replicated log. Stable string IDs are mandatory; rejected requests that cannot supply one use the empty string in the error response.

GET `/livez` reports process liveness. GET `/readyz` is successful only on a verified leader, suitable for a leader-aware load balancer. GET `/metrics` requires the bearer token and exports Prometheus-style request/error counters. Do not log bearer tokens, user values or TLS private keys.

## Verify and measure

```powershell
go vet ./...
go test -race -count=1 -timeout=180s ./...
go build ./cmd/rpcd ./cmd/rpcctl
```

Tests exercise real TCP Raft transports and temporary persistent stores: leader loss, loss of quorum, follower read rejection, snapshot/restart, durable idempotency, conflicting retry IDs, concurrent read-after-write, TLS requests, malformed/oversized input, authentication, backpressure, cancellation, ambiguous-write retry and cache outages. Set `MEMCACHED_ADDR` to run the external legacy wrapper check and actual cache reuse check. CI provides a Memcached service and enables the race detector.

For the reproducible **local** RPC load experiment:

```powershell
$env:RPCACHE_REPORT = 'reports/local-load.json'
go test ./rpcache -run TestRPCLocalLoad -count=1 -v
```

The fixture issues 500 SET/GET pairs from 20 concurrent clients, each using a separate key, through HTTP into three real Raft voters with durable BoltDB. Every GET is checked against its acknowledged write. Timings include RPC, quorum commit and read barriers, but exclude TLS, Memcached and WAN traffic. This is a same-host development measurement, not a throughput SLA or a proof covering all possible histories. [Recorded results and conditions](reports/VALIDATION.md).

## Deployment boundaries

The hardened path is `cmd/rpcd`; **do not deploy `cmd/server` or `cmd/client`**, which are preserved course experiments. This repository supplies a deployable private-network service and verification evidence, not a claim of audited public production use.

Before operating outside the local Compose topology:

1. Use `-tls-cert` and `-tls-key` (or a trusted TLS terminator) for RPC. Store and rotate `RPCACHE_TOKEN` outside source control. This version has one shared service token, not per-user RBAC.
2. Keep Raft transport and Memcached on a trusted, isolated network. The current Raft TCP transport is **not encrypted or authenticated**; Internet-facing Raft ports are unsupported. Native mTLS for peer traffic, hostile-peer protection and dynamic membership are not implemented.
3. Use independent persistent disks/hosts across failure domains. A three-container demo on one computer cannot tolerate failure of that computer. Back up volumes, monitor free disk/memory and test recovery from backups. The 10,000-key/value cap and bounded receipts are intentional, not an unlimited cache product.
4. Reads are leader-routed and perform a quorum verification plus apply barrier; they do not scale across arbitrary followers. Losing a majority sacrifices availability. This is not an implementation of the original Amoeba membership protocol and does not claim Byzantine fault tolerance.
5. Memcached lookup follows an authoritative state lookup and thus does not avoid quorum checks or promise a speedup. The optional cache integration demonstrates safe failure semantics; the load report does not attribute its performance to Memcached.

## Original experiment and provenance

`multicast/`, `memcached/wrapper.go`, `cmd/server/` and `cmd/client/` preserve the April 2026 course prototype. It explored Amoeba-inspired fixed-sequencer total-order multicast, read-index barriers and SET batching against asynchronous replication. The course report measured GET/client-log agreement from 89.66% to 97.93% with 50 clients. Those are **historical workload-specific figures**, not results for the new RPC path, not a formal linearizability test, and not production availability. The original election and transport code remains experimental and is not used by `rpcd`.

The RPC version uses HashiCorp Raft rather than claiming to have implemented consensus from scratch. Project code covers RPC contracts, replicated state transitions, idempotency, snapshot state, safe cache integration, client routing, resource admission, deployment and fault/load tests. Dependency versions are locked in `go.mod`/`go.sum`; dependency licenses remain with their respective authors.

## References

1. M. Frans Kaashoek, Andrew S. Tanenbaum, and Kees Verstoep. **Group Communication in Amoeba and its Applications**. *Distributed Systems Engineering* 1(1), 48–58 (1993). [doi:10.1088/0967-1846/1/1/006](https://doi.org/10.1088/0967-1846/1/1/006). Inspiration for the original fixed-sequencer experiment; not a claim to reproduce the complete protocol.
2. Diego Ongaro and John Ousterhout. **In Search of an Understandable Consensus Algorithm**. *USENIX ATC 2014*, 305–319. [Paper](https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro), [extended version](https://raft.github.io/raft.pdf). Consensus basis for the new replicated state machine.
3. HashiCorp. [Raft implementation](https://github.com/hashicorp/raft) and [raft-boltdb durable store](https://github.com/hashicorp/raft-boltdb). These libraries implement the underlying consensus/storage algorithms.

Machine-readable paper entries: [references.bib](references.bib).
