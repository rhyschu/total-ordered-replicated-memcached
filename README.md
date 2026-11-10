# Totally Ordered Replicated Memcached

A replicated key/value cache built around a fixed sequencer and total-order multicast. Concurrent writes receive a shared batch sequence, replicas apply that sequence in order, and reads use a read-index barrier before returning a value. The implementation builds on an Amoeba-inspired course project rather than delegating ordering to a consensus framework.

There is one server implementation, one client, and one replication protocol.

## Architecture

```text
                       fixed sequencer (any configured ID)
                      /         |          \
client -> HTTP RPC -> forward -> batch + sequence
                               /     |     \
                         ordered TCP multicast
                           /         |        \
                        replica   replica   replica
                           |         |         |
                        durable ordered state + optional local Memcached

GET at any replica -> sequencer read barrier -> catch up -> local read
```

- **Total order:** a single configured sequencer batches up to five writes, flushing after 2 ms when the batch is not full. Each batch has a monotonically increasing sequence and a hash linking it to the previous record. Every replica applies exactly that ordered prefix. Batch size and flush delay are configurable.
- **Transport:** logical multicast is implemented by TCP fan-out to the static members, not IP multicast. Sequencer and member IDs come from configuration; no operation assumes node 1.
- **Acknowledgments:** successful writes have been durably applied by **all configured replicas**. A missing replica causes an explicit error, not an acknowledgment of partial replication. A timeout can still have an unknown write outcome; retry with the same request ID.
- **Read barriers:** a replica contacts the sequencer, which synchronizes the configured replicas before returning its applied index. The reader waits for that index locally with a deadline. A missing key and an unavailable cluster are different outcomes.
- **Out-of-order delivery:** bounded buffering holds future sequence numbers until the gap is filled. Duplicate deliveries are harmless; conflicting history is rejected. A recovering replica fetches missing records from the sequencer during the next write or read barrier.
- **Durability and retries:** each replica atomically stores ordered records, values, applied index and retry receipts in bbolt. Identical write IDs return the original result; reusing an ID for a different command is rejected. The receipt window is the latest 10,000 distinct write IDs, not an unlimited exactly-once guarantee.
- **Memcached:** each replica can have a dedicated Memcached read cache. Durable ordered state remains authoritative, so eviction or a cache outage cannot become silent data loss. Content-derived keys and byte validation prevent an old cached value from replacing current data. This integration does not bypass read barriers or promise a cache speedup.
- **Bounded serving:** authenticated HTTP/TLS JSON-RPC, framed/authenticated peer TCP, deadlines, admission limits, and bounded command sizes. The HTTP client reuses connections and matches response IDs; there is no concurrent sharing of a raw encoder/decoder or unsafe hedged write.

## Project layout

```text
cmd/server/       server entry point, configuration and shutdown
cmd/client/       command-line RPC client
multicast/        fixed-sequencer ordering, batching, read barriers and durable state
rpcache/          HTTP API, bounded peer TCP transport and reusable Go client
scripts/          disposable Compose verification
reports/          measurements and their exact conditions
compose.yaml      three replicas, independent volumes and three local Memcached services
```

## Run

Requirements: Go 1.26 or newer; the pinned development/CI toolchain is Go 1.27.3. Docker Desktop/Engine with Compose is sufficient for the container deployment.

```ps
$env:RPCACHE_TOKEN = 'replace-with-a-random-secret-at-least-24-characters'
docker compose up -d --build
go run ./cmd/client -method Cache.Set -key example -value hello -id write-001
go run ./cmd/client -method Cache.Get -key example -id read-001
go run ./cmd/client -method Cache.Delete -key example -id delete-001
docker compose down
```

HTTP ports 8001–8003 bind only to loopback. Peer ports and Memcached are private to the Compose network. Each node owns a separate persistent volume; ordinary `down` preserves these volumes. `down --volumes` permanently deletes state and should only be used for disposable data.

Single-node development uses the same implementation, without replication fault tolerance:

```ps
$env:RPCACHE_TOKEN = 'replace-with-a-random-secret-at-least-24-characters'
go run ./cmd/server -id local -sequencer local -peer 127.0.0.1:9001 -http 127.0.0.1:8001 -data data/local '-peers={"local":"127.0.0.1:9001"}'
```

Add `-memcached 127.0.0.1:11211` for an optional local cache. Use `-tls-cert` and `-tls-key` for HTTP TLS. Store/rotate the token outside source control; all peers in a cluster use the same service token.

The persistent node ID, fixed sequencer and static membership are checked on startup. Do not repoint an existing node's data directory at another membership or promote a replica by changing flags. An incompatible database directory is rejected; use a fresh directory and retain any previous volumes until you have intentionally exported or retired their data.

## API

POST `/rpc` with `Authorization: Bearer <RPCACHE_TOKEN>`:

```json
{"jsonrpc":"2.0","id":"write-001","method":"Cache.Set","params":{"key":"example","value":"hello"}}
```

```json
{"jsonrpc":"2.0","id":"write-001","result":{"found":true,"revision":1}}
```

Methods are `Cache.Set`, `Cache.Get`, and `Cache.Delete`. A revision is a **batch sequence**, not a count of individual writes. Named parameters and nonempty string IDs are required; notifications and JSON-RPC batches are unsupported. `found:false` is a normal cache miss; unavailable replication is an error. Preserve a write ID after an ambiguous timeout and retry promptly inside the receipt window.

- GET `/livez`: process liveness.
- GET `/readyz`: read barrier succeeds with every configured replica; returns 503 otherwise.
- GET `/metrics`: authenticated Prometheus-style request/error counters.

Defaults: 128 KiB HTTP bodies, 16 KiB values, 200-byte non-whitespace keys, 128-byte IDs, 10,000 live keys, 128 queued writes, 128 in-flight HTTP requests and 128 peer handlers per node. Peer frames are capped at 2 MiB to accommodate a full 16-write batch even when JSON escaping expands the values. Reordering is limited to 128 records ahead. The journal stops accepting new batches at 100,000 records; it is not compacted automatically. Plan bounded workloads, disk monitoring and operator-managed retention rather than treating it as an unlimited database.

## Verify

```ps
go vet ./...
go test -race -count=1 -timeout=180s ./...
go build ./cmd/server ./cmd/client
$env:RPCACHE_REPORT = 'reports/local-load.json'
go test ./rpcache -run TestRPCLocalLoad -count=1 -v
python scripts/verify_compose.py
```

For an isolated local three-process deployment check without Docker, build the executables and run:

```ps
go build -o bin/server.exe ./cmd/server
go build -o bin/client.exe ./cmd/client
python scripts/verify_local.py
```

On Linux/macOS, omit `.exe`. The script creates private temporary stores, starts only its own processes, and removes its disposable state after verifying recovery.

The Compose script stops/restarts only this project's services and deletes its own unique test key; run it against disposable development state. Set `MEMCACHED_ADDR` to exercise a real external Memcached in the Go tests. Recorded measurements and their scope are in [reports/VALIDATION.md](reports/VALIDATION.md).

## Availability and deployment boundaries

This is a fixed-membership, crash-recovery service for a **trusted private network**. It does not elect a replacement sequencer. If the sequencer or any configured replica is unavailable, reads/writes fail until the original member returns and catches up. A lost sequencer disk requires operator recovery from backups; do not force-promote another node or bootstrap a divergent history.

Peer TCP authenticates a shared token but is not encrypted or individually authenticated. Keep it isolated; a compromised authorized peer is outside the fault model. Expose only TLS-protected RPC behind appropriate access controls. There is one service token, not per-user RBAC. A three-container deployment on one computer cannot survive failure of that computer. Use independent hosts/disks, backups and monitoring for operational deployments.

The original course measurements (89.66% versus 97.93% GET/client-log agreement) describe an April 2026 workload and remain in Git history/course materials. They are **not** benchmark results for the current implementation and do not establish general linearizability or availability. Current measurements must be reproduced with the checked-in tests.

## Reference

M. Frans Kaashoek, Andrew S. Tanenbaum, and Kees Verstoep. **Group Communication in Amoeba and its Applications.** *Distributed Systems Engineering* 1(1), 48–58 (1993). [doi:10.1088/0967-1846/1/1/006](https://doi.org/10.1088/0967-1846/1/1/006). The ordering design is inspired by fixed-sequencer total-order multicast; it is not the complete Amoeba membership/reformation protocol. Bibliographic entry: [references.bib](references.bib).
