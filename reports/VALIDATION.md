# Verification — November 9, 2026

This report applies only to the single fixed-sequencer total-order multicast implementation now in the repository. Earlier consensus-backed measurements have been removed; the original course percentages are not reused as measurements of this implementation.

## Completed checks

- `go vet ./...`: passed.
- `go test -race -count=1 -timeout=180s ./...`: passed. [Captured output](test-race.txt).
- Server and client executable builds: passed.
- `python scripts/verify_local.py`: passed using three separate compiled server processes, real TCP/HTTP and isolated durable stores. [Captured result](local-smoke.json).
- Docker image build: passed on Docker Engine 29.9.3, using the non-root runtime image.
- `python scripts/verify_compose.py`: passed with three server containers, three real Memcached containers and independent durable volumes. [Captured result](compose-smoke.json). The test checks cross-replica reads, unavailable-member rejection, member restart, cache outage fallback, all-server restart and durable retry receipts. Test containers were stopped and removed afterward; persistent volumes were retained.

Functional coverage includes ordered delivery after a gap, duplicate/conflicting record rejection, durable state and retry receipts, fixed-membership restart validation, sequencer loss, replica outage and catch-up, full cluster process restart, read-after-write across replicas, 50 concurrent writes to a shared key, actual write batching, partial-batch timer flush, authenticated/framed peer traffic, malformed/oversized input, HTTP authentication/admission, TLS requests, optional-cache failure, and ambiguous-write client retries. The race detector covers exercised paths rather than proving the absence of every possible race.

## Local load measurement

| Measure | Observed |
|---|---:|
| Concurrent client workers | 20 |
| SET/GET pairs | 500 |
| RPC requests | 1,000 |
| Failed or mismatched responses | 0 |
| Request-phase elapsed time | 1.4444 s |
| Aggregate request rate | 692.32 requests/s |
| Median RPC latency | 25.289 ms |
| P95 RPC latency | 46.405 ms |

[Raw measurement](local-load.json). Run timestamp: November 10, 2026 at 01:29:50 UTC, equivalent to November 9 at 20:29:50 EST. Toolchain: Go 1.27.3, Windows/amd64. Three real TCP replicas, bbolt durability, fixed sequencer, batch size 5 and flush delay 2 ms. Every acknowledged write waits for all configured replicas; reads include a sequencer fence. Each worker owns a separate key and checks its GET against its acknowledged SET. The distribution combines SET and GET timings.

Maximum-size values with JSON escaping pass an HTTP round trip, and a full 16-command batch fits and decodes within the bounded peer frame.

These timings exclude TLS, external Memcached, Docker and WAN traffic. Separate tests cover TLS and an unavailable cache backend. This is a single-host development workload, not a production throughput promise, a universal latency improvement or a formal history-based linearizability proof. It must not be compared directly with measurements of a different algorithm or test workload. [Source hashes](source-hashes.json) identify the working-tree implementation measured and tested.

## External cache integration

After Docker was restarted and its access check succeeded, the full race suite was rerun with `MEMCACHED_ADDR` pointing to a dedicated Memcached 1.6.39 container on a dynamically assigned loopback port. The external-cache test verifies that reads populate the actual daemon, that subsequent reads return the correct value, that deliberately corrupted cache bytes cannot override durable state, and that a later write does not return an old cached value. The temporary daemon was removed after testing. No cache latency improvement is claimed.

Container verification remains single-host. Multi-host partitions, WAN latency, long-duration soak tests and a formal linearizability checker were not exercised. Membership is static, and the fixed sequencer has no automatic failover.

## November 13, 2026: separate GET and SET latency

Repeated the original 20-worker, 500 SET/GET-pair workload five times on the current implementation. Each run created a fresh three-node cluster. Only measurement instrumentation changed; replication, persistence and client behavior were not modified. `go test ./...` passed before these dedicated runs.

| Method | Pooled requests | Median | P95 (nearest rank) |
|---|---:|---:|---:|
| GET | 2,500 | 16.1065 ms | 21.524 ms |
| SET | 2,500 | 40.771 ms | 49.495 ms |
| Combined | 5,000 | 25.055 ms | 45.348 ms |

All five trials passed with zero failed or mismatched responses. Per-method percentiles pool the raw samples; they are not averages of trial percentiles. The combined median remains close to the earlier 25.289 ms observation; this is repeatability evidence, not a claimed performance optimization.

Scope: Go 1.27.3 on Windows/amd64; real peer TCP and loopback HTTP, bbolt durability, batch size 5, flush 2 ms. Clients try the sequencer endpoint first, so these figures do not characterize follower-ingress latency. Runs have fresh state and no explicit warm-up; they exclude external Memcached, TLS, Docker and WAN. The 1,000 requests in each trial contain 500 calls of each method. SET and GET failures are checked separately from timing, and no samples were dropped.

[Pooled summary and trial metrics](latency-2026-11-13/summary.json), [raw per-method samples, trial 1](latency-2026-11-13/run-1.json), [source hashes](latency-2026-11-13/source-hashes.json). Trials 2–5 are stored alongside trial 1. Reproduce each trial with `RPCACHE_REPORT` set to a distinct `run-N.json` path and `go test -run '^TestRPCLocalLoad$' -count=1 -v ./rpcache`, then run `python scripts/summarize_latency.py reports/latency-2026-11-13` to pool all samples.
