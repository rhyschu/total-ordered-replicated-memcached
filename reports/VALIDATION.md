# Verification — November 9, 2026

This report applies only to the single fixed-sequencer total-order multicast implementation now in the repository. Earlier consensus-backed measurements have been removed; the original course percentages are not reused as measurements of this implementation.

## Completed checks

- `go vet ./...`: passed.
- `go test -race -count=1 -timeout=180s ./...`: passed. [Captured output](test-race.txt).
- Server and client executable builds: passed.
- `python scripts/verify_local.py`: passed using three separate compiled server processes, real TCP/HTTP and isolated durable stores. [Captured result](local-smoke.json).
- Compose YAML: parsed and checked for three server services, three dedicated Memcached services and independent node volumes. This is a structural check, not a container runtime test.

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

## Not completed in this environment

- Docker image build and the container smoke script were **not run**: the Docker engine was unavailable. Starting the installed Docker Desktop through elevated execution was not executed because automatic approval could not refresh its revoked login token. No approval safeguard was bypassed.
- A real external Memcached service was unavailable, so `MEMCACHED_ADDR` was not set. The external-cache branch remains to be exercised with a running service; this run establishes durable fallback during cache connection failure, not measured cache reuse against an actual daemon.

The local three-process smoke test passed sequencer/replica outage rejection, recovery after member restart, recovery after all processes restart, stable retry receipts, and the command-line client. It does not replace container or external-Memcached integration verification.
