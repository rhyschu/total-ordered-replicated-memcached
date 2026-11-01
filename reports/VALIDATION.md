# Verification — November 1, 2026

## Gates completed

- `go vet ./...`: passed.
- `go test -race -count=1 -timeout=180s ./...`: passed with `MEMCACHED_ADDR=127.0.0.1:21212`, using a real isolated Memcached 1.6.39 container. [Test output](test-race.txt). The race detector covers exercised paths, not an audit of the preserved experimental server/client.
- `go build ./cmd/rpcd ./cmd/rpcctl`: passed.
- `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`: reported **No vulnerabilities found** on this run. This is a point-in-time reachable-code/database check, not a guarantee of absence of vulnerabilities.
- `docker compose config --quiet`, non-root image build and three-voter deployment: passed.
- Container smoke test: acknowledged value retained after elected leader shutdown; served the correct value with Memcached stopped; retained value and original retry receipt after all voter containers were removed and recreated with their existing volumes. [Raw smoke result](compose-smoke.json), [reproduction script](../scripts/verify_compose.py). The script deliberately stops/restarts this project's cluster and deletes its own test key; use disposable development state.

## Recorded local RPC load

| Measure | Observed |
|---|---:|
| Concurrent client workers | 20 |
| SET/GET pairs | 500 |
| RPC requests | 1,000 |
| Failed or mismatched responses | 0 |
| Elapsed request phase | 2.7295 s |
| Aggregate request rate | 366.37 requests/s |
| Median RPC latency | 49.550 ms |
| P95 RPC latency | 92.728 ms |

[Raw observations](local-load.json). Runtime: Go 1.27.3, Windows/amd64, real TCP transport between three voters, BoltDB persistence and HTTP RPC. Each worker owns a separate key and checks its GET value against its acknowledged SET; both methods are counted in the latency distribution. The test excludes leader election startup from elapsed time and includes quorum commit/read barriers. The HTTP client uses connection reuse. Raft batching is library-managed; this is not the original batch-size experiment.

This run omits Memcached and TLS to isolate the RPC/consensus path. Separate functional tests cover actual Memcached, cache failure and TLS. All voters share one development machine: the figures are not cross-host/WAN performance, production throughput, formal history-based linearizability verification, or 100% availability. A repeated test may vary with disk, antivirus, CPU and scheduler load. Zero mismatches describes these 1,000 observed requests only.

The resume uses the request count, concurrency and zero observed failures/mismatches. It does not reuse the old 89.66%/97.93% course measurements for the RPC implementation. [Source hashes](source-hashes.json) identify the measured implementation.
