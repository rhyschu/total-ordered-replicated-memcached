package rpcache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestRPCLocalLoad(t *testing.T) {
	nodes := cluster(t, 3)
	urls := []string{}
	for _, n := range nodes {
		s, _ := NewService(n, token, 128)
		server := httptest.NewServer(s.Handler())
		defer server.Close()
		urls = append(urls, server.URL)
	}
	c := NewClient(urls, token)
	defer c.HTTP.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var mu sync.Mutex
	latencies := []float64{}
	byMethod := map[string][]float64{"Cache.Set": {}, "Cache.Get": {}}
	failures := 0
	var wg sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < 20; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprintf("load-%d", worker)
			for step := 0; step < 25; step++ {
				value := fmt.Sprint(step)
				for _, method := range []string{"Cache.Set", "Cache.Get"} {
					began := time.Now()
					r, e := c.Call(ctx, Request{ID: fmt.Sprintf("load-%d-%d-%s", worker, step, method), Method: method, Params: Params{Key: key, Value: value}})
					elapsed := float64(time.Since(began).Microseconds()) / 1000
					mu.Lock()
					latencies = append(latencies, elapsed)
					byMethod[method] = append(byMethod[method], elapsed)
					if e != nil || !r.Found || (method == "Cache.Get" && r.Value != value) {
						failures++
					}
					mu.Unlock()
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	sort.Float64s(latencies)
	methodStats := map[string]any{}
	for method, samples := range byMethod {
		sort.Float64s(samples)
		median := (samples[(len(samples)-1)/2] + samples[len(samples)/2]) / 2
		methodStats[method] = map[string]any{
			"requests": len(samples), "median_ms": median,
			"p95_ms":              samples[(len(samples)*95-1)/100],
			"latencies_ms_sorted": samples,
		}
	}
	result := map[string]any{"timestamp_utc": time.Now().UTC().Format(time.RFC3339), "scope": "3 fixed-sequencer total-order multicast nodes; real framed TCP, per-node durable bbolt transactions, loopback HTTP RPC, 20 clients, 500 SET/GET pairs; batching=5, flush=2ms; all replicas acknowledge; no Memcached, TLS or WAN in timing", "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "clients": 20, "requests": len(latencies), "failures": failures, "elapsed_seconds": elapsed, "requests_per_second": float64(len(latencies)) / elapsed, "median_ms": latencies[len(latencies)/2], "p95_ms": latencies[(len(latencies)*95-1)/100]}
	result["by_method"] = methodStats
	result["measurement_notes"] = "Fresh three-node cluster per run; no warm-up; client tries sequencer endpoint first; per-method median averages the middle pair, P95 uses nearest rank; aggregate median retains original upper-middle definition. Timings include all completed calls; failures are counted separately."
	b, _ := json.MarshalIndent(result, "", "  ")
	t.Log(string(b))
	if path := os.Getenv("RPCACHE_REPORT"); path != "" {
		if !filepath.IsAbs(path) {
			path = filepath.Join("..", path)
		}
		if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(path, append(b, '\n'), 0644); e != nil {
			t.Fatal(e)
		}
	}
	if failures != 0 {
		t.Fatalf("%d failed or inconsistent RPC responses", failures)
	}
}
