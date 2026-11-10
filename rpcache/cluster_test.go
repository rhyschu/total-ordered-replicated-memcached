package rpcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"total-ordered-replicated-memcached/multicast"
)

const token = "ordered-cache-test-secret-at-least-24"

func address(t testing.TB) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func cluster(t testing.TB, count int) []*Node {
	t.Helper()
	peers := map[string]string{}
	for i := 0; i < count; i++ {
		peers[fmt.Sprint(i)] = address(t)
	}
	nodes := make([]*Node, count)
	for i := range nodes {
		n, e := Open(Config{ID: fmt.Sprint(i), Address: peers[fmt.Sprint(i)], Peers: peers, Sequencer: "0", Token: token, Directory: filepath.Join(t.TempDir(), "state")})
		if e != nil {
			t.Fatal(e)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				n.Close()
			}
		}
	})
	return nodes
}
func set(t testing.TB, n *Node, id, key, value string) Result {
	t.Helper()
	r, e := n.Write(Command{ID: id, Op: "Cache.Set", Key: key, Value: value}, 2*time.Second)
	if e != nil || r.Error != "" {
		t.Fatalf("write: %+v %v", r, e)
	}
	return r
}
func TestReplicationFollowerReadsAndSequencerLoss(t *testing.T) {
	nodes := cluster(t, 3)
	set(t, nodes[1], "first", "key", "value")
	for _, n := range nodes {
		r, e := n.Read("key", time.Second)
		if e != nil || r.Value != "value" {
			t.Fatalf("read: %+v %v", r, e)
		}
	}
	nodes[0].Close()
	started := time.Now()
	if _, e := nodes[1].Read("key", 200*time.Millisecond); e == nil {
		t.Fatal("stale read succeeded without sequencer")
	}
	if time.Since(started) > time.Second {
		t.Fatal("read deadline not respected")
	}
	if _, e := nodes[1].Write(Command{ID: "two", Op: "Cache.Set", Key: "key", Value: "new"}, 200*time.Millisecond); e == nil {
		t.Fatal("write succeeded without sequencer")
	}
}
func TestReplicaOutageCatchupAndRestart(t *testing.T) {
	nodes := cluster(t, 3)
	initial := set(t, nodes[0], "one", "key", "old")
	config := nodes[2].config
	nodes[2].Close()
	// Simulate a lost reply after the sequencer durably orders a write, leaving a
	// replica behind. The next fence must replay the missing ordered prefix.
	seq, hash, _ := nodes[0].State.Position()
	record := multicast.Record{Sequence: seq + 1, Previous: hash, Batch: []Command{{ID: "partial", Op: "Cache.Set", Key: "key", Value: "new"}}}
	record.Hash = record.Digest()
	if e := nodes[0].State.Deliver(record); e != nil {
		t.Fatal(e)
	}
	if _, e := nodes[0].Read("key", 300*time.Millisecond); e == nil {
		t.Fatal("read succeeded before all replicas caught up")
	}
	var e error
	nodes[2], e = Open(config)
	if e != nil {
		t.Fatal(e)
	}
	r, e := nodes[2].Read("key", time.Second)
	if e != nil || r.Value != "new" {
		t.Fatalf("catchup: %+v %v", r, e)
	}
	retry := set(t, nodes[1], "one", "key", "old")
	if retry.Revision != initial.Revision {
		t.Fatal("retry reapplied")
	}
	r, e = nodes[0].Read("key", time.Second)
	if e != nil || r.Value != "new" {
		t.Fatal("retry overwrote later write")
	}
	configs := make([]Config, len(nodes))
	for i, n := range nodes {
		configs[i] = n.config
		nodes[i] = nil
		if e = n.Close(); e != nil {
			t.Fatal(e)
		}
	}
	for i, c := range configs {
		nodes[i], e = Open(c)
		if e != nil {
			t.Fatal(e)
		}
	}
	for _, n := range nodes {
		r, e = n.Read("key", time.Second)
		if e != nil || r.Value != "new" {
			t.Fatal("restart lost ordered value", e)
		}
	}
	if r := set(t, nodes[2], "one", "key", "old"); r.Revision != initial.Revision {
		t.Fatal("restart lost original retry receipt")
	}
}
func TestDurableRetryAndMembershipLock(t *testing.T) {
	nodes := cluster(t, 1)
	n := nodes[0]
	config := n.config
	original := set(t, n, "once", "key", "value")
	n.Close()
	var e error
	nodes[0], e = Open(config)
	if e != nil {
		t.Fatal(e)
	}
	retry := set(t, nodes[0], "once", "key", "value")
	if retry.Revision != original.Revision {
		t.Fatal("lost durable idempotency")
	}
	r, e := nodes[0].Write(Command{ID: "once", Op: "Cache.Set", Key: "key", Value: "different"}, time.Second)
	if e != nil || r.Error == "" {
		t.Fatal("conflicting ID not rejected")
	}
}
func TestConcurrentReadAfterWrite(t *testing.T) {
	nodes := cluster(t, 3)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := nodes[i%3]
			key := fmt.Sprint(i)
			for j := 0; j < 10; j++ {
				v := fmt.Sprint(j)
				r, e := n.Write(Command{ID: fmt.Sprintf("%d-%d", i, j), Op: "Cache.Set", Key: key, Value: v}, 2*time.Second)
				if e != nil || r.Error != "" {
					t.Errorf("write: %+v %v", r, e)
					return
				}
				out, e := nodes[(i+1)%3].Read(key, 2*time.Second)
				if e != nil || out.Value != v {
					t.Errorf("cross-replica read: %+v %v", out, e)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestSharedKeyOrderAndBatchFlush(t *testing.T) {
	nodes := cluster(t, 3)
	const clients = 50
	start := make(chan struct{})
	revisions := make(chan uint64, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, e := nodes[i%3].Write(Command{ID: fmt.Sprintf("shared-%d", i), Op: "Cache.Set", Key: "shared", Value: fmt.Sprint(i)}, 3*time.Second)
			if e != nil || r.Error != "" {
				t.Errorf("write failed: %+v %v", r, e)
				return
			}
			revisions <- r.Revision
		}(i)
	}
	close(start)
	wg.Wait()
	close(revisions)
	seen := map[uint64]bool{}
	count := 0
	for r := range revisions {
		seen[r] = true
		count++
	}
	if count != clients || len(seen) >= clients {
		t.Fatalf("no actual batching: %d writes, %d batch revisions", count, len(seen))
	}
	first, e := nodes[0].Read("shared", time.Second)
	if e != nil {
		t.Fatal(e)
	}
	seq, hash, _ := nodes[0].State.Position()
	for _, n := range nodes {
		value, e := n.Read("shared", time.Second)
		if e != nil || value != first {
			t.Fatal("replicas diverged", e)
		}
		s, h, e := n.State.Position()
		if e != nil || s != seq || h != hash {
			t.Fatal("ordered prefix differs", e)
		}
	}
	begin := time.Now()
	set(t, nodes[2], "single-flush", "flush", "value")
	if time.Since(begin) > time.Second {
		t.Fatal("partial batch did not flush promptly")
	}
}
func rpc(t *testing.T, url, body, authorization string) (int, Response) {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/rpc", strings.NewReader(body))
	req.Header.Set("Authorization", authorization)
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	var out Response
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}
func TestHTTPValidationTLSAndBackpressure(t *testing.T) {
	nodes := cluster(t, 1)
	s, _ := NewService(nodes[0], token, 1)
	plain := httptest.NewServer(s.Handler())
	defer plain.Close()
	if status, _ := rpc(t, plain.URL, `{}`, ""); status != 401 {
		t.Fatal("missing auth accepted")
	}
	for _, body := range []string{`{}`, `[]`, `{"jsonrpc":"2.0","id":"bad","method":"unknown","params":{"key":"x"}}`, `{"jsonrpc":"2.0","id":"bad","method":"Cache.Set","params":{"key":"bad key"}}`, strings.Repeat("x", 40000)} {
		if _, r := rpc(t, plain.URL, body, "Bearer "+token); r.Error == nil {
			t.Fatalf("invalid body accepted: %.50s", body)
		}
	}
	s.slots <- struct{}{}
	if status, _ := rpc(t, plain.URL, `{}`, "Bearer "+token); status != 429 {
		t.Fatal("unbounded admission")
	}
	<-s.slots
	secure := httptest.NewTLSServer(s.Handler())
	defer secure.Close()
	req, _ := http.NewRequest("POST", secure.URL+"/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"tls","method":"Cache.Set","params":{"key":"key","value":"value"}}`))
	req.Header.Set("Authorization", "Bearer "+token)
	response, e := secure.Client().Do(req)
	if e != nil {
		t.Fatal(e)
	}
	var out Response
	json.NewDecoder(response.Body).Decode(&out)
	response.Body.Close()
	if out.Result == nil || out.Error != nil {
		t.Fatalf("TLS: %+v", out)
	}
}
func TestPeerAuthenticationAndFrames(t *testing.T) {
	nodes := cluster(t, 1)
	n := nodes[0]
	conn, e := net.Dial("tcp", n.config.Address)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	writePacket(conn, multicast.Packet{Kind: "status", Sender: "0", Token: "wrong"})
	out, e := readPacket(conn)
	if e != nil || out.Error != "unauthorized peer" {
		t.Fatal("peer authentication failed", e)
	}
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(maxFrame+1))
	if _, e = readPacket(&b); e == nil {
		t.Fatal("oversized frame accepted")
	}
	if _, e = readPacket(strings.NewReader("x")); e != io.ErrUnexpectedEOF {
		t.Fatal("truncated frame accepted", e)
	}
}
func TestMemcachedOutageAndRealCache(t *testing.T) {
	nodes := cluster(t, 1)
	n := nodes[0]
	set(t, n, "cache", "key", "value")
	n.Cache = memcache.New("127.0.0.1:1")
	n.Cache.Timeout = 10 * time.Millisecond
	if r, e := n.Read("key", time.Second); e != nil || r.Value != "value" {
		t.Fatal("cache outage lost durable data")
	}
	if addr := os.Getenv("MEMCACHED_ADDR"); addr != "" {
		n.Cache = memcache.New(addr)
		n.Cache.Timeout = 100 * time.Millisecond
		for i := 0; i < 2; i++ {
			if r, e := n.Read("key", time.Second); e != nil || r.Value != "value" {
				t.Fatal("real cache failure")
			}
		}
		set(t, n, "replace", "key", "replacement")
		if r, e := n.Read("key", time.Second); e != nil || r.Value != "replacement" {
			t.Fatal("stale cache returned")
		}
	} else {
		t.Log("external Memcached check not enabled")
	}
}
func TestClientAmbiguousRetryAndCancellation(t *testing.T) {
	nodes := cluster(t, 1)
	n := nodes[0]
	s, _ := NewService(n, token, 2)
	first := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first {
			first = false
			set(t, n, "retry", "key", "value")
			http.Error(w, "lost reply", 503)
			return
		}
		s.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	c := NewClient([]string{server.URL, server.URL}, token)
	defer c.HTTP.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, e := c.Call(ctx, Request{ID: "retry", Method: "Cache.Set", Params: Params{Key: "key", Value: "value"}})
	if e != nil || out.Revision != 1 {
		t.Fatalf("retry: %+v %v", out, e)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, e = c.Call(canceled, Request{ID: "cancel", Method: "Cache.Get", Params: Params{Key: "key"}}); e == nil {
		t.Fatal("canceled request accepted")
	}
}

func TestMaximumEscapedValueRoundtrip(t *testing.T) {
	nodes := cluster(t, 3)
	service, _ := NewService(nodes[1], token, 2)
	server := httptest.NewServer(service.Handler())
	defer server.Close()
	client := NewClient([]string{server.URL}, token)
	defer client.HTTP.CloseIdleConnections()
	value := strings.Repeat("<", 16384)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if result, e := client.Call(ctx, Request{ID: "escaped-write", Method: "Cache.Set", Params: Params{Key: "escaped", Value: value}}); e != nil || !result.Found {
		t.Fatalf("write: %+v %v", result, e)
	}
	if result, e := client.Call(ctx, Request{ID: "escaped-read", Method: "Cache.Get", Params: Params{Key: "escaped"}}); e != nil || result.Value != value {
		t.Fatalf("escaped response truncated: %v", e)
	}
	batch := make([]Command, multicast.MaxBatch)
	for i := range batch {
		batch[i] = Command{ID: fmt.Sprint(i), Op: "Cache.Set", Key: fmt.Sprint(i), Value: value}
	}
	record := multicast.Record{Sequence: 1, Batch: batch}
	record.Hash = record.Digest()
	var frame bytes.Buffer
	if e := writePacket(&frame, multicast.Packet{Kind: "deliver", Record: &record}); e != nil {
		t.Fatal("valid maximum batch exceeds peer frame", e)
	}
	if p, e := readPacket(&frame); e != nil || len(p.Record.Batch) != multicast.MaxBatch {
		t.Fatal("maximum batch cannot be decoded", e)
	}
}
