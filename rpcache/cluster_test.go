package rpcache

import (
	"bytes"
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
	"github.com/hashicorp/raft"
)

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
	members := map[string]string{}
	for i := 0; i < count; i++ {
		members[fmt.Sprint(i)] = address(t)
	}
	nodes := make([]*Node, count)
	for i := range nodes {
		n, e := Open(Config{ID: fmt.Sprint(i), Address: members[fmt.Sprint(i)], Peers: members, Directory: filepath.Join(t.TempDir(), "state"), Bootstrap: i == 0})
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
	leader(t, nodes)
	return nodes
}
func leader(t testing.TB, nodes []*Node) *Node {
	t.Helper()
	end := time.Now().Add(12 * time.Second)
	for time.Now().Before(end) {
		for _, n := range nodes {
			if n != nil && n.Raft.State() == raft.Leader && n.Raft.VerifyLeader().Error() == nil {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("leader not elected")
	return nil
}
func set(t testing.TB, n *Node, id, key, value string) Result {
	t.Helper()
	r, e := n.Write(Command{id, "Cache.Set", key, value}, time.Second)
	if e != nil || r.Error != "" {
		t.Fatalf("write: %+v %v", r, e)
	}
	return r
}

func TestReplicationFailoverAndQuorumLoss(t *testing.T) {
	nodes := cluster(t, 3)
	n := leader(t, nodes)
	set(t, n, "write-1", "key", "committed")
	for _, peer := range nodes {
		if peer != n {
			if _, e := peer.Read("key", time.Second); e == nil {
				t.Fatal("follower served a strong read")
			}
		}
	}
	for i, peer := range nodes {
		if peer == n {
			peer.Close()
			nodes[i] = nil
		}
	}
	replacement := leader(t, nodes)
	v, e := replacement.Read("key", time.Second)
	if e != nil || v.Value != "committed" {
		t.Fatalf("lost committed write: %+v %v", v, e)
	}
	set(t, replacement, "write-2", "key", "after-failover")
	for i, peer := range nodes {
		if peer != nil && peer != replacement {
			peer.Close()
			nodes[i] = nil
		}
	}
	done := make(chan error, 1)
	go func() {
		_, e := replacement.Write(Command{"write-3", "Cache.Set", "key", "no-quorum"}, time.Second)
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("write acknowledged without quorum")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("quorum loss did not terminate write")
	}
	if _, e := replacement.Read("key", time.Second); e == nil {
		t.Fatal("read accepted without quorum")
	}
}
func TestSnapshotRestartAndIdempotency(t *testing.T) {
	addr := address(t)
	dir := t.TempDir()
	c := Config{ID: "n", Address: addr, Directory: dir, Peers: map[string]string{"n": addr}, Bootstrap: true}
	n, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	leader(t, []*Node{n})
	first := set(t, n, "same-request", "key", "one")
	again := set(t, n, "same-request", "key", "one")
	if first.Revision != again.Revision {
		t.Fatal("retry changed revision")
	}
	if r, e := n.Write(Command{"same-request", "Cache.Set", "key", "two"}, time.Second); e != nil || r.Error == "" {
		t.Fatal("conflicting retry accepted")
	}
	if e := n.Raft.Snapshot().Error(); e != nil {
		t.Fatal(e)
	}
	n.Close()
	n, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	leader(t, []*Node{n})
	r, e := n.Read("key", time.Second)
	if e != nil || r.Value != "one" {
		t.Fatalf("restart lost value: %+v %v", r, e)
	}
	if retry := set(t, n, "same-request", "key", "one"); retry.Revision != first.Revision {
		t.Fatal("restart lost receipt")
	}
	r, e = n.Write(Command{"delete-1", "Cache.Delete", "key", ""}, time.Second)
	if e != nil || !r.Found {
		t.Fatal("delete failed")
	}
	r, e = n.Read("key", time.Second)
	if e != nil || r.Found {
		t.Fatal("deleted value returned")
	}
}

const token = "integration-token-32-characters-long"

func rpc(t testing.TB, url, body, auth string) (int, Response) {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/rpc", strings.NewReader(body))
	req.Header.Set("Authorization", auth)
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	var response Response
	if res.StatusCode == 200 {
		if e = json.NewDecoder(res.Body).Decode(&response); e != nil {
			t.Fatal(e)
		}
	}
	return res.StatusCode, response
}
func TestRPCValidationAuthLimitsAndTLS(t *testing.T) {
	nodes := cluster(t, 1)
	n := leader(t, nodes)
	s, _ := NewService(n, token, 1)
	server := httptest.NewTLSServer(s.Handler())
	defer server.Close()
	client := server.Client()
	req, _ := http.NewRequest("POST", server.URL+"/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"one","method":"Cache.Set","params":{"key":"x","value":"y"}}`))
	req.Header.Set("Authorization", "Bearer "+token)
	response, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	var v Response
	json.NewDecoder(response.Body).Decode(&v)
	response.Body.Close()
	if v.Error != nil || v.Result == nil {
		t.Fatalf("TLS RPC failed: %+v", v)
	}
	plain := httptest.NewServer(s.Handler())
	defer plain.Close()
	if status, _ := rpc(t, plain.URL, `{}`, ""); status != 401 {
		t.Fatal("missing auth accepted")
	}
	for _, body := range []string{`{}`, `[]`, `{"jsonrpc":"2.0","id":"bad","method":"invalid","params":{"key":"x"}}`, `{"jsonrpc":"2.0","id":"bad","method":"Cache.Set","params":{"key":"bad key"}}`, strings.Repeat("x", 40000)} {
		if _, r := rpc(t, plain.URL, body, "Bearer "+token); r.Error == nil {
			t.Fatalf("invalid input accepted: %.60s", body)
		}
	}
	s.slots <- struct{}{}
	if status, _ := rpc(t, plain.URL, `{}`, "Bearer "+token); status != 429 {
		t.Fatal("overload accepted")
	}
	<-s.slots
	metrics, _ := http.NewRequest("GET", plain.URL+"/metrics", nil)
	metrics.Header.Set("Authorization", "Bearer "+token)
	m, e := http.DefaultClient.Do(metrics)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(m.Body)
	m.Body.Close()
	if !bytes.Contains(b, []byte("rpcache_errors_total")) {
		t.Fatal("missing metrics")
	}
}
func TestConcurrentReadAfterWrite(t *testing.T) {
	nodes := cluster(t, 3)
	n := leader(t, nodes)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprint(i)
			for j := 0; j < 10; j++ {
				value := fmt.Sprint(j)
				r, e := n.Write(Command{fmt.Sprintf("%d-%d", i, j), "Cache.Set", key, value}, time.Second)
				if e != nil || r.Error != "" {
					t.Errorf("write: %v %v", r, e)
					return
				}
				v, e := n.Read(key, time.Second)
				if e != nil || v.Value != value {
					t.Errorf("read: %v %v", v, e)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
func TestMemcachedFailureDoesNotChangeResults(t *testing.T) {
	nodes := cluster(t, 1)
	n := leader(t, nodes)
	set(t, n, "one", "key", "value")
	n.Cache = memcache.New("127.0.0.1:1")
	n.Cache.Timeout = 10 * time.Millisecond
	r, e := n.Read("key", time.Second)
	if e != nil || r.Value != "value" {
		t.Fatal("cache outage lost value")
	}
	if a := os.Getenv("MEMCACHED_ADDR"); a != "" {
		n.Cache = memcache.New(a)
		r, e = n.Read("key", time.Second)
		if e != nil || r.Value != "value" {
			t.Fatal("real cache read failed")
		}
		r, e = n.Read("key", time.Second)
		if e != nil || r.Value != "value" {
			t.Fatal("cache reuse failed")
		}
	}
}
