package rpcache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientRetriesAmbiguousWriteWithSameID(t *testing.T) {
	nodes := cluster(t, 1)
	n := leader(t, nodes)
	service, _ := NewService(n, token, 2)
	handler := service.Handler()
	first := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first {
			first = false
			r1, e := n.Write(Command{"retry", "Cache.Set", "key", "value"}, time.Second)
			if e != nil || !r1.Found {
				t.Error("initial commit failed")
			}
			http.Error(w, "response lost", 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := NewClient([]string{server.URL, server.URL}, token)
	defer c.HTTP.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, e := c.Call(ctx, Request{ID: "retry", Method: "Cache.Set", Params: Params{Key: "key", Value: "value"}})
	if e != nil {
		t.Fatal(e)
	}
	original := n.State.data.Receipts["retry"].Result
	if out.Revision != original.Revision {
		t.Fatal("ambiguous retry executed again")
	}
	canceled, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, e = c.Call(canceled, Request{ID: "read", Method: "Cache.Get", Params: Params{Key: "key"}}); e == nil {
		t.Fatal("canceled call accepted")
	}
}
