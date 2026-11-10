package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"total-ordered-replicated-memcached/rpcache"
)

func run() error {
	id := flag.String("id", "n1", "Unique node ID")
	peer := flag.String("peer", "127.0.0.1:9001", "Private TCP peer listen address")
	bind := flag.String("http", "127.0.0.1:8001", "HTTP RPC listen address")
	dir := flag.String("data", "data/n1", "Exclusive persistent node directory")
	peers := flag.String("peers", `{"n1":"127.0.0.1:9001"}`, "Static ID/address map, JSON")
	sequencer := flag.String("sequencer", "n1", "Fixed sequencer ID; identical on every node")
	batch := flag.Int("batch", 5, "Maximum writes per ordered batch (1-16)")
	flush := flag.Duration("flush", 2*time.Millisecond, "Maximum batching delay")
	backend := flag.String("memcached", "", "Optional dedicated Memcached read cache")
	cert := flag.String("tls-cert", "", "TLS certificate path")
	key := flag.String("tls-key", "", "TLS private key path")
	flag.Parse()
	if *batch < 1 || *batch > 16 || *flush < time.Microsecond || *flush > 100*time.Millisecond {
		return errors.New("batch must be 1-16 and flush must be 1us-100ms")
	}
	var members map[string]string
	if e := json.Unmarshal([]byte(*peers), &members); e != nil {
		return e
	}
	token := os.Getenv("RPCACHE_TOKEN")
	n, e := rpcache.Open(rpcache.Config{ID: *id, Address: *peer, Directory: *dir, Peers: members, Sequencer: *sequencer, Token: token, BatchSize: *batch, FlushInterval: *flush})
	if e != nil {
		return e
	}
	defer n.Close()
	if *backend != "" {
		n.Cache = memcache.New(*backend)
		n.Cache.Timeout = 50 * time.Millisecond
		n.Cache.MaxIdleConns = 16
	}
	service, e := rpcache.NewService(n, token, 128)
	if e != nil {
		return e
	}
	server := &http.Server{Addr: *bind, Handler: service.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Shutdown(deadline)
		case <-shutdownDone:
		}
	}()
	defer close(shutdownDone)
	slog.Info("ordered cache starting", "node", *id, "sequencer", *sequencer, "http", *bind, "peer", *peer)
	if *cert != "" || *key != "" {
		e = server.ListenAndServeTLS(*cert, *key)
	} else {
		e = server.ListenAndServe()
	}
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
func main() {
	if e := run(); e != nil {
		slog.Error("server failed", "error", e)
		os.Exit(1)
	}
}
