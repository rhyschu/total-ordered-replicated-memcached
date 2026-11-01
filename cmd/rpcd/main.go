package main

import (
	"context"
	"encoding/json"
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

func main() {
	id := flag.String("id", "n1", "Unique node ID")
	address := flag.String("raft", "127.0.0.1:9001", "Private Raft address")
	bind := flag.String("http", "127.0.0.1:8001", "RPC listen address")
	dir := flag.String("data", "data/n1", "Exclusive persistent node directory")
	peers := flag.String("peers", `{"n1":"127.0.0.1:9001"}`, "Static voter map, JSON")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap fresh cluster on exactly one node")
	backend := flag.String("memcached", "", "Optional dedicated Memcached address")
	cert := flag.String("tls-cert", "", "TLS certificate path")
	key := flag.String("tls-key", "", "TLS private key path")
	flag.Parse()
	var members map[string]string
	if err := json.Unmarshal([]byte(*peers), &members); err != nil {
		slog.Error("invalid peers")
		os.Exit(1)
	}
	token := os.Getenv("RPCACHE_TOKEN")
	if len(token) < 24 {
		slog.Error("set RPCACHE_TOKEN to a random secret of at least 24 characters")
		os.Exit(1)
	}
	n, err := rpcache.Open(rpcache.Config{ID: *id, Address: *address, Directory: *dir, Peers: members, Bootstrap: *bootstrap})
	if err != nil {
		slog.Error("start failed", "error", err)
		os.Exit(1)
	}
	defer n.Close()
	if *backend != "" {
		n.Cache = memcache.New(*backend)
		n.Cache.Timeout = 50 * time.Millisecond
		n.Cache.MaxIdleConns = 16
	}
	service, err := rpcache.NewService(n, token, 128)
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return
	}
	server := &http.Server{Addr: *bind, Handler: service.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	slog.Info("rpc service starting", "node", *id, "http", *bind, "raft", *address)
	if *cert != "" || *key != "" {
		err = server.ListenAndServeTLS(*cert, *key)
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		slog.Error("listen failed", "error", err)
		os.Exit(1)
	}
}
