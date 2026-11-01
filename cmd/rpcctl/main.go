package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"
	"total-ordered-replicated-memcached/rpcache"
)

func main() {
	targets := flag.String("targets", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003", "Comma-separated RPC URLs")
	method := flag.String("method", "Cache.Get", "Cache.Get, Cache.Set, Cache.Delete")
	key := flag.String("key", "example", "Cache key")
	value := flag.String("value", "", "Value")
	id := flag.String("id", "", "Stable retry ID; generated when empty")
	flag.Parse()
	if *id == "" {
		b := make([]byte, 16)
		if _, e := rand.Read(b); e != nil {
			log.Fatal(e)
		}
		*id = hex.EncodeToString(b)
	}
	log.Printf("request ID: %s (reuse on retry)", *id)
	c := rpcache.NewClient(strings.Split(*targets, ","), os.Getenv("RPCACHE_TOKEN"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := c.Call(ctx, rpcache.Request{ID: *id, Method: *method, Params: rpcache.Params{Key: *key, Value: *value}})
	if err != nil {
		log.Fatal(err)
	}
	json.NewEncoder(os.Stdout).Encode(result)
}
