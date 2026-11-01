package rpcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb/v2"
)

type Config struct {
	ID, Address, Directory string
	Peers                  map[string]string
	Bootstrap              bool
}
type Node struct {
	Raft      *raft.Raft
	State     *State
	transport *raft.NetworkTransport
	store     *bolt.BoltStore
	Cache     *memcache.Client
}

func Open(c Config) (*Node, error) {
	if c.ID == "" || c.Address == "" || c.Directory == "" || len(c.Peers) == 0 || c.Peers[c.ID] != c.Address {
		return nil, fmt.Errorf("id, raft address, directory and matching static peer membership are required")
	}
	if err := os.MkdirAll(c.Directory, 0700); err != nil {
		return nil, err
	}
	address, err := net.ResolveTCPAddr("tcp", c.Address)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(c.Address, address, 4, time.Second, io.Discard)
	if err != nil {
		return nil, err
	}
	db, err := bolt.NewBoltStore(filepath.Join(c.Directory, "raft.db"))
	if err != nil {
		transport.Close()
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(c.Directory, 2, io.Discard)
	if err != nil {
		db.Close()
		transport.Close()
		return nil, err
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(c.ID)
	config.LogOutput = io.Discard
	config.SnapshotThreshold = 512
	config.SnapshotInterval = 20 * time.Second
	config.TrailingLogs = 128
	state := NewState()
	r, err := raft.NewRaft(config, state, db, db, snaps, transport)
	if err != nil {
		db.Close()
		transport.Close()
		return nil, err
	}
	n := &Node{Raft: r, State: state, transport: transport, store: db}
	existing, err := raft.HasExistingState(db, db, snaps)
	if err != nil {
		n.Close()
		return nil, err
	}
	if c.Bootstrap && !existing {
		membership := raft.Configuration{}
		for id, address := range c.Peers {
			membership.Servers = append(membership.Servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(address)})
		}
		if err = r.BootstrapCluster(membership).Error(); err != nil {
			n.Close()
			return nil, err
		}
	}
	return n, nil
}
func (n *Node) Close() error {
	err := n.Raft.Shutdown().Error()
	n.transport.Close()
	if e := n.store.Close(); err == nil {
		err = e
	}
	return err
}
func (n *Node) Write(c Command, timeout time.Duration) (Result, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return Result{}, err
	}
	f := n.Raft.Apply(b, timeout)
	if err = f.Error(); err != nil {
		return Result{}, err
	}
	r, ok := f.Response().(Result)
	if !ok {
		return Result{}, fmt.Errorf("invalid state machine response")
	}
	return r, nil
}
func (n *Node) Read(key string, timeout time.Duration) (Result, error) {
	if n.Raft.State() != raft.Leader {
		return Result{}, raft.ErrNotLeader
	}
	if err := n.Raft.VerifyLeader().Error(); err != nil {
		return Result{}, err
	}
	if err := n.Raft.Barrier(timeout).Error(); err != nil {
		return Result{}, err
	}
	r := n.State.Get(key)
	// The state machine is authoritative; cache eviction/failure cannot lose data.
	// Content-derived keys prevent stale values after replacement or cluster reuse.
	if r.Found && n.Cache != nil {
		sum := sha256.Sum256([]byte(key + "\x00" + r.Value))
		cacheKey := "rpcache:" + hex.EncodeToString(sum[:])
		if item, err := n.Cache.Get(cacheKey); err == nil && string(item.Value) == r.Value {
			return r, nil
		}
		_ = n.Cache.Set(&memcache.Item{Key: cacheKey, Value: []byte(r.Value), Expiration: 300})
	}
	return r, nil
}
