package rpcache

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"total-ordered-replicated-memcached/multicast"
)

type Command = multicast.Command
type Result = multicast.Result
type Config struct {
	ID, Address, Directory, Sequencer, Token string
	Peers                                    map[string]string
	BatchSize                                int
	FlushInterval                            time.Duration
}
type Node struct {
	Protocol *multicast.Consistent
	State    *multicast.State
	Cache    *memcache.Client
	config   Config
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	slots    chan struct{}
	once     sync.Once
	closeErr error
}

func Open(c Config) (*Node, error) {
	if c.BatchSize == 0 {
		c.BatchSize = 5
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 2 * time.Millisecond
	}
	if c.ID == "" || c.Directory == "" || c.Sequencer == "" || len(c.Token) < 24 || c.Peers[c.ID] != c.Address || c.Peers[c.Sequencer] == "" || len(c.Peers) > 16 || c.BatchSize < 1 || c.BatchSize > multicast.MaxBatch || c.FlushInterval < time.Microsecond || c.FlushInterval > 100*time.Millisecond {
		return nil, errors.New("invalid node, static membership, sequencer, token or batch configuration")
	}
	members := map[string]string{}
	seen := map[string]bool{}
	ids := []string{}
	for id, address := range c.Peers {
		if id == "" || address == "" || seen[address] {
			return nil, errors.New("invalid or duplicate peer address")
		}
		if _, e := net.ResolveTCPAddr("tcp", address); e != nil {
			return nil, e
		}
		members[id] = address
		seen[address] = true
		if id != c.ID {
			ids = append(ids, id)
		}
	}
	c.Peers = members
	sort.Strings(ids)
	identity, _ := json.Marshal(struct {
		ID, Sequencer string
		Peers         map[string]string
	}{c.ID, c.Sequencer, c.Peers})
	state, err := multicast.OpenState(c.Directory, string(identity))
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		state.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{State: state, config: c, listener: listener, ctx: ctx, cancel: cancel, slots: make(chan struct{}, 128)}
	n.Protocol = multicast.NewConsistent(c.ID, c.Sequencer, ids, state, n.call, c.BatchSize, c.FlushInterval, 2*time.Second)
	n.workers.Add(1)
	go n.accept()
	return n, nil
}
func (n *Node) Close() error {
	n.once.Do(func() {
		n.cancel()
		n.listener.Close()
		n.Protocol.Close()
		n.workers.Wait()
		n.closeErr = n.State.Close()
	})
	return n.closeErr
}
func (n *Node) Write(c Command, timeout time.Duration) (Result, error) {
	ctx, cancel := context.WithTimeout(n.ctx, timeout)
	defer cancel()
	return n.Protocol.Write(ctx, c)
}
func (n *Node) Ready(ctx context.Context) error { _, e := n.Protocol.Fence(ctx); return e }
func (n *Node) Read(key string, timeout time.Duration) (Result, error) {
	ctx, cancel := context.WithTimeout(n.ctx, timeout)
	defer cancel()
	r, err := n.Protocol.Read(ctx, key)
	if err != nil {
		return Result{}, err
	}
	// The ordered durable state remains authoritative. Backend failure is not a
	// miss; a cache entry cannot substitute bytes from an older revision/history.
	if r.Found && n.Cache != nil {
		sum := sha256.Sum256([]byte(key + "\x00" + r.Value))
		cacheKey := "ordered:" + hex.EncodeToString(sum[:])
		if item, e := n.Cache.Get(cacheKey); e == nil && string(item.Value) == r.Value {
			return r, nil
		}
		_ = n.Cache.Set(&memcache.Item{Key: cacheKey, Value: []byte(r.Value), Expiration: 300})
	}
	return r, nil
}

const maxFrame = 2 << 20

func readPacket(r io.Reader) (multicast.Packet, error) {
	var size [4]byte
	var p multicast.Packet
	if _, e := io.ReadFull(r, size[:]); e != nil {
		return p, e
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > maxFrame {
		return p, errors.New("invalid peer frame length")
	}
	b := make([]byte, length)
	if _, e := io.ReadFull(r, b); e != nil {
		return p, e
	}
	e := json.Unmarshal(b, &p)
	return p, e
}
func writePacket(w io.Writer, p multicast.Packet) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	if len(b) > maxFrame {
		return errors.New("peer frame too large")
	}
	frame := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(frame, uint32(len(b)))
	copy(frame[4:], b)
	for len(frame) > 0 {
		count, e := w.Write(frame)
		if e != nil {
			return e
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		frame = frame[count:]
	}
	return nil
}
func (n *Node) call(ctx context.Context, id string, p multicast.Packet) (multicast.Packet, error) {
	address, ok := n.config.Peers[id]
	if !ok {
		return multicast.Packet{}, errors.New("unknown peer")
	}
	dialer := net.Dialer{Timeout: time.Second}
	conn, e := dialer.DialContext(ctx, "tcp", address)
	if e != nil {
		return multicast.Packet{}, e
	}
	defer conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	p.Token = n.config.Token
	p.Sender = n.config.ID
	if e = writePacket(conn, p); e != nil {
		return multicast.Packet{}, e
	}
	return readPacket(conn)
}
func (n *Node) accept() {
	defer n.workers.Done()
	for {
		conn, e := n.listener.Accept()
		if e != nil {
			return
		}
		select {
		case n.slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		n.workers.Add(1)
		go func() {
			defer n.workers.Done()
			defer func() { <-n.slots }()
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			p, e := readPacket(conn)
			if e != nil {
				return
			}
			if subtle.ConstantTimeCompare([]byte(p.Token), []byte(n.config.Token)) != 1 || n.config.Peers[p.Sender] == "" {
				writePacket(conn, multicast.Packet{Error: "unauthorized peer"})
				return
			}
			ctx, cancel := context.WithTimeout(n.ctx, 2500*time.Millisecond)
			defer cancel()
			out := n.Protocol.Receive(ctx, p)
			if e = writePacket(conn, out); e != nil {
				return
			}
		}()
	}
}
func (n *Node) String() string {
	return fmt.Sprintf("%s (sequencer=%s)", n.config.ID, n.config.Sequencer)
}
