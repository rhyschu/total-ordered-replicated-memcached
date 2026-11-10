package multicast

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type PeerCall func(context.Context, string, Packet) (Packet, error)
type Consistent struct {
	ID, Sequencer  string
	Peers          []string
	State          *State
	call           PeerCall
	order          chan struct{}
	queue          chan pendingWrite
	batchSize      int
	flush, timeout time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	stopped        chan struct{}
}

func NewConsistent(id, sequencer string, peers []string, state *State, call PeerCall, batch int, flush, timeout time.Duration) *Consistent {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Consistent{ID: id, Sequencer: sequencer, Peers: append([]string(nil), peers...), State: state, call: call, order: make(chan struct{}, 1), queue: make(chan pendingWrite, 128), batchSize: batch, flush: flush, timeout: timeout, ctx: ctx, cancel: cancel, stopped: make(chan struct{})}
	if id == sequencer {
		go p.runSequencer()
	} else {
		close(p.stopped)
	}
	return p
}
func (p *Consistent) Close() { p.cancel(); <-p.stopped }
func (p *Consistent) Write(ctx context.Context, c Command) (Result, error) {
	if err := c.Validate(); err != nil {
		return Result{}, err
	}
	if p.ID != p.Sequencer {
		r, e := p.call(ctx, p.Sequencer, Packet{Kind: "submit", Command: &c})
		if e != nil {
			return Result{}, e
		}
		if r.Error != "" {
			return Result{}, errors.New(r.Error)
		}
		return r.Result, nil
	}
	w := pendingWrite{command: c, ctx: ctx, done: make(chan writeResult, 1)}
	select {
	case <-p.ctx.Done():
		return Result{}, ErrUnavailable
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}
	select {
	case p.queue <- w:
	default:
		return Result{}, errors.New("sequencer queue is full")
	}
	select {
	case r := <-w.done:
		return r.result, r.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-p.ctx.Done():
		return Result{}, ErrUnavailable
	}
}
func (p *Consistent) Fence(ctx context.Context) (uint64, error) {
	if p.ID != p.Sequencer {
		r, e := p.call(ctx, p.Sequencer, Packet{Kind: "fence"})
		if e != nil {
			return 0, e
		}
		if r.Error != "" {
			return 0, errors.New(r.Error)
		}
		return r.Sequence, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	select {
	case p.order <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-p.ctx.Done():
		return 0, ErrUnavailable
	}
	defer func() { <-p.order }()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := p.synchronize(ctx); err != nil {
		return 0, err
	}
	seq, _, err := p.State.Position()
	return seq, err
}
func (p *Consistent) Read(ctx context.Context, key string) (Result, error) {
	seq, err := p.Fence(ctx)
	if err != nil {
		return Result{}, err
	}
	if err = p.State.Wait(ctx, seq); err != nil {
		return Result{}, err
	}
	return p.State.Get(key)
}

// All configured replicas must acknowledge the ordered prefix. This deliberately
// sacrifices availability on any replica failure; it is not quorum consensus.
func (p *Consistent) synchronize(ctx context.Context) error {
	seq, _, err := p.State.Position()
	if err != nil {
		return err
	}
	results := make(chan error, len(p.Peers))
	for _, id := range p.Peers {
		go func(id string) {
			status, e := p.call(ctx, id, Packet{Kind: "status"})
			if e != nil {
				results <- e
				return
			}
			if status.Error != "" {
				results <- errors.New(status.Error)
				return
			}
			if status.Sequence > seq {
				results <- errors.New("replica is ahead of fixed sequencer")
				return
			}
			hash, e := p.State.HashAt(status.Sequence)
			if e != nil || hash != status.Hash {
				results <- errors.New("replica history conflicts with sequencer")
				return
			}
			for index := status.Sequence + 1; index <= seq; index++ {
				if e = ctx.Err(); e != nil {
					results <- e
					return
				}
				record, e := p.State.Record(index)
				if e != nil {
					results <- e
					return
				}
				ack, e := p.call(ctx, id, Packet{Kind: "deliver", Record: &record})
				if e != nil {
					results <- e
					return
				}
				if ack.Error != "" || ack.Sequence < index {
					results <- fmt.Errorf("replica %s failed ordered delivery", id)
					return
				}
			}
			results <- nil
		}(id)
	}
	for range p.Peers {
		if e := <-results; e != nil {
			err = e
		}
	}
	return err
}
func (p *Consistent) Receive(ctx context.Context, packet Packet) Packet {
	out := Packet{}
	switch packet.Kind {
	case "status":
		var e error
		out.Sequence, out.Hash, e = p.State.Position()
		if e != nil {
			out.Error = e.Error()
		}
	case "deliver":
		if p.ID == p.Sequencer || packet.Sender != p.Sequencer || packet.Record == nil {
			out.Error = "invalid sequencer delivery"
			break
		}
		if e := p.State.Deliver(*packet.Record); e != nil {
			out.Error = e.Error()
		}
		out.Sequence, out.Hash, _ = p.State.Position()
	case "submit":
		if p.ID != p.Sequencer || packet.Command == nil {
			out.Error = "not the configured sequencer"
			break
		}
		r, e := p.Write(ctx, *packet.Command)
		out.Result = r
		if e != nil {
			out.Error = e.Error()
		}
	case "fence":
		if p.ID != p.Sequencer {
			out.Error = "not the configured sequencer"
			break
		}
		seq, e := p.Fence(ctx)
		out.Sequence = seq
		if e != nil {
			out.Error = e.Error()
		}
	default:
		out.Error = "unknown peer message"
	}
	return out
}
