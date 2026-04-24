package multicast

import (
	"total-ordered-replicated-memcached/memcached"
	"time"
	"fmt"
	"sync"
	"sync/atomic"
)

type Consistent struct {
	mc             *memcached.Client
	ID             int
	Peers          []int
	Func1          func(targetID int, pac Packet)
	IsSequencer    bool
	Sequencer      *Sequencer
	HighestApplied int64
	Mutex          sync.Mutex
	Cond           *sync.Cond
	RIMap          sync.Map
	RICounter      int64
	SetMap         sync.Map
}

func NewConsistent(mc *memcached.Client, id int, peers []int, func1 func(int, Packet), isSequencer bool, batchSize int) *Consistent {
	c := &Consistent{mc: mc, ID: id, Peers: peers, Func1: func1, IsSequencer: isSequencer}
	c.Cond = sync.NewCond(&c.Mutex)
	if isSequencer {
		c.Sequencer = NewSequencer(batchSize, 10*time.Millisecond)
		go c.runSequencer()
	}
	return c
}

func (p *Consistent) runSequencer() {
	for msg := range p.Sequencer.MulticastChannel() {
		pac := Packet{MsgType: ServerTOMulticast, Msg: msg}
		p.HandleServerMsg(pac)
		for _, peerID := range p.Peers {
			go p.Func1(peerID, pac)
		}
	}
}

func (p *Consistent) HandleClientGet(req GetRequest) GetResponse {
	var readIndex int64
	if p.IsSequencer {
		readIndex = p.Sequencer.GetCurrentSeq()
	} else {
		riID := atomic.AddInt64(&p.RICounter, 1)
		ch := make(chan int64, 1)
		p.RIMap.Store(riID, ch)
		p.Func1(1, Packet{MsgType: ServerRIQuery, Msg: RIQuery{RequestID: riID, NodeID: p.ID}})
		readIndex = <-ch
		p.RIMap.Delete(riID)
	}
	p.Mutex.Lock()
	for p.HighestApplied < readIndex {
		p.Cond.Wait()
	}
	p.Mutex.Unlock()
	value, success := p.mc.Get(req.Key)
	return GetResponse{Value: value, Success: success, RequestID: req.RequestID}
}

func (p *Consistent) HandleClientSet(req SetRequest) SetResponse {
	done := make(chan bool, 1)
	p.SetMap.Store(req.RequestID, done)
	if p.IsSequencer {
		p.Sequencer.AddSet(req)
	} else {
		p.Func1(1, Packet{MsgType: ServerForwardSet, Msg: ForwardSet{Request: req, SourceID: p.ID}})
	}
	success := <-done
	p.SetMap.Delete(req.RequestID)
	return SetResponse{Success: success, RequestID: req.RequestID}
}

func (p *Consistent) HandleServerMsg(pac Packet) error {
	switch pac.MsgType {
	case ServerForwardSet:
		if p.IsSequencer {
			p.Sequencer.AddSet(pac.Msg.(ForwardSet).Request)
		}
	case ServerTOMulticast:
		msg := pac.Msg.(TOMulticast)
		for _, req := range msg.Batch {
			p.mc.Set(req.Key, req.Value)
			ch, ok := p.SetMap.Load(req.RequestID)
			if ok {
				ch.(chan bool) <- true
			}
		}
		p.Mutex.Lock()
		p.HighestApplied = msg.SequenceNumber
		p.Cond.Broadcast()
		p.Mutex.Unlock()
	case ServerRIQuery:
		if p.IsSequencer {
			msg := pac.Msg.(RIQuery)
			p.Func1(msg.NodeID, Packet{MsgType: ServerRIResponse, Msg: RIResponse{LastSequence: p.Sequencer.GetCurrentSeq(), RequestID: msg.RequestID}})
		}
	case ServerRIResponse:
		msg := pac.Msg.(RIResponse)
		ch, ok := p.RIMap.Load(msg.RequestID)
		if ok {
			ch.(chan int64) <- msg.LastSequence
		}
	default:
		return fmt.Errorf("invalid message type: %v", pac.MsgType)
	}
	return nil
}
