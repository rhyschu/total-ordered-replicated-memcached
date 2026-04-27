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
	SendFunc       func(targetID int, pac Packet)
	IsSequencer    bool
	Sequencer      *Sequencer
	HighestApplied int64
	Mutex          sync.Mutex
	Cond           *sync.Cond
	PendingMsgs    map[int64]TOMulticast
	RIMap          sync.Map
	RICounter      int64
	SetMap         sync.Map
}

func NewConsistent(mc *memcached.Client, id int, peers []int, sendFunc func(int, Packet), isSequencer bool, batchSize int) *Consistent {
	c := &Consistent{mc: mc, ID: id, Peers: peers, SendFunc: sendFunc, IsSequencer: isSequencer, PendingMsgs: make(map[int64]TOMulticast)}
	c.Cond = sync.NewCond(&c.Mutex)
	if isSequencer {
		c.Sequencer = NewSequencer(batchSize, 100*time.Millisecond)
		go c.RunSequencer()
	}
	return c
}

func (p *Consistent) RunSequencer() {
	for msg := range p.Sequencer.MulticastChan {
		pac := Packet{MsgType: ServerTOMulticast, Msg: msg}
		p.HandleServerMsg(pac)
		for _, peerID := range p.Peers {
			p.SendFunc(peerID, pac)
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
		p.SendFunc(1, Packet{MsgType: ServerRIQuery, Msg: RIQuery{RequestID: riID, NodeID: p.ID}})
		select {
		case readIndex = <-ch:
		case <-time.After(5*time.Second):
			return GetResponse{Success: false, RequestID: req.RequestID}
		}
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
	key := fmt.Sprintf("%s_%d", req.ClientID, req.RequestID)
	p.SetMap.Store(key, done)
	if p.IsSequencer {
		p.Sequencer.AddSet(req)
	} else {
		p.SendFunc(1, Packet{MsgType: ServerForwardSet, Msg: ForwardSet{Request: req, SourceID: p.ID}})
	}
	success := false
	select {
	case success = <-done:
	case <-time.After(5*time.Second):
		success = false
	}
	p.SetMap.Delete(key)
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
		p.Mutex.Lock()
		if msg.SequenceNumber <= p.HighestApplied {
			p.Mutex.Unlock()
			return nil
		}
		p.PendingMsgs[msg.SequenceNumber] = msg
		for {
			nextSeq := p.HighestApplied + 1
			nextMsg, ok := p.PendingMsgs[nextSeq]
			if !ok {
				break
			}
			for _, req := range nextMsg.Batch {
				p.mc.Set(req.Key, req.Value)
				key := fmt.Sprintf("%s_%d", req.ClientID, req.RequestID)
				if ch, ok := p.SetMap.Load(key); ok {
					ch.(chan bool) <- true
				}
			}
			p.HighestApplied = nextSeq
			delete(p.PendingMsgs, nextSeq)
			p.Cond.Broadcast()
		}
		p.Mutex.Unlock()
	case ServerRIQuery:
		if p.IsSequencer {
			msg := pac.Msg.(RIQuery)
			p.SendFunc(msg.NodeID, Packet{MsgType: ServerRIResponse, Msg: RIResponse{LastSequence: p.Sequencer.GetCurrentSeq(), RequestID: msg.RequestID}})
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
