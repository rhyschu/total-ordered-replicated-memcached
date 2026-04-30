// consistent.go
// server actions upon receiving messages for consistent protocol
package multicast

import (
	"total-ordered-replicated-memcached/memcached"
	"fmt"
	"time"
	"sync"
	"sync/atomic"
)

type Consistent struct {
	mc                *memcached.Client
	ID                int
	Peers             []int
	SendFunc          func(targetID int, pac Packet)
	IsSequencer       bool
	Sequencer         *Sequencer
	HighestApplied    int64
	Mutex             sync.Mutex
	Cond              *sync.Cond // conditionally wait on elections and applying messages
	PendingMsgs       map[int64]TOMulticast
	RIMap             sync.Map // maps RI request ID to channels
	RICounter         int64 // unique, incrementing ID for RI requests
	SetMap            sync.Map // maps clientID_requestID to channels
	Election          bool
	ElectionResponses map[int]int64 // maps peer ID to their response number
}

func NewConsistent(mc *memcached.Client, id int, peers []int, sendFunc func(int, Packet), isSequencer bool, batchSize int) *Consistent {
	c := &Consistent{mc: mc, ID: id, Peers: peers, SendFunc: sendFunc, IsSequencer: isSequencer, PendingMsgs: make(map[int64]TOMulticast), ElectionResponses: make(map[int]int64)}
	c.Cond = sync.NewCond(&c.Mutex)
	if isSequencer {
		c.Sequencer = NewSequencer(batchSize, 100*time.Millisecond) // subject to change
		go c.RunSequencer()
	}
	return c
}

func (p *Consistent) RunSequencer() {
	for msg := range p.Sequencer.MulticastChan {
		pac := Packet{MsgType: ServerTOMulticast, Msg: msg}
		p.HandleServerMsg(pac)
		for _, peerID := range p.Peers {
			go p.SendFunc(peerID, pac)
		}
	}
}

func (p *Consistent) HandleClientGet(req GetRequest) GetResponse {
	p.Mutex.Lock()
    for p.Election {
        p.Cond.Wait()
    }
    p.Mutex.Unlock()
	var readIndex int64
	if p.IsSequencer {
		readIndex = p.Sequencer.GetCurrentSeq()
	} else {
		riID := atomic.AddInt64(&p.RICounter, 1)
		ch := make(chan int64, 1)
		p.RIMap.Store(riID, ch)
		p.SendFunc(1, Packet{MsgType: ServerRIQuery, Msg: RIQuery{RequestID: riID, SourceID: p.ID}})
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
	p.Mutex.Lock()
    for p.Election {
        p.Cond.Wait()
    }
    p.Mutex.Unlock()
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
			p.Cond.Broadcast() // wake up any waiting handlers
		}
		p.Mutex.Unlock()
	case ServerRIQuery:
		if p.IsSequencer {
			msg := pac.Msg.(RIQuery)
			p.SendFunc(msg.SourceID, Packet{MsgType: ServerRIResponse, Msg: RIResponse{LastSequence: p.Sequencer.GetCurrentSeq(), RequestID: msg.RequestID}})
		}
	case ServerRIResponse:
		msg := pac.Msg.(RIResponse)
		ch, ok := p.RIMap.Load(msg.RequestID)
		if ok {
			ch.(chan int64) <- msg.LastSequence
		}
	case ServerReformationInvite:
        p.Mutex.Lock()
        p.Election = true
        p.Mutex.Unlock()
        msg := pac.Msg.(ReformationInvite)
        p.SendFunc(msg.SourceID, Packet{MsgType: ServerReformationResponse, Msg: ReformationResponse{SourceID: p.ID, HighestApplied: p.HighestApplied}})
    case ServerReformationResponse:
        p.Mutex.Lock()
		msg := pac.Msg.(ReformationResponse)
        p.ElectionResponses[msg.SourceID] = msg.HighestApplied
        if len(p.ElectionResponses) == len(p.Peers) { 
            maxID := p.ID
            maxApplied := p.HighestApplied
            for peerID, applied := range p.ElectionResponses {
                if applied > maxApplied {
                    maxID = peerID
                    maxApplied = applied
                }
            }
			for _, peerID := range p.Peers {
				go p.SendFunc(peerID, Packet{MsgType: ServerSequencerAnnouncement, Msg: SequencerAnnouncement{SequencerID: maxID}})
			}
        }
        p.Mutex.Unlock()
    case ServerSequencerAnnouncement:
        p.Mutex.Lock()
		p.Election = false
		msg := pac.Msg.(SequencerAnnouncement)
		if (msg.SequencerID == p.ID) && p.Sequencer == nil {
            p.Sequencer = NewSequencer(1, 100*time.Millisecond) // subject to change
            p.Sequencer.CurrentSeq = p.HighestApplied
            go p.RunSequencer()
        }
        fmt.Printf("New Sequencer %d is running\n", msg.SequencerID)
        p.Mutex.Unlock()
	default:
		return fmt.Errorf("invalid message type: %v", pac.MsgType)
	}
	return nil
}

func (p *Consistent) InitElection() {
	p.Mutex.Lock()
    p.Election = true
    p.ElectionResponses[p.ID] = p.HighestApplied
    p.Mutex.Unlock()
	for _, peerID := range p.Peers {
        go p.SendFunc(peerID, Packet{MsgType: ServerReformationInvite, Msg: ReformationInvite{SourceID: p.ID}})
    }
}
