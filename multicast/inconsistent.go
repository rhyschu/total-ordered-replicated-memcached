package multicast

import (
	"total-ordered-replicated-memcached/memcached"
	"fmt"
)

type Inconsistent struct {
	mc       *memcached.Client
	ID       int
	Peers    []int
	SendFunc func(targetID int, pac Packet)
}

func NewInconsistent(mc *memcached.Client, id int, peers []int, sendFunc func(int, Packet)) *Inconsistent {
	return &Inconsistent{mc: mc, ID: id, Peers: peers, SendFunc: sendFunc}
}

func (p *Inconsistent) HandleClientGet(req GetRequest) GetResponse {
	value, success := p.mc.Get(req.Key)
	return GetResponse{Value: value, Success: success, RequestID: req.RequestID}
}

func (p *Inconsistent) HandleClientSet(req SetRequest) SetResponse {
	success := p.mc.Set(req.Key, req.Value)
	msg := Packet{MsgType: ServerForwardSet, Msg: ForwardSet{Request: req, SourceID: p.ID}}
	for _, peerID := range p.Peers {
		go p.SendFunc(peerID, msg)
	}
	return SetResponse{Success: success, RequestID: req.RequestID}
}

func (p *Inconsistent) HandleServerMsg(pac Packet) error {
	switch pac.MsgType {
	case ServerForwardSet:
		msg := pac.Msg.(ForwardSet)
		p.mc.Set(msg.Request.Key, msg.Request.Value)
	default:
		return fmt.Errorf("invalid message type: %v", pac.MsgType)
	}
	return nil
}

func (p *Inconsistent) InitElection() {
	return
}
