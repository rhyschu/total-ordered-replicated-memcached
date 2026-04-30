// types.go
// defines the message types, message structures, and the protocol interface
package multicast

import (
	"encoding/gob"
)

type MsgType int

const (
	ClientGetRequest MsgType = iota
	ClientSetRequest
	ClientGetResponse
	ClientSetResponse
	ServerForwardSet
	ServerTOMulticast
	ServerRIQuery
	ServerRIResponse
	ServerReformationInvite
	ServerReformationResponse
	ServerSequencerAnnouncement
)

type GetRequest struct {
	Key       string
	ClientID  string
	RequestID int64
}

type SetRequest struct {
	Key       string
	Value     string
	ClientID  string
	RequestID int64
}

type GetResponse struct {
	Value     string
	Success   bool
	RequestID int64
}

type SetResponse struct {
	Success   bool
	RequestID int64
}

type ForwardSet struct {
	Request  SetRequest
	SourceID int
}

type TOMulticast struct {
	SequenceNumber int64
	Batch          []SetRequest
}

type RIQuery struct {
	RequestID int64
	SourceID  int
}

type RIResponse struct {
	LastSequence int64
	RequestID    int64
}

type ReformationInvite struct {
	SourceID int
}

type ReformationResponse struct {
	SourceID       int
	HighestApplied int64
}

type SequencerAnnouncement struct {
	SequencerID int
}

type Packet struct {
	MsgType MsgType
	Msg     interface{}
}

func init() {
	gob.Register(GetRequest{})
	gob.Register(SetRequest{})
	gob.Register(GetResponse{})
	gob.Register(SetResponse{})
	gob.Register(ForwardSet{})
	gob.Register(TOMulticast{})
	gob.Register(RIQuery{})
	gob.Register(RIResponse{})
	gob.Register(ReformationInvite{})
	gob.Register(ReformationResponse{})
	gob.Register(SequencerAnnouncement{})
}

type ConsistencyProtocol interface {
	HandleClientGet(req GetRequest) GetResponse
	HandleClientSet(req SetRequest) SetResponse
	HandleServerMsg(pac Packet) error
	InitElection()
}
