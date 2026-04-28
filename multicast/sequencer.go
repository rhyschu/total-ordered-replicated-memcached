package multicast

import (
	"sync"
	"time"
)

type Sequencer struct {
	Mutex         sync.Mutex
	CurrentSeq    int64
	Batch         []SetRequest
	BatchSize     int
	MulticastChan chan TOMulticast
	Timeout   	  time.Duration
	Timer         *time.Timer
}

func NewSequencer(batchSize int, timeout time.Duration) *Sequencer {
	s := &Sequencer{CurrentSeq: 0, BatchSize: batchSize, Timeout: timeout, MulticastChan: make(chan TOMulticast, 100), Batch: make([]SetRequest, 0, batchSize)}
	return s
}

func (s *Sequencer) AddSet(req SetRequest) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.Batch = append(s.Batch, req)
	if len(s.Batch) >= s.BatchSize {
		s.TriggerMulticast()
	} else if len(s.Batch) == 1 {
		s.ResetTimer()
	}
}

func (s *Sequencer) TriggerMulticast() {
	if len(s.Batch) == 0 {
		return
	}
	s.CurrentSeq++
	msg := TOMulticast{SequenceNumber: s.CurrentSeq, Batch: s.Batch}
	s.Batch = make([]SetRequest, 0, s.BatchSize)
	if s.Timer != nil {
		s.Timer.Stop()
	}
	s.MulticastChan <- msg
}

func (s *Sequencer) ResetTimer() {
	if s.Timer != nil {
		s.Timer.Stop()
	}
	s.Timer = time.AfterFunc(s.Timeout, func() {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		s.TriggerMulticast()
	})
}

func (s *Sequencer) GetCurrentSeq() int64 {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if len(s.Batch) > 0 {
		return s.CurrentSeq + 1
	}
	return s.CurrentSeq
}
