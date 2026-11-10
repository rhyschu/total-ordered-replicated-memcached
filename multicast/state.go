package multicast

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type State struct {
	db      *bolt.DB
	mu      sync.Mutex
	pending map[uint64]Record
	changed chan struct{}
	closed  bool
}
type storedValue struct {
	Text     string
	Revision uint64
}
type receipt struct {
	Command Command
	Result  Result
}

var bucketNames = []string{"meta", "values", "log", "receipts", "receipt-order"}

func number(n uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, n); return b }
func integer(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
func OpenState(dir, identity string) (*State, error) {
	if _, e := os.Stat(filepath.Join(dir, "raft.db")); e == nil {
		return nil, errors.New("directory contains an incompatible database; choose a new empty data directory")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "ordered.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	s := &State{db: db, pending: map[uint64]Record{}, changed: make(chan struct{})}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range bucketNames {
			if _, e := tx.CreateBucketIfNotExists([]byte(name)); e != nil {
				return e
			}
		}
		meta := tx.Bucket([]byte("meta"))
		prior := meta.Get([]byte("identity"))
		if prior != nil && string(prior) != identity {
			return errors.New("stored node/membership/sequencer identity differs; refusing unsafe reconfiguration")
		}
		return meta.Put([]byte("identity"), []byte(identity))
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *State) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.changed)
	return s.db.Close()
}
func (s *State) Position() (seq uint64, hash string, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket([]byte("meta"))
		seq = integer(m.Get([]byte("sequence")))
		hash = string(m.Get([]byte("hash")))
		return nil
	})
	return
}
func (s *State) Record(seq uint64) (r Record, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log")).Get(number(seq))
		if b == nil {
			return fmt.Errorf("record %d missing", seq)
		}
		return json.Unmarshal(b, &r)
	})
	return
}
func (s *State) HashAt(seq uint64) (string, error) {
	if seq == 0 {
		return "", nil
	}
	r, e := s.Record(seq)
	return r.Hash, e
}
func (s *State) Get(key string) (r Result, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("values")).Get([]byte(key))
		if b == nil {
			return nil
		}
		var v storedValue
		if e := json.Unmarshal(b, &v); e != nil {
			return e
		}
		r = Result{Value: v.Text, Found: true, Revision: v.Revision}
		return nil
	})
	return
}
func (s *State) Receipt(c Command) (r Result, found bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("receipts")).Get([]byte(c.ID))
		if b == nil {
			return nil
		}
		var prior receipt
		if e := json.Unmarshal(b, &prior); e != nil {
			return e
		}
		found = true
		if prior.Command != c {
			r.Error = "request ID reused with different input"
		} else {
			r = prior.Result
		}
		return nil
	})
	return
}
func (s *State) Wait(ctx context.Context, index uint64) error {
	for {
		s.mu.Lock()
		changed := s.changed
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return errors.New("state closed")
		}
		seq, _, err := s.Position()
		if err != nil {
			return err
		}
		if seq >= index {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (s *State) Deliver(record Record) error {
	if record.Sequence == 0 || len(record.Batch) == 0 || len(record.Batch) > MaxBatch || record.Hash != record.Digest() {
		return errors.New("invalid ordered record")
	}
	for _, c := range record.Batch {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	// Copy caller-owned slices before buffering. Caller mutation cannot alter a
	// future record after its digest has been checked.
	record.Batch = append([]Command(nil), record.Batch...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("state closed")
	}
	seq, _, err := s.Position()
	if err != nil {
		return err
	}
	if record.Sequence <= seq {
		hash, e := s.HashAt(record.Sequence)
		if e != nil {
			return e
		}
		if hash != record.Hash {
			return errors.New("conflicting record at applied sequence")
		}
		return nil
	}
	if record.Sequence > seq+128 {
		return errors.New("out-of-order window exceeded")
	}
	if prior, ok := s.pending[record.Sequence]; ok && prior.Hash != record.Hash {
		return errors.New("conflicting buffered record")
	}
	s.pending[record.Sequence] = record
	for {
		next, ok := s.pending[seq+1]
		if !ok {
			break
		}
		if err = s.apply(next); err != nil {
			delete(s.pending, next.Sequence)
			return err
		}
		seq = next.Sequence
		delete(s.pending, seq)
		close(s.changed)
		s.changed = make(chan struct{})
	}
	if seq < record.Sequence {
		return ErrGap
	}
	return nil
}
func (s *State) apply(record Record) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		seq := integer(meta.Get([]byte("sequence")))
		if record.Sequence != seq+1 || record.Previous != string(meta.Get([]byte("hash"))) {
			return errors.New("record does not extend the applied history")
		}
		if record.Sequence > MaxRecords {
			return errors.New("journal capacity reached; operator maintenance required")
		}
		values := tx.Bucket([]byte("values"))
		receipts := tx.Bucket([]byte("receipts"))
		order := tx.Bucket([]byte("receipt-order"))
		count := integer(meta.Get([]byte("keys")))
		writes := integer(meta.Get([]byte("writes")))
		for _, c := range record.Batch {
			if b := receipts.Get([]byte(c.ID)); b != nil {
				continue
			}
			result := Result{Revision: record.Sequence}
			switch c.Op {
			case "Cache.Set":
				exists := values.Get([]byte(c.Key)) != nil
				if !exists && count >= MaxKeys {
					result.Error = "key capacity reached"
				} else {
					b, _ := json.Marshal(storedValue{Text: c.Value, Revision: record.Sequence})
					if e := values.Put([]byte(c.Key), b); e != nil {
						return e
					}
					if !exists {
						count++
					}
					result.Found = true
				}
			case "Cache.Delete":
				result.Found = values.Get([]byte(c.Key)) != nil
				if result.Found {
					if e := values.Delete([]byte(c.Key)); e != nil {
						return e
					}
					count--
				}
			}
			b, _ := json.Marshal(receipt{Command: c, Result: result})
			if e := receipts.Put([]byte(c.ID), b); e != nil {
				return e
			}
			writes++
			if e := order.Put(number(writes), []byte(c.ID)); e != nil {
				return e
			}
			if writes > DedupWindow {
				old := number(writes - DedupWindow)
				if e := receipts.Delete(order.Get(old)); e != nil {
					return e
				}
				if e := order.Delete(old); e != nil {
					return e
				}
			}
		}
		data, _ := json.Marshal(record)
		if e := tx.Bucket([]byte("log")).Put(number(record.Sequence), data); e != nil {
			return e
		}
		for key, value := range map[string][]byte{"sequence": number(record.Sequence), "hash": []byte(record.Hash), "keys": number(count), "writes": number(writes)} {
			if e := meta.Put([]byte(key), value); e != nil {
				return e
			}
		}
		return nil
	})
}
