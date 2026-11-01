package rpcache

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const MaxKeys = 10000
const DedupWindow = 10000

type Command struct {
	ID    string `json:"id"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
type Value struct {
	Text     string `json:"text"`
	Revision uint64 `json:"revision"`
}
type Result struct {
	Value    string `json:"value,omitempty"`
	Found    bool   `json:"found"`
	Revision uint64 `json:"revision"`
	Error    string `json:"error,omitempty"`
}
type receipt struct {
	Command Command
	Result  Result
}
type image struct {
	Values   map[string]Value
	Receipts map[string]receipt
	Order    []string
}
type State struct {
	mu   sync.RWMutex
	data image
}

func NewState() *State {
	return &State{data: image{Values: map[string]Value{}, Receipts: map[string]receipt{}}}
}
func (s *State) Apply(log *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return Result{Error: "invalid command"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.data.Receipts[c.ID]; ok {
		if prior.Command != c {
			return Result{Error: "request ID reused with different input"}
		}
		return prior.Result
	}
	r := Result{Revision: log.Index}
	switch c.Op {
	case "Cache.Set":
		if _, exists := s.data.Values[c.Key]; !exists && len(s.data.Values) >= MaxKeys {
			r.Error = "key capacity reached"
		} else {
			s.data.Values[c.Key] = Value{c.Value, log.Index}
			r.Found = true
		}
	case "Cache.Delete":
		_, r.Found = s.data.Values[c.Key]
		delete(s.data.Values, c.Key)
	default:
		r.Error = "invalid operation"
	}
	if len(s.data.Order) >= DedupWindow {
		delete(s.data.Receipts, s.data.Order[0])
		s.data.Order = s.data.Order[1:]
	}
	s.data.Order = append(s.data.Order, c.ID)
	s.data.Receipts[c.ID] = receipt{c, r}
	return r
}
func (s *State) Get(key string) Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Values[key]
	return Result{Value: v.Text, Found: ok, Revision: v.Revision}
}

type snapshot struct{ bytes []byte }

func (s *State) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := json.Marshal(s.data)
	return &snapshot{b}, err
}
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.bytes); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *snapshot) Release() {}
func (s *State) Restore(r io.ReadCloser) error {
	defer r.Close()
	var data image
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	if data.Values == nil {
		data.Values = map[string]Value{}
	}
	if data.Receipts == nil {
		data.Receipts = map[string]receipt{}
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}
