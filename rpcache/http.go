package rpcache

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
)

type Request struct {
	Version string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  Params `json:"params"`
}
type Params struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
type Response struct {
	Version string    `json:"jsonrpc"`
	ID      string    `json:"id"`
	Result  *Result   `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}
type Service struct {
	node     *Node
	token    string
	slots    chan struct{}
	calls    atomic.Uint64
	failures atomic.Uint64
}

func NewService(n *Node, token string, concurrency int) (*Service, error) {
	if len(token) < 24 || concurrency < 1 {
		return nil, errors.New("token must have at least 24 characters; concurrency must be positive")
	}
	return &Service{node: n, token: token, slots: make(chan struct{}, concurrency)}, nil
}
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("alive\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.node.Raft.VerifyLeader().Error() != nil {
			http.Error(w, "not the ready leader", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte("# TYPE rpcache_requests_total counter\nrpcache_requests_total " + itoa(s.calls.Load()) + "\n# TYPE rpcache_errors_total counter\nrpcache_errors_total " + itoa(s.failures.Load()) + "\n"))
	})
	mux.HandleFunc("POST /rpc", s.serve)
	return mux
}
func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 20)
	for n > 0 {
		b = append(b, byte(n%10)+'0')
		n /= 10
	}
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return string(b)
}
func (s *Service) authorized(r *http.Request) bool {
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) == 1
}
func (s *Service) serve(w http.ResponseWriter, r *http.Request) {
	s.calls.Add(1)
	if !s.authorized(r) {
		s.failures.Add(1)
		http.Error(w, "unauthorized", 401)
		return
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.failures.Add(1)
		http.Error(w, "overloaded", 429)
		return
	}
	started := false
	defer func() {
		if !started {
			<-s.slots
		}
	}()
	var req Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32768))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.reply(w, "", nil, -32600, "invalid or oversized request")
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		s.reply(w, req.ID, nil, -32600, "one request per body is required")
		return
	}
	if req.Version != "2.0" || req.ID == "" || len(req.ID) > 128 {
		s.reply(w, req.ID, nil, -32600, "jsonrpc 2.0 and a string request ID are required")
		return
	}
	if req.Method != "Cache.Get" && req.Method != "Cache.Set" && req.Method != "Cache.Delete" {
		s.reply(w, req.ID, nil, -32601, "unknown method")
		return
	}
	if len(req.Params.Key) < 1 || len(req.Params.Key) > 200 || strings.IndexFunc(req.Params.Key, func(c rune) bool { return unicode.IsSpace(c) || unicode.IsControl(c) }) >= 0 || len(req.Params.Value) > 16384 {
		s.reply(w, req.ID, nil, -32602, "invalid key or value exceeds 16 KiB")
		return
	}
	// Bound the HTTP lifetime even if an accepted Raft operation outlives its caller.
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	type outcome struct {
		r Result
		e error
	}
	done := make(chan outcome, 1)
	started = true
	go func() {
		defer func() { <-s.slots }()
		if req.Method == "Cache.Get" {
			v, e := s.node.Read(req.Params.Key, 2*time.Second)
			done <- outcome{v, e}
		} else {
			v, e := s.node.Write(Command{req.ID, req.Method, req.Params.Key, req.Params.Value}, 2*time.Second)
			done <- outcome{v, e}
		}
	}()
	select {
	case out := <-done:
		if out.e != nil {
			s.reply(w, req.ID, nil, -32001, "unavailable or not leader; retry same request ID on elected leader")
			return
		}
		if out.r.Error != "" {
			s.reply(w, req.ID, nil, -32002, out.r.Error)
			return
		}
		s.reply(w, req.ID, &out.r, 0, "")
	case <-ctx.Done():
		s.reply(w, req.ID, nil, -32000, "deadline exceeded; write outcome may be unknown, reuse request ID")
	}
}
func (s *Service) reply(w http.ResponseWriter, id string, result *Result, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{Version: "2.0", ID: id, Result: result}
	if code != 0 {
		s.failures.Add(1)
		response.Error = &RPCError{code, message}
	}
	_ = json.NewEncoder(w).Encode(response)
}
