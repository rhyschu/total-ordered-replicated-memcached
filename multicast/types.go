package multicast

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode"
)

const MaxKeys = 10000
const DedupWindow = 10000
const MaxRecords = 100000
const MaxBatch = 16

var ErrUnavailable = errors.New("sequencer or a configured replica is unavailable")
var ErrGap = errors.New("ordered record is waiting for an earlier sequence")

type Command struct {
	ID    string `json:"id"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Validate() error {
	if c.ID == "" || len(c.ID) > 128 || (c.Op != "Cache.Set" && c.Op != "Cache.Delete") || len(c.Key) == 0 || len(c.Key) > 200 || len(c.Value) > 16384 || strings.IndexFunc(c.Key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return errors.New("invalid ordered command")
	}
	return nil
}

type Result struct {
	Value    string `json:"value,omitempty"`
	Found    bool   `json:"found"`
	Revision uint64 `json:"revision"`
	Error    string `json:"error,omitempty"`
}
type Record struct {
	Sequence uint64    `json:"sequence"`
	Previous string    `json:"previous"`
	Hash     string    `json:"hash"`
	Batch    []Command `json:"batch"`
}

func (r Record) Digest() string {
	r.Hash = ""
	b, _ := json.Marshal(r)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type Packet struct {
	Token    string   `json:"token,omitempty"`
	Sender   string   `json:"sender,omitempty"`
	Kind     string   `json:"kind"`
	Command  *Command `json:"command,omitempty"`
	Record   *Record  `json:"record,omitempty"`
	Sequence uint64   `json:"sequence,omitempty"`
	Hash     string   `json:"hash,omitempty"`
	Result   Result   `json:"result,omitempty"`
	Error    string   `json:"error,omitempty"`
}
