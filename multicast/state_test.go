package multicast

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testRecord(seq uint64, previous string, commands ...Command) Record {
	r := Record{Sequence: seq, Previous: previous, Batch: commands}
	r.Hash = r.Digest()
	return r
}
func TestOrderedDeliveryAndRecovery(t *testing.T) {
	dir := t.TempDir()
	s, e := OpenState(dir, "node-a/static-membership")
	if e != nil {
		t.Fatal(e)
	}
	first := testRecord(1, "", Command{"one", "Cache.Set", "key", "v1"})
	second := testRecord(2, first.Hash, Command{"two", "Cache.Set", "key", "v2"})
	if e = s.Deliver(second); !errors.Is(e, ErrGap) {
		t.Fatalf("out-of-order: %v", e)
	}
	if r, _ := s.Get("key"); r.Found {
		t.Fatal("future write visible before gap filled")
	}
	if e = s.Deliver(first); e != nil {
		t.Fatal(e)
	}
	if r, _ := s.Get("key"); r.Value != "v2" || r.Revision != 2 {
		t.Fatalf("not applied in order: %+v", r)
	}
	if e = s.Deliver(first); e != nil {
		t.Fatal("duplicate delivery failed", e)
	}
	conflict := testRecord(1, "", Command{"one", "Cache.Set", "key", "wrong"})
	if e = s.Deliver(conflict); e == nil {
		t.Fatal("conflicting sequence accepted")
	}
	retry := testRecord(3, second.Hash, Command{"one", "Cache.Set", "key", "v1"})
	if e = s.Deliver(retry); e != nil {
		t.Fatal(e)
	}
	if r, _ := s.Get("key"); r.Value != "v2" {
		t.Fatal("retry overwrote newer write")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = OpenState(dir, "changed-sequencer"); e == nil {
		t.Fatal("unsafe reconfiguration accepted")
	}
	s, e = OpenState(dir, "node-a/static-membership")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if r, ok, e := s.Receipt(first.Batch[0]); e != nil || !ok || r.Revision != 1 {
		t.Fatalf("lost durable receipt: %+v %v", r, e)
	}
	if r, _ := s.Get("key"); r.Value != "v2" {
		t.Fatal("lost durable value")
	}
	if _, _, e = s.Receipt(Command{"one", "Cache.Set", "key", "different"}); e != nil {
		t.Fatal(e)
	}
	if r, _, _ := s.Receipt(Command{"one", "Cache.Set", "key", "different"}); r.Error == "" {
		t.Fatal("conflicting ID accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e = s.Wait(ctx, 100); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("unbounded read wait", e)
	}
}
func TestDeliveryValidationAndBounds(t *testing.T) {
	s, e := OpenState(t.TempDir(), "one")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	invalid := testRecord(1, "", Command{"id", "Cache.Set", "bad key", "value"})
	if e = s.Deliver(invalid); e == nil {
		t.Fatal("invalid command accepted")
	}
	far := testRecord(129, "", Command{"id", "Cache.Set", "key", "value"})
	if e = s.Deliver(far); e == nil {
		t.Fatal("unbounded reorder buffer")
	}
	wrong := testRecord(1, "", Command{"id", "Cache.Set", "key", "value"})
	wrong.Hash = "wrong"
	if e = s.Deliver(wrong); e == nil {
		t.Fatal("corrupt record accepted")
	}
}
