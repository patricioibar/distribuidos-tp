package persistence

import (
	"testing"

	"github.com/patricioibar/distribuidos-tp/bitmap"
)

func TestPersistentState_SerializeDeserialize(t *testing.T) {
	s := NewPersistentState()
	s.AggregatedData["k1"] = []interface{}{1.5, 2}
	s.AggregatedBatches.Add(11)
	s.AggregatedBatches.Add(22)
	s.EmptyBatches.Add(33)
	rb := bitmap.New()
	rb.Add(7)
	s.ReceivedAggregatorsBatches["worker-1"] = rb
	s.AggregatorsDone = []string{"worker-1"}
	s.DuplicatedBatches["worker-1"] = rb

	data, err := s.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	s2 := NewPersistentState()
	if err := s2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize error: %v", err)
	}

	// AggregatedData numbers will be decoded as float64 from JSON
	rd, ok := s2.AggregatedData["k1"]
	if !ok || len(rd) != 2 {
		t.Fatalf("AggregatedData missing or wrong: %#v", s2.AggregatedData)
	}
	if v, ok := rd[0].(float64); !ok || v != 1.5 {
		t.Fatalf("k1[0] mismatch, got %#v", rd[0])
	}
	if v, ok := rd[1].(float64); !ok || v != 2.0 {
		t.Fatalf("k1[1] mismatch, got %#v", rd[1])
	}

	if !s2.AggregatedBatches.Contains(11) || !s2.AggregatedBatches.Contains(22) {
		t.Fatalf("AggregatedBatches not restored: %v", s2.AggregatedBatches)
	}
	if !s2.EmptyBatches.Contains(33) {
		t.Fatalf("EmptyBatches not restored: %v", s2.EmptyBatches)
	}
	if rb2, ok := s2.ReceivedAggregatorsBatches["worker-1"]; !ok || !rb2.Contains(7) {
		t.Fatalf("ReceivedAggregatorsBatches not restored: %#v", s2.ReceivedAggregatorsBatches)
	}
	if len(s2.AggregatorsDone) != 1 || s2.AggregatorsDone[0] != "worker-1" {
		t.Fatalf("AggregatorsDone mismatch: %#v", s2.AggregatorsDone)
	}
	if rb2, ok := s2.DuplicatedBatches["worker-1"]; !ok || !rb2.Contains(7) {
		t.Fatalf("DuplicatedBatches not restored: %v", s2.DuplicatedBatches)
	}
}

func TestPersistentState_AggregateBatchOp(t *testing.T) {
	s := NewPersistentState()
	data := map[string][]interface{}{
		"k1": {1.5, 2},
		"k2": {3},
	}
	op := NewAggregateBatchOp(10, data)
	if err := s.Apply(op); err != nil {
		t.Fatalf("aggregateBatchOp error: %v", err)
	}

	rd, ok := s.AggregatedData["k1"]
	if !ok || len(rd) != 2 {
		t.Fatalf("AggregatedData k1 missing or wrong: %#v", s.AggregatedData)
	}
	if v, ok := rd[0].(float64); !ok || v != 1.5 {
		t.Fatalf("k1[0] mismatch, got %#v", rd[0])
	}
	if v, ok := rd[1].(int); !ok || v != int(2) {
		t.Fatalf("k1[1] mismatch, got %#v", rd[1])
	}
	rk2, ok := s.AggregatedData["k2"]
	if !ok || len(rk2) != 1 {
		t.Fatalf("AggregatedData k2 missing or wrong: %#v", s.AggregatedData)
	}
	if v, ok := rk2[0].(int); !ok || v != int(3) {
		t.Fatalf("k2[0] mismatch, got %#v", rk2[0])
	}
}
func TestPersistentState_AddEmptyBatchesOp(t *testing.T) {
	s := NewPersistentState()
	b := bitmap.New()
	b.Add(10)
	b.Add(20)
	op := NewAddEmptyBatchesOp(1, b)
	if err := s.Apply(op); err != nil {
		t.Fatalf("apply AddEmptyBatchesOp error: %v", err)
	}
	if !s.EmptyBatches.Contains(10) || !s.EmptyBatches.Contains(20) {
		t.Fatalf("EmptyBatches not updated: %v", s.EmptyBatches)
	}
}

func TestPersistentState_ReceiveAggregatorBatchOp(t *testing.T) {
	s := NewPersistentState()
	data := map[string][]interface{}{
		"k": {1.5, 2},
	}
	op := NewReceiveAggregatorBatchOp(7, "worker-1", data)
	if err := s.Apply(op); err != nil {
		t.Fatalf("apply ReceiveAggregatorBatchOp error: %v", err)
	}
	// Received batches tracking
	if rb, ok := s.ReceivedAggregatorsBatches["worker-1"]; !ok || !rb.Contains(7) {
		t.Fatalf("ReceivedAggregatorsBatches not updated: %#v", s.ReceivedAggregatorsBatches)
	}
	// AggregatedData should contain the values
	rd, ok := s.AggregatedData["k"]
	if !ok || len(rd) != 2 {
		t.Fatalf("AggregatedData missing or wrong: %#v", s.AggregatedData)
	}
	if v, ok := rd[0].(float64); !ok || v != 1.5 {
		t.Fatalf("k[0] mismatch: %#v", rd[0])
	}
	if v, ok := rd[1].(int); !ok || v != 2 {
		t.Fatalf("k[1] mismatch: %#v", rd[1])
	}
}

func TestPersistentState_AddAggregatorDoneOp(t *testing.T) {
	s := NewPersistentState()
	// existing aggregated batch
	s.AggregatedBatches.Add(5)
	seqs := bitmap.New()
	seqs.Add(5)
	seqs.Add(6)
	op := NewAddAggregatorDoneOp(8, "worker-1", seqs)
	if err := s.Apply(op); err != nil {
		t.Fatalf("apply AddAggregatorDoneOp error: %v", err)
	}
	// aggregated batches now include 6
	if !s.AggregatedBatches.Contains(6) {
		t.Fatalf("AggregatedBatches missing 6: %v", s.AggregatedBatches)
	}
	// duplicated batches for worker-1 should contain 5
	if db, ok := s.DuplicatedBatches["worker-1"]; !ok || !db.Contains(5) {
		t.Fatalf("DuplicatedBatches not set correctly: %#v", s.DuplicatedBatches)
	}
}

func TestPersistentState_RevertBatchAggregationOp(t *testing.T) {
	s := NewPersistentState()
	s.AggregatedData["k1"] = []interface{}{5.0}
	// duplicated batches contains seq 9 for aggregator "agg"
	s.DuplicatedBatches["agg"] = bitmap.New()
	s.DuplicatedBatches["agg"].Add(9)

	data := map[string][]interface{}{"k1": {2.0}}
	op := NewRevertBatchAggregationOp(9, "agg", data)
	if err := s.Apply(op); err != nil {
		t.Fatalf("apply RevertBatchAggregationOp error: %v", err)
	}
	// duplicated entry removed
	if db, ok := s.DuplicatedBatches["agg"]; !ok {
		t.Fatalf("DuplicatedBatches missing aggregator: %#v", s.DuplicatedBatches)
	} else if db.Contains(9) {
		t.Fatalf("DuplicatedBatches still contains reverted seq: %v", db)
	}
	// aggregated data reduced by 2.0 -> 3.0
	rd, ok := s.AggregatedData["k1"]
	if !ok || len(rd) == 0 {
		t.Fatalf("AggregatedData missing after revert: %#v", s.AggregatedData)
	}
	if v, ok := rd[0].(float64); !ok || v != 3.0 {
		t.Fatalf("k1[0] mismatch after revert: %#v", rd[0])
	}
}
