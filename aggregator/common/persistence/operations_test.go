package persistence

import (
	"reflect"
	"testing"

	"github.com/patricioibar/distribuidos-tp/bitmap"
)

func TestAggregateBatchOp_EncodeDecode(t *testing.T) {
	data := map[string][]interface{}{"k": {3}, "n": {1.5}}
	op := NewAggregateBatchOp(42, data)
	b, err := op.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	gotI, err := decodeAggregateBatchOp(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	got := gotI.(*AggregateBatchOp)
	if got.Seq != 42 || got.ID != AggregateBatchTypeID {
		t.Fatalf("header mismatch: got seq=%d id=%d", got.Seq, got.ID)
	}
	kArr, ok := got.Data["k"]
	if !ok || len(kArr) == 0 {
		t.Fatalf("data.k missing or wrong type: %#v", got.Data["k"])
	}
	if kf, ok := kArr[0].(float64); !ok || kf != 3.0 {
		t.Fatalf("data.k mismatch: %#v", kArr[0])
	}
	nArr, ok := got.Data["n"]
	if !ok || len(nArr) == 0 {
		t.Fatalf("data.n missing or wrong type: %#v", got.Data["n"])
	}
	if n, ok := nArr[0].(float64); !ok || n != 1.5 {
		t.Fatalf("data.n mismatch: %#v", nArr[0])
	}
}

func TestAddEmptyBatchesOp_EncodeDecode(t *testing.T) {
	bm := bitmap.New()
	bm.Add(3)
	bm.Add(5)
	op := NewAddEmptyBatchesOp(7, bm)
	b, err := op.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	gotI, err := decodeAddEmptyBatchesOp(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	got := gotI.(*AddEmptyBatchesOp)
	if got.seq != 7 || got.ID != AddEmptyBatchesTypeID {
		t.Fatalf("header mismatch: got seq=%d id=%d", got.seq, got.ID)
	}
	if !got.EmptyBatchesSeqs.Contains(3) || !got.EmptyBatchesSeqs.Contains(5) {
		t.Fatalf("bitmap mismatch: %v", got.EmptyBatchesSeqs.ToArray())
	}
}

func TestReceiveAggregatorBatchOp_EncodeDecode(t *testing.T) {
	data := map[string][]interface{}{"a": {3}, "x": {2.0}}
	op := NewReceiveAggregatorBatchOp(9, "worker-1", data)
	b, err := op.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	gotI, err := decodeReceiveAggregatorBatchOp(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	got := gotI.(*ReceiveAggregatorBatchOp)
	if got.Seq != 9 || got.ID != ReceiveAggregatorBatchTypeID {
		t.Fatalf("header mismatch: got seq=%d id=%d", got.Seq, got.ID)
	}
	if got.AggregatorID != "worker-1" {
		t.Fatalf("aggregator id mismatch: %s", got.AggregatorID)
	}
	aArr, ok := got.Data["a"]
	if !ok || len(aArr) == 0 {
		t.Fatalf("data.a missing or wrong type: %#v", got.Data["a"])
	}
	if af, ok := aArr[0].(float64); !ok || af != 3.0 {
		t.Fatalf("data.a mismatch: %#v", aArr[0])
	}
	xArr, ok := got.Data["x"]
	if !ok || len(xArr) == 0 {
		t.Fatalf("data.x missing or wrong type: %#v", got.Data["x"])
	}
	if xf, ok := xArr[0].(float64); !ok || xf != 2.0 {
		t.Fatalf("data.x mismatch: %#v", xArr[0])
	}
}

func TestAddAggregatorDoneOp_EncodeDecode(t *testing.T) {
	bm := bitmap.New()
	bm.Add(10)
	op := NewAddAggregatorDoneOp(11, "worker-1", bm)
	b, err := op.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	gotI, err := decodeAddAggregatorDoneOp(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	got := gotI.(*AddAggregatorDoneOp)
	if got.seq != 11 || got.ID != AddAggregatorDoneTypeID {
		t.Fatalf("header mismatch: got seq=%d id=%d", got.seq, got.ID)
	}
	if got.AggregatorDone != "worker-1" {
		t.Fatalf("aggregator done mismatch: %s", got.AggregatorDone)
	}
	if !got.Seqs.Contains(10) {
		t.Fatalf("seqs bitmap mismatch: %v", got.Seqs.ToArray())
	}
}

func TestRevertBatchAggregationOp_EncodeDecode(t *testing.T) {
	data := map[string][]interface{}{"foo": {"bar"}}
	op := NewRevertBatchAggregationOp(12, "agg", data)
	b, err := op.Encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	gotI, err := decodeRevertBatchAggregationOp(b)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	got := gotI.(*RevertBatchAggregationOp)
	if got.Seq != 12 || got.ID != RevertBatchAggregationTypeID {
		t.Fatalf("header mismatch: got seq=%d id=%d", got.Seq, got.ID)
	}
	if got.Aggregator != "agg" {
		t.Fatalf("aggregator mismatch: got=%q want=%q", got.Aggregator, "agg")
	}
	if !reflect.DeepEqual(got.Data, data) {
		t.Fatalf("data mismatch: got=%#v want=%#v", got.Data, data)
	}
}
