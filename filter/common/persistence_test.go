package filter

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

func TestFilterStateSerializeDeserialize(t *testing.T) {
	fs := NewFilterState()
	fs.TotallyFiltered.Add(10)
	fs.TotallyFiltered.Add(42)
	fs.SeenBatches.Add(7)

	data, err := fs.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	fs2 := NewFilterState()
	if err := fs2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if !fs2.TotallyFiltered.Contains(10) || !fs2.TotallyFiltered.Contains(42) {
		t.Fatalf("TotallyFiltered missing values after deserialize: %v", fs2.TotallyFiltered)
	}
	if !fs2.SeenBatches.Contains(7) {
		t.Fatalf("SeenBatches missing value after deserialize: %v", fs2.SeenBatches)
	}
}

func TestAddBitmapOpEncodeDecodeApply(t *testing.T) {
	// build operation
	values := []uint64{1, 2, 3}
	seq := uint64(12345)
	op := NewAddBitmapOp(seq, targetTotallyFiltered, values)

	enc, err := op.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// decode using decodeAddBitmapOp present in the package
	decodedOpI, err := decodeAddBitmapOp(enc)
	if err != nil {
		t.Fatalf("decodeAddBitmapOp failed: %v", err)
	}
	decodedOp, ok := decodedOpI.(*AddBitmapOp)
	if !ok {
		t.Fatalf("decoded op has wrong type: %T", decodedOpI)
	}
	if decodedOp.SeqNumber() != seq {
		t.Fatalf("seq mismatch: got %d want %d", decodedOp.SeqNumber(), seq)
	}

	// apply to state
	fs := NewFilterState()
	if err := fs.Apply(decodedOp); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	for _, v := range values {
		if !fs.TotallyFiltered.Contains(v) {
			t.Fatalf("value %d not found in bitmap after apply", v)
		}
	}
}

func TestStateManagerIntegrationWithAddBitmapOp(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestStateManagerIntegrationWithAddBitmapOp*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	// ensure manual cleanup because TempDir auto-cleanup can race with file handles on Windows
	defer func() {
		os.RemoveAll(dir)
	}()

	// create state log
	sl, err := pers.NewStateLog(dir)
	if err != nil {
		t.Fatalf("NewStateLog failed: %v", err)
	}

	// initial manager with empty state
	fs := NewFilterState()
	sm, err := pers.NewStateManager(fs, sl, 1000)
	if err != nil {
		t.Fatalf("NewStateManager failed: %v", err)
	}

	// log an operation
	op := NewAddBitmapOp(1, targetTotallyFiltered, []uint64{55, 66})
	if err := sm.Log(op); err != nil {
		t.Fatalf("sm.Log failed: %v", err)
	}

	// create fresh state and restore using StateManager.Restore
	fs2 := NewFilterState()
	sm2, err := pers.NewStateManager(fs2, sl, 1000)
	if err != nil {
		t.Fatalf("NewStateManager failed for restore: %v", err)
	}
	if err := sm2.Restore(); err != nil {
		t.Fatalf("StateManager.Restore failed: %v", err)
	}

	if !fs2.TotallyFiltered.Contains(55) || !fs2.TotallyFiltered.Contains(66) {
		t.Fatalf("restored state missing values: %v", fs2.TotallyFiltered)
	}

	// ensure state manager resources are closed before test exit
	if err := sm.Close(); err != nil {
		t.Fatalf("failed to close state manager: %v", err)
	}
	// allow OS to release file handles on Windows before TempDir cleanup
	time.Sleep(50 * time.Millisecond)

	// verify we can retrieve op by seq number through state manager helper
	gotOp, err := sm.GetOperationWithSeqNumber(op.SeqNumber())
	if err != nil {
		t.Fatalf("GetOperationWithSeqNumber failed: %v", err)
	}
	if gotOp == nil {
		t.Fatalf("GetOperationWithSeqNumber returned nil for seq %d", op.SeqNumber())
	}
	// verify encoded first byte equals type id
	enc, _ := gotOp.Encode()
	if len(enc) == 0 || enc[0] != addBitmapTypeID {
		t.Fatalf("retrieved op has wrong type id: %v", enc)
	}

	// also check seq bytes in the encoded payload match
	seqFromEnc := binary.LittleEndian.Uint64(enc[1:9])
	if seqFromEnc != gotOp.SeqNumber() {
		t.Fatalf("seq in encoded payload mismatch: got %d want %d", seqFromEnc, gotOp.SeqNumber())
	}
}
