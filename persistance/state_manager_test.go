package persistance

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"
)

const testOperationTypeID = byte(1)

func init() {
	RegisterOperation(testOperationTypeID, decodeTestOperation)
}

type testState struct {
	value int64
}

func (s *testState) Apply(op Operation) error {
	testOp, ok := op.(*testOperation)
	if !ok {
		return fmt.Errorf("unexpected operation type: %T", op)
	}
	s.value += testOp.delta
	return nil
}

func (s *testState) Serialize() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(s.value))
	return buf, nil
}

func (s *testState) Deserialize(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("invalid state data length: %d", len(data))
	}
	s.value = int64(binary.LittleEndian.Uint64(data))
	return nil
}

type testOperation struct {
	seq   uint64
	delta int64
}

func (op *testOperation) TypeID() byte { return testOperationTypeID }

func (op *testOperation) SeqNumber() uint64 { return op.seq }

func (op *testOperation) Encode() ([]byte, error) {
	buf := make([]byte, 1+8+8)
	buf[0] = op.TypeID()
	binary.LittleEndian.PutUint64(buf[1:9], op.seq)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(op.delta))
	return buf, nil
}

func (op *testOperation) ApplyTo(st State) error {
	testState, ok := st.(*testState)
	if !ok {
		return fmt.Errorf("unexpected state type: %T", st)
	}
	testState.value += op.delta
	return nil
}

func decodeTestOperation(data []byte) (Operation, error) {
	if len(data) < 17 {
		return nil, fmt.Errorf("test operation payload too short: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	delta := int64(binary.LittleEndian.Uint64(data[9:17]))
	return &testOperation{seq: seq, delta: delta}, nil
}

func TestStateManagerLogAndRestore(t *testing.T) {
	dir := t.TempDir()
	state := &testState{}
	stateLog, err := NewStateLog(dir)
	if err != nil {
		t.Fatalf("failed to create state log: %v", err)
	}
	defer stateLog.Close()
	sm, err := NewStateManager(state, stateLog, 2)
	if err != nil {
		t.Fatalf("failed to create state manager: %v", err)
	}
	defer sm.Close()

	ops := []*testOperation{
		{seq: 1, delta: 5},
		{seq: 2, delta: -1},
		{seq: 3, delta: 2},
	}
	for _, op := range ops {
		if err := sm.Log(op); err != nil {
			t.Fatalf("failed to log operation %d: %v", op.seq, err)
		}
	}

	if want, got := int64(6), state.value; got != want {
		t.Fatalf("expected state value %d, got %d", want, got)
	}

	iter, err := stateLog.GetLogsIterator()
	if err != nil {
		t.Fatalf("failed to get wal iterator: %v", err)
	}
	defer iter.Close()

	var decoded []*testOperation
	for {
		entry, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("iterator failed: %v", err)
		}
		if entry == nil {
			continue
		}
		op, err := sm.decodeOperation(entry)
		if err != nil {
			t.Fatalf("failed to decode entry: %v", err)
		}
		decoded = append(decoded, op.(*testOperation))
	}

	if len(decoded) != len(ops) {
		t.Fatalf("expected %d logged entries, got %d", len(ops), len(decoded))
	}
	for i, want := range ops {
		if got := decoded[i]; got.seq != want.seq || got.delta != want.delta {
			t.Fatalf("entry %d mismatch: want %+v, got %+v", i, want, got)
		}
	}

	state.value = 0
	if err := sm.Restore(); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if want, got := int64(6), state.value; got != want {
		t.Fatalf("expected state value after restore %d, got %d", want, got)
	}
}

func TestStateManagerGetOperationWithSeqNumber(t *testing.T) {
	dir := t.TempDir()
	state := &testState{}
	stateLog, err := NewStateLog(dir)
	if err != nil {
		t.Fatalf("failed to create state log: %v", err)
	}
	defer stateLog.Close()
	sm, err := NewStateManager(state, stateLog, 100)
	if err != nil {
		t.Fatalf("failed to create state manager: %v", err)
	}
	defer sm.Close()

	op := &testOperation{seq: 42, delta: 99}
	if err := sm.Log(op); err != nil {
		t.Fatalf("failed to log test operation: %v", err)
	}

	found, err := sm.GetOperationWithSeqNumber(42)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if found == nil {
		t.Fatalf("operation with seq 42 not found")
	}
	foundOp, ok := found.(*testOperation)
	if !ok {
		t.Fatalf("unexpected operation type: %T", found)
	}
	if foundOp.seq != op.seq || foundOp.delta != op.delta {
		t.Fatalf("found operation mismatch: want %+v, got %+v", op, foundOp)
	}
}
