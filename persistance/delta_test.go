package persistance

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"testing"
)

type mockColumn struct {
	index int64
	name  string
	value int64
}

type mockTableState struct {
	columns map[int64]*mockColumn
}

func newMockTableState(cols []mockColumn) *mockTableState {
	state := &mockTableState{columns: make(map[int64]*mockColumn, len(cols))}
	for i := range cols {
		col := cols[i]
		copy := col // avoid referencing loop variable
		state.columns[col.index] = &copy
	}
	return state
}

func (s *mockTableState) Apply(op Operation) error {
	delta, ok := op.(*Delta)
	if !ok {
		return nil
	}
	for i, idx := range delta.columnIndexes {
		col := s.columns[idx]
		if col == nil {
			continue
		}
		col.value += delta.deltas[i]
	}
	return nil
}

func (s *mockTableState) Serialize() ([]byte, error) {
	var indexes []int64
	for idx := range s.columns {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(indexes))); err != nil {
		return nil, err
	}
	for _, idx := range indexes {
		col := s.columns[idx]
		if err := binary.Write(buf, binary.LittleEndian, idx); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, col.value); err != nil {
			return nil, err
		}
		nameBytes := []byte(col.name)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (s *mockTableState) Deserialize(data []byte) error {
	if len(data) == 0 {
		s.columns = map[int64]*mockColumn{}
		return nil
	}
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return err
	}
	cols := make(map[int64]*mockColumn, count)
	for i := uint32(0); i < count; i++ {
		var idx int64
		var value int64
		var nameLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &idx); err != nil {
			return err
		}
		if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
			return err
		}
		if err := binary.Read(reader, binary.LittleEndian, &nameLen); err != nil {
			return err
		}
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(reader, name); err != nil {
			return err
		}
		cols[idx] = &mockColumn{index: idx, name: string(name), value: value}
	}
	s.columns = cols
	return nil
}

func TestDeltaApplyToUpdatesColumns(t *testing.T) {
	state := newMockTableState([]mockColumn{
		{index: 1, name: "temperature", value: 25},
		{index: 2, name: "pressure", value: 100},
	})
	delta := &Delta{
		ID:            DeltaTypeID,
		seq:           7,
		columnIndexes: []int64{1, 2},
		deltas:        []int64{5, -20},
	}
	if err := delta.ApplyTo(state); err != nil {
		t.Fatalf("ApplyTo returned error: %v", err)
	}
	if got := state.columns[1].value; got != 30 {
		t.Fatalf("temperature column value mismatch: want 30, got %d", got)
	}
	if got := state.columns[2].value; got != 80 {
		t.Fatalf("pressure column value mismatch: want 80, got %d", got)
	}
	if name := state.columns[1].name; name != "temperature" {
		t.Fatalf("column name unexpectedly changed: %s", name)
	}
}

func TestDeltaEncodeDecodeRoundTrip(t *testing.T) {
	delta := &Delta{
		ID:            DeltaTypeID,
		seq:           99,
		columnIndexes: []int64{3, 4, 8},
		deltas:        []int64{-2, 5, 12},
	}
	encoded, err := delta.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	op, err := decodeDelta(encoded)
	if err != nil {
		t.Fatalf("decodeDelta failed: %v", err)
	}
	decoded, ok := op.(*Delta)
	if !ok {
		t.Fatalf("decoded operation type mismatch: %T", op)
	}
	if decoded.TypeID() != delta.TypeID() {
		t.Fatalf("TypeID mismatch: want %d, got %d", delta.TypeID(), decoded.TypeID())
	}
	if decoded.SeqNumber() != delta.SeqNumber() {
		t.Fatalf("SeqNumber mismatch: want %d, got %d", delta.SeqNumber(), decoded.SeqNumber())
	}
	if len(decoded.columnIndexes) != len(delta.columnIndexes) {
		t.Fatalf("column count mismatch: want %d, got %d", len(delta.columnIndexes), len(decoded.columnIndexes))
	}
	for i := range delta.columnIndexes {
		if decoded.columnIndexes[i] != delta.columnIndexes[i] {
			t.Fatalf("column index %d mismatch: want %d, got %d", i, delta.columnIndexes[i], decoded.columnIndexes[i])
		}
		if decoded.deltas[i] != delta.deltas[i] {
			t.Fatalf("delta %d mismatch: want %d, got %d", i, delta.deltas[i], decoded.deltas[i])
		}
	}
}

func TestStateManagerRestoreWithDeltaOperations(t *testing.T) {
	dir := t.TempDir()
	initialColumns := []mockColumn{
		{index: 1, name: "temperature", value: 10},
		{index: 2, name: "pressure", value: 50},
		{index: 3, name: "humidity", value: 70},
	}

	state := newMockTableState(initialColumns)
	stateLog, err := NewStateLog(dir)
	if err != nil {
		t.Fatalf("failed to create state log: %v", err)
	}
	sm, err := NewStateManager(state, stateLog, 2)
	if err != nil {
		t.Fatalf("failed to create state manager: %v", err)
	}

	ops := []*Delta{
		{ID: DeltaTypeID, seq: 1, columnIndexes: []int64{1}, deltas: []int64{5}},
		{ID: DeltaTypeID, seq: 2, columnIndexes: []int64{2}, deltas: []int64{-10}},
		{ID: DeltaTypeID, seq: 3, columnIndexes: []int64{1, 3}, deltas: []int64{3, -20}},
		{ID: DeltaTypeID, seq: 4, columnIndexes: []int64{2, 3}, deltas: []int64{7, 5}},
		{ID: DeltaTypeID, seq: 5, columnIndexes: []int64{1, 2, 3}, deltas: []int64{-2, 4, 1}},
	}
	for _, op := range ops {
		if err := sm.Log(op); err != nil {
			t.Fatalf("failed to log operation %d: %v", op.seq, err)
		}
	}

	expected := make(map[int64]int64, len(state.columns))
	for idx, col := range state.columns {
		expected[idx] = col.value
	}
	if err := sm.Close(); err != nil {
		t.Fatalf("failed to close state manager: %v", err)
	}

	uninitializedColumns := []mockColumn{
		{index: 1, name: "temperature", value: 0},
		{index: 2, name: "pressure", value: 0},
		{index: 3, name: "humidity", value: 0},
	}
	restoreState := newMockTableState(uninitializedColumns)
	restoreLog, err := NewStateLog(dir)
	if err != nil {
		t.Fatalf("failed to reopen state log: %v", err)
	}
	restoreManager := &StateManager{
		state:          restoreState,
		stateLog:       restoreLog,
		snapshotPeriod: 2,
	}
	defer restoreManager.Close()

	if err := restoreManager.Restore(); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	for idx, want := range expected {
		col := restoreState.columns[idx]
		if col == nil {
			t.Fatalf("missing column %d in restored state", idx)
		}
		if col.value != want {
			t.Fatalf("column %d value mismatch: want %d, got %d", idx, want, col.value)
		}
	}
}
