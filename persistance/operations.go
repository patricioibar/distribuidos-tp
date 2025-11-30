package persistance

import (
	"encoding/binary"
	"fmt"
)

var DeltaTypeID byte = 2

type Delta struct {
	ID            byte
	seq           uint64
	columnIndexes []int64
	deltas        []int64
}

func (op *Delta) ApplyTo(st State) error {
	st.Apply(op)
	return nil
}

func (op *Delta) SeqNumber() uint64 { return op.seq }

func (op *Delta) TypeID() byte { return op.ID }

func (op *Delta) Encode() ([]byte, error) {
	payload := make([]byte, 1+8+(8*len(op.columnIndexes))+(8*len(op.deltas)))
	payload[0] = op.TypeID()
	binary.LittleEndian.PutUint64(payload[1:9], op.seq)
	offset := 9
	for _, colIdx := range op.columnIndexes {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(colIdx))
		offset += 8
	}
	for _, delta := range op.deltas {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(delta))
		offset += 8
	}
	return payload, nil
}

func decodeDelta(data []byte) (Operation, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("invalid delta data length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	numColumns := (len(data) - 9) / 16
	columnIndexes := make([]int64, numColumns)
	deltas := make([]int64, numColumns)
	offset := 9
	for i := 0; i < numColumns; i++ {
		columnIndexes[i] = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}
	for i := 0; i < numColumns; i++ {
		deltas[i] = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}
	return &Delta{
		ID:            data[0],
		seq:           seq,
		columnIndexes: columnIndexes,
		deltas:        deltas,
	}, nil
}
