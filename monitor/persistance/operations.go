package persistance

import (
	"bytes"
	"encoding/binary"
	"fmt"

	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

const (
	RegisterNewMonitorOpID byte = 1
	RegisterNewWorkerOpID  byte = 2
)

type registerNewMonitorOperation struct {
	id        byte
	seq       uint64
	monitorID string
}

var _ pers.Operation = (*registerNewMonitorOperation)(nil)

func NewRegisterNewMonitorOperation(monitorID string) pers.Operation {
	return &registerNewMonitorOperation{
		id:        RegisterNewMonitorOpID,
		seq:       0,
		monitorID: monitorID,
	}
}

func (op *registerNewMonitorOperation) TypeID() byte {
	return op.id
}

func (op *registerNewMonitorOperation) SeqNumber() uint64 {
	return op.seq
}

func (op *registerNewMonitorOperation) ApplyTo(state pers.State) error {
	ms, ok := state.(*monitoreState)
	if !ok {
		return fmt.Errorf("invalid state type")
	}
	// Check if monitorID is already in the slice
	for _, id := range ms.monitorsSeen {
		if id == op.monitorID {
			return nil // already registered
		}
	}
	ms.monitorsSeen = append(ms.monitorsSeen, op.monitorID)
	return nil
}

func (op *registerNewMonitorOperation) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.seq); err != nil {
		return nil, err
	}
	if err := writeString(buf, op.monitorID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type registerNewWorkerOperation struct {
	id       byte
	seq      uint64
	workerID string
}

func NewRegisterNewWorkerOperation(workerID string) pers.Operation {
	return &registerNewWorkerOperation{
		id:       RegisterNewWorkerOpID,
		seq:      0,
		workerID: workerID,
	}
}

func (op *registerNewWorkerOperation) TypeID() byte {
	return op.id
}

func (op *registerNewWorkerOperation) SeqNumber() uint64 {
	return op.seq
}

func (op *registerNewWorkerOperation) ApplyTo(state pers.State) error {
	ms, ok := state.(*monitoreState)
	if !ok {
		return fmt.Errorf("invalid state type")
	}
	// Check if workerID is already in the slice
	for _, id := range ms.workersSeen {
		if id == op.workerID {
			return nil // already registered
		}
	}
	ms.workersSeen = append(ms.workersSeen, op.workerID)
	return nil
}

func (op *registerNewWorkerOperation) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.seq); err != nil {
		return nil, err
	}
	if err := writeString(buf, op.workerID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeRegisterNewMonitorOperation(id byte, seq uint64, data []byte) (pers.Operation, error) {
	monitorID, _, err := readString(data)
	if err != nil {
		return nil, err
	}
	return &registerNewMonitorOperation{id: id, seq: seq, monitorID: monitorID}, nil
}

func DecodeRegisterNewWorkerOperation(id byte, seq uint64, data []byte) (pers.Operation, error) {
	workerID, _, err := readString(data)
	if err != nil {
		return nil, err
	}
	return &registerNewWorkerOperation{id: id, seq: seq, workerID: workerID}, nil
}

func init() {
	pers.RegisterOperation(RegisterNewMonitorOpID, decodeRegisterNewMonitorEntry)
	pers.RegisterOperation(RegisterNewWorkerOpID, decodeRegisterNewWorkerEntry)
}

func decodeRegisterNewMonitorEntry(entry []byte) (pers.Operation, error) {
	id, seq, payload, err := parseEntry(entry)
	if err != nil {
		return nil, err
	}
	return DecodeRegisterNewMonitorOperation(id, seq, payload)
}

func decodeRegisterNewWorkerEntry(entry []byte) (pers.Operation, error) {
	id, seq, payload, err := parseEntry(entry)
	if err != nil {
		return nil, err
	}
	return DecodeRegisterNewWorkerOperation(id, seq, payload)
}

func parseEntry(entry []byte) (byte, uint64, []byte, error) {
	const headerSize = 1 + 8
	if len(entry) < headerSize {
		return 0, 0, nil, fmt.Errorf("entry too short: %d", len(entry))
	}
	id := entry[0]
	seq := binary.LittleEndian.Uint64(entry[1:9])
	return id, seq, entry[9:], nil
}

func writeString(buf *bytes.Buffer, value string) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}
	if _, err := buf.WriteString(value); err != nil {
		return err
	}
	return nil
}

func readString(data []byte) (string, []byte, error) {
	if len(data) < 4 {
		return "", nil, fmt.Errorf("invalid data length: %d", len(data))
	}
	length := binary.LittleEndian.Uint32(data[:4])
	remaining := len(data[4:])
	if int(length) > remaining {
		return "", nil, fmt.Errorf("string length %d exceeds payload %d", length, remaining)
	}
	start := 4
	end := start + int(length)
	return string(data[start:end]), data[end:], nil
}
