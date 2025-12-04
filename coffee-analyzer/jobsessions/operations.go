package jobsessions

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/patricioibar/distribuidos-tp/persistance"
)

const uploadTableOperationTypeID byte = 1
const startResponsesOperationTypeID byte = 2
const finishUploadOperationTypeID byte = 3

type uploadTableOperation struct {
	ID        byte
	seq       uint64
	sessionID uuid.UUID
	tableName string
}

type startResponsesOperation struct {
	ID        byte
	seq       uint64
	sessionID uuid.UUID
}

type finishUploadOperation struct {
	ID        byte
	seq       uint64
	sessionID uuid.UUID
	tableName string
}

var _ persistance.Operation = (*uploadTableOperation)(nil)
var _ persistance.Operation = (*startResponsesOperation)(nil)
var _ persistance.Operation = (*finishUploadOperation)(nil)

func NewUploadTableOperation(sessionID uuid.UUID, tableName string) *uploadTableOperation {
	return &uploadTableOperation{
		ID:        uploadTableOperationTypeID,
		seq:       0,
		sessionID: sessionID,
		tableName: tableName,
	}
}

func (op *uploadTableOperation) TypeID() byte { return op.ID }

func (op *uploadTableOperation) SeqNumber() uint64 { return op.seq }

func (op *uploadTableOperation) Encode() ([]byte, error) {
	nameBytes := []byte(op.tableName)
	if len(nameBytes) > 1<<32-1 {
		return nil, fmt.Errorf("table name too long: %d", len(nameBytes))
	}
	sessionBytes, err := op.sessionID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1+8+16+4+len(nameBytes))
	buf[0] = op.TypeID()
	binary.LittleEndian.PutUint64(buf[1:9], op.seq)
	copy(buf[9:25], sessionBytes)
	binary.LittleEndian.PutUint32(buf[25:29], uint32(len(nameBytes)))
	copy(buf[29:], nameBytes)
	return buf, nil
}

func (op *uploadTableOperation) ApplyTo(st persistance.State) error {
	jss, ok := st.(*JobSessionsState)
	if !ok {
		return fmt.Errorf("invalid state type %T", st)
	}
	session := jss.GetOrCreateSession(op.sessionID)
	session.AddNewTable(op.tableName)
	return nil
}

func NewStartResponsesOperation(sessionID uuid.UUID) *startResponsesOperation {
	return &startResponsesOperation{
		ID:        startResponsesOperationTypeID,
		seq:       0,
		sessionID: sessionID,
	}
}

func (op *startResponsesOperation) TypeID() byte { return op.ID }

func (op *startResponsesOperation) SeqNumber() uint64 { return op.seq }

func (op *startResponsesOperation) Encode() ([]byte, error) {
	sessionBytes, err := op.sessionID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1+8+16)
	buf[0] = op.TypeID()
	binary.LittleEndian.PutUint64(buf[1:9], op.seq)
	copy(buf[9:], sessionBytes)
	return buf, nil
}

func (op *startResponsesOperation) ApplyTo(st persistance.State) error {
	jss, ok := st.(*JobSessionsState)
	if !ok {
		return fmt.Errorf("invalid state type %T", st)
	}
	session, exists := jss.GetSession(op.sessionID)
	if !exists {
		return fmt.Errorf("session not found: %v", op.sessionID)
	}
	session.MarkStartedReceivingResponses()
	session.UpdateLastActivity(time.Now().Unix())
	return nil
}

func NewFinishUploadOperation(sessionID uuid.UUID, tableName string) *finishUploadOperation {
	return &finishUploadOperation{
		ID:        finishUploadOperationTypeID,
		seq:       0,
		sessionID: sessionID,
		tableName: tableName,
	}
}

func (op *finishUploadOperation) TypeID() byte { return op.ID }

func (op *finishUploadOperation) SeqNumber() uint64 { return op.seq }

func (op *finishUploadOperation) Encode() ([]byte, error) {
	nameBytes := []byte(op.tableName)
	if len(nameBytes) > 1<<32-1 {
		return nil, fmt.Errorf("table name too long: %d", len(nameBytes))
	}
	sessionBytes, err := op.sessionID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1+8+16+4+len(nameBytes))
	buf[0] = op.TypeID()
	binary.LittleEndian.PutUint64(buf[1:9], op.seq)
	copy(buf[9:25], sessionBytes)
	binary.LittleEndian.PutUint32(buf[25:29], uint32(len(nameBytes)))
	copy(buf[29:], nameBytes)
	return buf, nil
}

func (op *finishUploadOperation) ApplyTo(st persistance.State) error {
	jss, ok := st.(*JobSessionsState)
	if !ok {
		return fmt.Errorf("invalid state type %T", st)
	}
	session, exists := jss.GetSession(op.sessionID)
	if !exists {
		return fmt.Errorf("session not found: %v", op.sessionID)
	}
	session.MarkTableAsUploaded(op.tableName)
	return nil
}

func decodeUploadTableOperation(data []byte) (persistance.Operation, error) {
	if len(data) < 1+8+16+4 {
		return nil, fmt.Errorf("upload table entry too short: %d", len(data))
	}
	if data[0] != uploadTableOperationTypeID {
		return nil, fmt.Errorf("unexpected operation type: %d", data[0])
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	sessionBytes := data[9 : 9+16]
	var sessionID uuid.UUID
	if err := sessionID.UnmarshalBinary(sessionBytes); err != nil {
		return nil, err
	}
	nameLen := binary.LittleEndian.Uint32(data[25:29])
	if int(29+nameLen) > len(data) {
		return nil, fmt.Errorf("invalid table name length %d", nameLen)
	}
	name := string(data[29 : 29+nameLen])
	return &uploadTableOperation{
		ID:        data[0],
		seq:       seq,
		sessionID: sessionID,
		tableName: name,
	}, nil
}

func decodeStartResponsesOperation(data []byte) (persistance.Operation, error) {
	if len(data) < 1+8+16 {
		return nil, fmt.Errorf("start responses entry too short: %d", len(data))
	}
	if data[0] != startResponsesOperationTypeID {
		return nil, fmt.Errorf("unexpected operation type: %d", data[0])
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	var sessionID uuid.UUID
	if err := sessionID.UnmarshalBinary(data[9 : 9+16]); err != nil {
		return nil, err
	}
	return &startResponsesOperation{
		ID:        data[0],
		seq:       seq,
		sessionID: sessionID,
	}, nil
}

func decodeFinishUploadOperation(data []byte) (persistance.Operation, error) {
	if len(data) < 1+8+16+4 {
		return nil, fmt.Errorf("finish upload entry too short: %d", len(data))
	}
	if data[0] != finishUploadOperationTypeID {
		return nil, fmt.Errorf("unexpected operation type: %d", data[0])
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	var sessionID uuid.UUID
	if err := sessionID.UnmarshalBinary(data[9 : 9+16]); err != nil {
		return nil, err
	}
	nameLen := binary.LittleEndian.Uint32(data[25:29])
	if int(29+nameLen) > len(data) {
		return nil, fmt.Errorf("invalid table name length %d", nameLen)
	}
	name := string(data[29 : 29+nameLen])
	return &finishUploadOperation{
		ID:        data[0],
		seq:       seq,
		sessionID: sessionID,
		tableName: name,
	}, nil
}

func init() {
	persistance.RegisterOperation(uploadTableOperationTypeID, decodeUploadTableOperation)
	persistance.RegisterOperation(startResponsesOperationTypeID, decodeStartResponsesOperation)
	persistance.RegisterOperation(finishUploadOperationTypeID, decodeFinishUploadOperation)
}
