package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

const (
	AggregateBatchTypeID         byte = 201
	AddEmptyBatchesTypeID        byte = 202
	ReceiveAggregatorBatchTypeID byte = 203
	AddAggregatorDoneTypeID      byte = 204
	RevertBatchAggregationTypeID byte = 205
	SetRecoveringTypeID          byte = 206
)

type AggregateBatchOp struct {
	ID   byte
	Seq  uint64
	Data map[string][]interface{}
}

func NewAggregateBatchOp(seq uint64, data map[string][]interface{}) *AggregateBatchOp {
	return &AggregateBatchOp{ID: AggregateBatchTypeID, Seq: seq, Data: data}
}
func (op *AggregateBatchOp) TypeID() byte                { return op.ID }
func (op *AggregateBatchOp) SeqNumber() uint64           { return op.Seq }
func (op *AggregateBatchOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *AggregateBatchOp) Encode() ([]byte, error) {
	payload := op.Data
	jb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.Seq); err != nil {
		return nil, err
	}
	if _, err := buf.Write(jb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeAggregateBatchOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid aggregateBatch op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	payload := data[9:]
	var p map[string][]interface{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
	} else {
		p = make(map[string][]interface{})
	}
	return &AggregateBatchOp{ID: data[0], Seq: seq, Data: p}, nil
}

type AddEmptyBatchesOp struct {
	ID               byte
	seq              uint64
	EmptyBatchesSeqs *bitmap.Bitmap
}

func NewAddEmptyBatchesOp(seq uint64, emptyBatchesSeqs *bitmap.Bitmap) *AddEmptyBatchesOp {
	return &AddEmptyBatchesOp{ID: AddEmptyBatchesTypeID, seq: seq, EmptyBatchesSeqs: emptyBatchesSeqs}
}
func (op *AddEmptyBatchesOp) TypeID() byte                { return op.ID }
func (op *AddEmptyBatchesOp) SeqNumber() uint64           { return op.seq }
func (op *AddEmptyBatchesOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *AddEmptyBatchesOp) Encode() ([]byte, error) {
	// encode as JSON array of seqs
	vals := []uint64{}
	if op.EmptyBatchesSeqs != nil {
		vals = op.EmptyBatchesSeqs.ToArray()
	}
	jb, err := json.Marshal(struct {
		Vals []uint64 `json:"vals"`
	}{Vals: vals})
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.seq); err != nil {
		return nil, err
	}
	if _, err := buf.Write(jb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeAddEmptyBatchesOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid addEmptyBatches op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	payload := data[9:]
	var p struct {
		Vals []uint64 `json:"vals"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
	}
	b := bitmap.New()
	for _, v := range p.Vals {
		b.Add(v)
	}
	return &AddEmptyBatchesOp{ID: data[0], seq: seq, EmptyBatchesSeqs: b}, nil
}

type ReceiveAggregatorBatchOp struct {
	ID           byte
	Seq          uint64
	AggregatorID string
	Data         map[string][]interface{}
}

func NewReceiveAggregatorBatchOp(seq uint64, aggregatorID string, data map[string][]interface{}) *ReceiveAggregatorBatchOp {
	return &ReceiveAggregatorBatchOp{ID: ReceiveAggregatorBatchTypeID, Seq: seq, AggregatorID: aggregatorID, Data: data}
}
func (op *ReceiveAggregatorBatchOp) TypeID() byte                { return op.ID }
func (op *ReceiveAggregatorBatchOp) SeqNumber() uint64           { return op.Seq }
func (op *ReceiveAggregatorBatchOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *ReceiveAggregatorBatchOp) Encode() ([]byte, error) {
	payload := struct {
		AggregatorID string                   `json:"aggregator_id"`
		Data         map[string][]interface{} `json:"data"`
	}{AggregatorID: op.AggregatorID, Data: op.Data}
	jb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.Seq); err != nil {
		return nil, err
	}
	if _, err := buf.Write(jb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeReceiveAggregatorBatchOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid receiveAggregatorBatch op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	payload := data[9:]
	var p struct {
		AggregatorID string                   `json:"aggregator_id"`
		Data         map[string][]interface{} `json:"data"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
	}
	return &ReceiveAggregatorBatchOp{ID: data[0], Seq: seq, AggregatorID: p.AggregatorID, Data: p.Data}, nil
}

type AddAggregatorDoneOp struct {
	ID             byte
	seq            uint64
	AggregatorDone string
	Seqs           *bitmap.Bitmap
}

func NewAddAggregatorDoneOp(seq uint64, aggregatorDone string, seqs *bitmap.Bitmap) *AddAggregatorDoneOp {
	return &AddAggregatorDoneOp{ID: AddAggregatorDoneTypeID, seq: seq, AggregatorDone: aggregatorDone, Seqs: seqs}
}
func (op *AddAggregatorDoneOp) TypeID() byte                { return op.ID }
func (op *AddAggregatorDoneOp) SeqNumber() uint64           { return op.seq }
func (op *AddAggregatorDoneOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *AddAggregatorDoneOp) Encode() ([]byte, error) {
	payload := struct {
		AggregatorDone string   `json:"aggregator_done"`
		Seqs           []uint64 `json:"seqs"`
	}{AggregatorDone: op.AggregatorDone}
	if op.Seqs != nil {
		payload.Seqs = op.Seqs.ToArray()
	} else {
		payload.Seqs = []uint64{}
	}
	jb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.seq); err != nil {
		return nil, err
	}
	if _, err := buf.Write(jb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeAddAggregatorDoneOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid addAggregatorDone op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	payload := data[9:]
	var p struct {
		AggregatorDone string   `json:"aggregator_done"`
		Seqs           []uint64 `json:"seqs"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
	}
	b := bitmap.New()
	for _, v := range p.Seqs {
		b.Add(v)
	}
	return &AddAggregatorDoneOp{ID: data[0], seq: seq, AggregatorDone: p.AggregatorDone, Seqs: b}, nil
}

type RevertBatchAggregationOp struct {
	ID         byte
	Seq        uint64
	Aggregator string
	Data       map[string][]interface{}
}

func NewRevertBatchAggregationOp(seq uint64, aggregator string, data map[string][]interface{}) *RevertBatchAggregationOp {
	return &RevertBatchAggregationOp{ID: RevertBatchAggregationTypeID, Seq: seq, Aggregator: aggregator, Data: data}
}
func (op *RevertBatchAggregationOp) TypeID() byte                { return op.ID }
func (op *RevertBatchAggregationOp) SeqNumber() uint64           { return op.Seq }
func (op *RevertBatchAggregationOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *RevertBatchAggregationOp) Encode() ([]byte, error) {
	payload := struct {
		Aggregator string                   `json:"aggregator"`
		Data       map[string][]interface{} `json:"data"`
	}{Aggregator: op.Aggregator, Data: op.Data}
	jb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.Seq); err != nil {
		return nil, err
	}
	if _, err := buf.Write(jb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeRevertBatchAggregationOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid revertBatchAggregation op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	var p struct {
		Aggregator string                   `json:"aggregator"`
		Data       map[string][]interface{} `json:"data"`
	}
	payload := data[9:]
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
	} else {
		p.Data = make(map[string][]interface{})
	}
	return &RevertBatchAggregationOp{ID: data[0], Seq: seq, Aggregator: p.Aggregator, Data: p.Data}, nil
}

type SetRecoveringOp struct {
	ID    byte
	Seq   uint64
	Value bool
}

func NewSetRecoveringOp(seq uint64, value bool) *SetRecoveringOp {
	return &SetRecoveringOp{ID: SetRecoveringTypeID, Seq: seq, Value: value}
}
func (op *SetRecoveringOp) TypeID() byte                { return op.ID }
func (op *SetRecoveringOp) SeqNumber() uint64           { return op.Seq }
func (op *SetRecoveringOp) ApplyTo(st pers.State) error { return st.Apply(op) }
func (op *SetRecoveringOp) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.Seq); err != nil {
		return nil, err
	}
	var valByte byte = 0
	if op.Value {
		valByte = 1
	}
	if err := buf.WriteByte(valByte); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func decodeStartRecoveringOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8+1 {
		return nil, fmt.Errorf("invalid startRecovering op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	valByte := data[9]
	var value bool
	if valByte == 1 {
		value = true
	} else {
		value = false
	}
	return &SetRecoveringOp{ID: data[0], Seq: seq, Value: value}, nil
}

func init() {
	// register operations
	pers.RegisterOperation(AggregateBatchTypeID, decodeAggregateBatchOp)
	pers.RegisterOperation(AddEmptyBatchesTypeID, decodeAddEmptyBatchesOp)
	pers.RegisterOperation(ReceiveAggregatorBatchTypeID, decodeReceiveAggregatorBatchOp)
	pers.RegisterOperation(AddAggregatorDoneTypeID, decodeAddAggregatorDoneOp)
	pers.RegisterOperation(RevertBatchAggregationTypeID, decodeRevertBatchAggregationOp)
	pers.RegisterOperation(SetRecoveringTypeID, decodeStartRecoveringOp)
}
