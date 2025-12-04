package common

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/patricioibar/distribuidos-tp/bitmap"

	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

type JoinerState struct {
	RightSeqRecv *bitmap.Bitmap `json:"right_seq_recv,omitempty"`
	LeftSeqRecv  *bitmap.Bitmap `json:"left_seq_recv,omitempty"`
	RightCache   *TableCache    `json:"right_cache,omitempty"`
}

func NewJoinerState() *JoinerState {
	return &JoinerState{
		RightSeqRecv: bitmap.New(),
		LeftSeqRecv:  bitmap.New(),
		RightCache:   nil,
	}
}

// ---- Persistence (implements pers.State) ----

// Serialize encodes the two bitmaps as JSON using the JSONBitmap wrapper
func (js *JoinerState) Serialize() ([]byte, error) {
	type wrapper struct {
		RightSeqRecv *bitmap.JSONBitmap `json:"right_seq_recv"`
		LeftSeqRecv  *bitmap.JSONBitmap `json:"left_seq_recv"`
		RightCache   *TableCache        `json:"right_cache,omitempty"`
	}
	w := wrapper{
		RightSeqRecv: &bitmap.JSONBitmap{Bitmap: js.RightSeqRecv},
		LeftSeqRecv:  &bitmap.JSONBitmap{Bitmap: js.LeftSeqRecv},
		RightCache:   js.RightCache,
	}
	return json.Marshal(w)
}

// Deserialize decodes JSON produced by Serialize
func (js *JoinerState) Deserialize(data []byte) error {
	type wrapper struct {
		RightSeqRecv *bitmap.JSONBitmap `json:"right_seq_recv"`
		LeftSeqRecv  *bitmap.JSONBitmap `json:"left_seq_recv"`
		RightCache   *TableCache        `json:"right_cache,omitempty"`
	}
	var w wrapper
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	if w.RightSeqRecv != nil {
		js.RightSeqRecv = w.RightSeqRecv.Bitmap
	} else {
		js.RightSeqRecv = bitmap.New()
	}
	if w.LeftSeqRecv != nil {
		js.LeftSeqRecv = w.LeftSeqRecv.Bitmap
	} else {
		js.LeftSeqRecv = bitmap.New()
	}
	// restore right cache if present
	if w.RightCache != nil {
		js.RightCache = w.RightCache
	} else {
		js.RightCache = nil
	}
	return nil
}

// Apply applies an operation to the joiner state. Currently supports AddBitmapOp
func (js *JoinerState) Apply(op pers.Operation) error {
	switch op.TypeID() {
	case addBitmapTypeIDJoiner:
		abo, ok := op.(*AddBitmapOpJoiner)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", addBitmapTypeIDJoiner)
		}
		switch abo.Target {
		case targetRightSeqRecv:
			if js.RightSeqRecv == nil {
				js.RightSeqRecv = bitmap.New()
			}
			for _, v := range abo.Values {
				js.RightSeqRecv.Add(v)
			}
		case targetLeftSeqRecv:
			if js.LeftSeqRecv == nil {
				js.LeftSeqRecv = bitmap.New()
			}
			for _, v := range abo.Values {
				js.LeftSeqRecv.Add(v)
			}
		default:
			return fmt.Errorf("unknown AddBitmapJoiner target: %d", abo.Target)
		}
		return nil
	case addRightCacheTypeID:
		aro, ok := op.(*AddRightCacheOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", addRightCacheTypeID)
		}
		// create right cache if needed
		if js.RightCache == nil {
			tc, err := NewTableCache(aro.Columns)
			if err != nil {
				return err
			}
			js.RightCache = tc
		}
		// append rows
		for _, row := range aro.Rows {
			if err := js.RightCache.AddRow(row); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported operation TypeID: %d", op.TypeID())
	}
}

// ------------------------------------------------------------------
// Operation to add values to one of the bitmaps in JoinerState
// ------------------------------------------------------------------

const (
	// choose IDs unlikely to collide with other ops
	addBitmapTypeIDJoiner byte = 110
	targetRightSeqRecv    byte = 1
	targetLeftSeqRecv     byte = 2
)

type AddBitmapOpJoiner struct {
	ID     byte
	seq    uint64
	Target byte
	Values []uint64
}

func NewAddBitmapOp(seq uint64, target byte, values []uint64) *AddBitmapOpJoiner {
	return &AddBitmapOpJoiner{ID: addBitmapTypeIDJoiner, seq: seq, Target: target, Values: values}
}

func (op *AddBitmapOpJoiner) TypeID() byte      { return op.ID }
func (op *AddBitmapOpJoiner) SeqNumber() uint64 { return op.seq }

// Encode layout: [typeID(1)][seq(8)][target(1)][count(8)][values... 8 bytes each]
func (op *AddBitmapOpJoiner) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(op.TypeID()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, op.seq); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(op.Target); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(op.Values))); err != nil {
		return nil, err
	}
	for _, v := range op.Values {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (op *AddBitmapOpJoiner) ApplyTo(st pers.State) error {
	return st.Apply(op)
}

func decodeAddBitmapOpJoiner(data []byte) (pers.Operation, error) {
	if len(data) < 1+8+1+8 {
		return nil, fmt.Errorf("invalid addBitmap op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	target := data[9]
	count := binary.LittleEndian.Uint64(data[10:18])
	expected := 1 + 8 + 1 + 8 + 8*int(count)
	if len(data) != expected {
		return nil, fmt.Errorf("unexpected addBitmap payload length: got %d, want %d", len(data), expected)
	}
	values := make([]uint64, count)
	offset := 18
	for i := 0; i < int(count); i++ {
		values[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
	}
	return &AddBitmapOpJoiner{ID: data[0], seq: seq, Target: target, Values: values}, nil
}

// ------------------------------------------------------------------
// Operation to create/populate the RightCache (columns + rows)
// ------------------------------------------------------------------

const (
	addRightCacheTypeID byte = 111
)

type AddRightCacheOp struct {
	ID      byte
	seq     uint64
	Columns []string
	Rows    [][]interface{}
}

func NewAddRightCacheOp(seq uint64, columns []string, rows [][]interface{}) *AddRightCacheOp {
	return &AddRightCacheOp{ID: addRightCacheTypeID, seq: seq, Columns: columns, Rows: rows}
}

func (op *AddRightCacheOp) TypeID() byte      { return op.ID }
func (op *AddRightCacheOp) SeqNumber() uint64 { return op.seq }

// Encode layout: [typeID(1)][seq(8)][jsonPayload]
func (op *AddRightCacheOp) Encode() ([]byte, error) {
	payload := struct {
		Columns []string        `json:"columns"`
		Rows    [][]interface{} `json:"rows"`
	}{Columns: op.Columns, Rows: op.Rows}
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

func (op *AddRightCacheOp) ApplyTo(st pers.State) error {
	// Delegate to state's Apply if implemented, but provide direct application too
	return st.Apply(op)
}

func decodeAddRightCacheOp(data []byte) (pers.Operation, error) {
	if len(data) < 1+8 {
		return nil, fmt.Errorf("invalid addRightCache op length: %d", len(data))
	}
	seq := binary.LittleEndian.Uint64(data[1:9])
	payload := data[9:]
	var p struct {
		Columns []string        `json:"columns"`
		Rows    [][]interface{} `json:"rows"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	return &AddRightCacheOp{ID: data[0], seq: seq, Columns: p.Columns, Rows: p.Rows}, nil
}

func init() {
	// register operations
	pers.RegisterOperation(addBitmapTypeIDJoiner, func(b []byte) (pers.Operation, error) { return decodeAddBitmapOpJoiner(b) })
	pers.RegisterOperation(addRightCacheTypeID, func(b []byte) (pers.Operation, error) { return decodeAddRightCacheOp(b) })
}
