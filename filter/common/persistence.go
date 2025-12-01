package filter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

type FilterState struct {
	TotallyFiltered *bitmap.Bitmap `json:"totally_filtered,omitempty"`
	SeenBatches     *bitmap.Bitmap `json:"seen_batches,omitempty"`
}

func NewFilterState() *FilterState {
	return &FilterState{
		TotallyFiltered: bitmap.New(),
		SeenBatches:     bitmap.New(),
	}
}

// Serialize implements pers.State by encoding the two bitmaps as JSON
func (fs *FilterState) Serialize() ([]byte, error) {
	// Use JSONBitmap wrapper to get base64 binary representation
	type wrapper struct {
		TotallyFiltered *bitmap.JSONBitmap `json:"totally_filtered"`
		SeenBatches     *bitmap.JSONBitmap `json:"seen_batches"`
	}
	w := wrapper{
		TotallyFiltered: &bitmap.JSONBitmap{Bitmap: fs.TotallyFiltered},
		SeenBatches:     &bitmap.JSONBitmap{Bitmap: fs.SeenBatches},
	}
	return json.Marshal(w)
}

// Deserialize implements pers.State by decoding JSON produced by Serialize
func (fs *FilterState) Deserialize(data []byte) error {
	type wrapper struct {
		TotallyFiltered *bitmap.JSONBitmap `json:"totally_filtered"`
		SeenBatches     *bitmap.JSONBitmap `json:"seen_batches"`
	}
	var w wrapper
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	if w.TotallyFiltered != nil {
		fs.TotallyFiltered = w.TotallyFiltered.Bitmap
	} else {
		fs.TotallyFiltered = bitmap.New()
	}
	if w.SeenBatches != nil {
		fs.SeenBatches = w.SeenBatches.Bitmap
	} else {
		fs.SeenBatches = bitmap.New()
	}
	return nil
}

// Apply implements pers.State: apply an Operation produced/registered via pers.RegisterOperation
func (fs *FilterState) Apply(op pers.Operation) error {
	switch op.TypeID() {
	case addBitmapTypeID:
		abo, ok := op.(*AddBitmapOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", addBitmapTypeID)
		}
		switch abo.Target {
		case targetTotallyFiltered:
			if fs.TotallyFiltered == nil {
				fs.TotallyFiltered = bitmap.New()
			}
			for _, v := range abo.Values {
				fs.TotallyFiltered.Add(v)
			}
		case targetSeenBatches:
			if fs.SeenBatches == nil {
				fs.SeenBatches = bitmap.New()
			}
			for _, v := range abo.Values {
				fs.SeenBatches.Add(v)
			}
		default:
			return fmt.Errorf("unknown AddBitmap target: %d", abo.Target)
		}
		return nil
	default:
		return fmt.Errorf("unsupported operation TypeID: %d", op.TypeID())
	}
}

// ------------------------------------------------------------------
// Operation to add values to one of the bitmaps in FilterState
// ------------------------------------------------------------------

const (
	// choose IDs unlikely to collide with existing ones
	addBitmapTypeID       byte = 100
	targetTotallyFiltered byte = 1
	targetSeenBatches     byte = 2
)

type AddBitmapOp struct {
	ID     byte
	seq    uint64
	Target byte
	Values []uint64
}

func NewAddBitmapOp(seq uint64, target byte, values []uint64) *AddBitmapOp {
	return &AddBitmapOp{ID: addBitmapTypeID, seq: seq, Target: target, Values: values}
}

func (op *AddBitmapOp) TypeID() byte      { return op.ID }
func (op *AddBitmapOp) SeqNumber() uint64 { return op.seq }

// Encode layout: [typeID(1)][seq(8)][target(1)][count(8)][values... 8 bytes each]
func (op *AddBitmapOp) Encode() ([]byte, error) {
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

func (op *AddBitmapOp) ApplyTo(st pers.State) error {
	// follow existing pattern: delegate to state's Apply
	return st.Apply(op)
}

func decodeAddBitmapOp(data []byte) (pers.Operation, error) {
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
	return &AddBitmapOp{ID: data[0], seq: seq, Target: target, Values: values}, nil
}

func init() {
	// register our operation decoder with the persistance package
	pers.RegisterOperation(addBitmapTypeID, func(b []byte) (pers.Operation, error) { return decodeAddBitmapOp(b) })
}
