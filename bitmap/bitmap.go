package bitmap

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// Bitmap is a simple uint64 set backed by a map. It implements a subset of
// the interface provided by roaring.Bitmap that the project expects.
type Bitmap struct {
	m map[uint64]struct{}
}

func New() *Bitmap {
	return &Bitmap{m: make(map[uint64]struct{})}
}

// compatibility alias
func NewBitmap() *Bitmap { return New() }

func (b *Bitmap) Add(x uint64) { b.m[x] = struct{}{} }

func (b *Bitmap) Contains(x uint64) bool {
	if b == nil {
		return false
	}
	_, ok := b.m[x]
	return ok
}

func (b *Bitmap) GetCardinality() uint64 {
	if b == nil {
		return 0
	}
	return uint64(len(b.m))
}

func (b *Bitmap) Or(other *Bitmap) {
	if b == nil || other == nil {
		return
	}
	for k := range other.m {
		b.m[k] = struct{}{}
	}
}

// And returns a new Bitmap with the intersection
func And(a, c *Bitmap) *Bitmap {
	if a == nil || c == nil {
		return New()
	}
	// iterate smaller map
	res := New()
	if len(a.m) < len(c.m) {
		for k := range a.m {
			if _, ok := c.m[k]; ok {
				res.m[k] = struct{}{}
			}
		}
	} else {
		for k := range c.m {
			if _, ok := a.m[k]; ok {
				res.m[k] = struct{}{}
			}
		}
	}
	return res
}

// And is a receiver that returns intersection with another as a new Bitmap
func (b *Bitmap) And(other *Bitmap) *Bitmap {
	return And(b, other)
}

// RemoveRange removes values v such that start <= v < end
func (b *Bitmap) RemoveRange(start, end uint64) {
	if b == nil {
		return
	}
	for k := range b.m {
		if k >= start && k < end {
			delete(b.m, k)
		}
	}
}

func (b *Bitmap) ToArray() []uint64 {
	if b == nil {
		return []uint64{}
	}
	out := make([]uint64, 0, len(b.m))
	for k := range b.m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Maximum returns the maximum element; panics if empty (to mimic roaring)
func (b *Bitmap) Maximum() uint64 {
	if b == nil || len(b.m) == 0 {
		panic("Maximum called on empty Bitmap")
	}
	var max uint64
	first := true
	for k := range b.m {
		if first || k > max {
			max = k
			first = false
		}
	}
	return max
}

// WriteTo writes a binary representation: len(uint64) followed by values
func (b *Bitmap) WriteTo(w io.Writer) (int64, error) {
	var buf bytes.Buffer
	arr := b.ToArray()
	// write length
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(arr))); err != nil {
		return 0, err
	}
	for _, v := range arr {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return 0, err
		}
	}
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the binary format written by WriteTo
func (b *Bitmap) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	var length uint64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return total, err
	}
	total += 8
	for i := uint64(0); i < length; i++ {
		var v uint64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return total, err
		}
		total += 8
		if b.m == nil {
			b.m = make(map[uint64]struct{})
		}
		b.m[v] = struct{}{}
	}
	return total, nil
}

// MarshalBinary returns binary encoding
func (b *Bitmap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary parses binary encoding
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	b.m = make(map[uint64]struct{})
	_, err := b.ReadFrom(bytes.NewReader(data))
	return err
}

type JSONBitmap struct{ *Bitmap }

func (r *JSONBitmap) MarshalJSON() ([]byte, error) {
	if r.Bitmap == nil {
		return []byte("null"), nil
	}
	data, err := r.Bitmap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	return json.Marshal(encoded)
}

func (r *JSONBitmap) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	r.Bitmap = New()
	return r.Bitmap.UnmarshalBinary(decoded)
}

func (b *Bitmap) AddRange(start, end uint64) {
	for i := start; i < end; i++ {
		b.Add(i)
	}
}

// String for debugging
func (b *Bitmap) String() string {
	return fmt.Sprintf("Bitmap%v", b.ToArray())
}
