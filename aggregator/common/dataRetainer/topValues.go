package dataretainer

import (
	a "aggregator/common/aggFunctions"
	"container/heap"
	"sort"

	cmp "cmp"
)

// Entry almacena una Key arbitraria (interface{}) y un Value ordenable
type Entry[V cmp.Ordered] struct {
	Key   interface{}
	Aggs  []a.Aggregation
	Value V
}

type entryHeap[V cmp.Ordered] struct {
	items   []Entry[V]
	largest bool // true => guardamos los N mayores
}

func (h entryHeap[V]) Len() int      { return len(h.items) }
func (h entryHeap[V]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h entryHeap[V]) Less(i, j int) bool {
	if h.largest {
		return h.items[i].Value < h.items[j].Value // min-heap para top mayores
	}
	return h.items[i].Value > h.items[j].Value // max-heap para top menores
}
func (h *entryHeap[V]) Push(x interface{}) { h.items = append(h.items, x.(Entry[V])) }
func (h *entryHeap[V]) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

type TopN[V cmp.Ordered] struct {
	h        *entryHeap[V]
	capacity int
}

func NewTopN[V cmp.Ordered](capacity int, largest bool) *TopN[V] {
	if capacity <= 0 {
		capacity = 1
	}
	h := &entryHeap[V]{items: make([]Entry[V], 0, capacity), largest: largest}
	heap.Init(h)
	return &TopN[V]{h: h, capacity: capacity}
}

func (t *TopN[V]) Insert(e Entry[V]) {
	if t.h.Len() < t.capacity {
		heap.Push(t.h, e)
		return
	}
	root := t.h.items[0]
	if t.h.largest {
		if e.Value > root.Value {
			t.h.items[0] = e
			heap.Fix(t.h, 0)
		}
	} else {
		if e.Value < root.Value {
			t.h.items[0] = e
			heap.Fix(t.h, 0)
		}
	}
}

func (t *TopN[V]) Values() []Entry[V] {
	out := make([]Entry[V], len(t.h.items))
	copy(out, t.h.items)
	if t.h.largest {
		sort.Slice(out, func(i, j int) bool { return out[i].Value > out[j].Value })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	}
	return out
}
