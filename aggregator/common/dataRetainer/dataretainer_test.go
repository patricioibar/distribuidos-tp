package dataretainer

import (
	"reflect"
	"testing"
)

// helper: extrae los Values de []Entry[int]
func valuesOf(entries []Entry[int]) []int {
	out := make([]int, len(entries))
	for i, e := range entries {
		out[i] = e.Value
	}
	return out
}

func TestTopN_FiveLargest(t *testing.T) {
	top := NewTopN[int](5, true) // top 5 mayores
	inputs := []Entry[int]{
		{Key: "a", Value: 7},
		{Key: "b", Value: 1},
		{Key: "c", Value: 5},
		{Key: "d", Value: 3},
		{Key: "e", Value: 12},
		{Key: "f", Value: 9},
		{Key: "g", Value: 20},
		{Key: "h", Value: 2},
		{Key: "i", Value: 15},
	}
	for _, e := range inputs {
		top.Insert(e)
	}
	got := valuesOf(top.Values())
	want := []int{20, 15, 12, 9, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FiveLargest: got %v, want %v", got, want)
	}
}

func TestTopN_ThreeSmallest(t *testing.T) {
	top := NewTopN[int](3, false) // top 3 menores
	inputs := []Entry[int]{
		{Key: "a", Value: 7},
		{Key: "b", Value: 1},
		{Key: "c", Value: 5},
		{Key: "d", Value: 3},
		{Key: "e", Value: 12},
		{Key: "f", Value: 9},
		{Key: "g", Value: 20},
		{Key: "h", Value: 2},
		{Key: "i", Value: 15},
	}
	for _, e := range inputs {
		top.Insert(e)
	}
	got := valuesOf(top.Values())
	want := []int{1, 2, 3} // ascending
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ThreeSmallest: got %v, want %v", got, want)
	}
}

func TestTopN_OneLargest(t *testing.T) {
	top := NewTopN[int](1, true) // solo 1 mayor
	inputs := []Entry[int]{
		{Key: "a", Value: 10},
		{Key: "b", Value: 20},
		{Key: "c", Value: 15},
	}
	for _, e := range inputs {
		top.Insert(e)
	}
	got := valuesOf(top.Values())
	want := []int{20}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OneLargest: got %v, want %v", got, want)
	}
}

func TestTopN_ThreeSmallestWithDuplicates(t *testing.T) {
	top := NewTopN[int](3, false) // top 3 menores
	inputs := []Entry[int]{
		{Key: "a", Value: 5},
		{Key: "b", Value: 1},
		{Key: "c", Value: 3},
		{Key: "d", Value: 1},
		{Key: "e", Value: 2},
		{Key: "f", Value: 1}, // three 1s in total
		{Key: "g", Value: 4},
	}
	for _, e := range inputs {
		top.Insert(e)
	}
	got := valuesOf(top.Values())
	// esperamos los 3 menores: [1,1,1]
	want := []int{1, 1, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ThreeSmallestWithDuplicates: got %v, want %v", got, want)
	}
}
