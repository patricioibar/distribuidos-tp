package persistance

import (
	"slices"
	"testing"
)

func TestMonitorStateSerializeDeserialize(t *testing.T) {
	original := NewMonitorState().(*monitoreState)
	original.monitorsSeen = []string{"monitor-1", "monitor-2"}
	original.workersSeen = []string{"worker-1", "worker-2"}

	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	restored := NewMonitorState().(*monitoreState)
	if err := restored.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if !slices.Equal(restored.monitorsSeen, original.monitorsSeen) {
		t.Fatalf("monitors slice mismatch: got %v want %v", restored.monitorsSeen, original.monitorsSeen)
	}
	if !slices.Equal(restored.workersSeen, original.workersSeen) {
		t.Fatalf("workers slice mismatch: got %v want %v", restored.workersSeen, original.workersSeen)
	}
}

func TestMonitorStateDeserializeEmpty(t *testing.T) {
	state := NewMonitorState().(*monitoreState)
	state.monitorsSeen = []string{"existing"}
	state.workersSeen = []string{"worker"}

	if err := state.Deserialize(nil); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if len(state.monitorsSeen) != 0 || len(state.workersSeen) != 0 {
		t.Fatalf("expected empty slices after deserializing empty data")
	}
}
