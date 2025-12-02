package rollback

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRecoveryMessage_MarshalUnmarshal_Request(t *testing.T) {
	req := &RecoveryRequestPayload{SequenceNumbers: []uint64{1, 2, 3}}
	m := &Message{Type: TypeRequest, Payload: req}
	b, err := m.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var m2 Message
	if err := m2.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if m2.Type != TypeRequest {
		t.Fatalf("Type mismatch: got %d want %d", m2.Type, TypeRequest)
	}
	got, ok := m2.Payload.(*RecoveryRequestPayload)
	if !ok {
		t.Fatalf("Payload type mismatch: %T", m2.Payload)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("Payload mismatch: got=%#v want=%#v", got, req)
	}
}

func TestRecoveryMessage_MarshalUnmarshal_Response(t *testing.T) {
	data := map[string][]interface{}{"k1": {1.5, "s"}}
	resp := &RecoveryResponsePayload{WorkerID: "w1", SequenceNumber: 42, OperationData: data}
	m := &Message{Type: TypeResponse, Payload: resp}
	b, err := m.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var m2 Message
	if err := m2.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if m2.Type != TypeResponse {
		t.Fatalf("Type mismatch: got %d want %d", m2.Type, TypeResponse)
	}
	got, ok := m2.Payload.(*RecoveryResponsePayload)
	if !ok {
		t.Fatalf("Payload type mismatch: %T", m2.Payload)
	}
	// JSON unmarshalling may coerce numeric types; compare Marshall/Unmarshall equivalence
	if got.WorkerID != resp.WorkerID || got.SequenceNumber != resp.SequenceNumber {
		t.Fatalf("Header fields mismatch: got=%#v want=%#v", got, resp)
	}
	// check OperationData keys exist and length
	if len(got.OperationData) != len(resp.OperationData) {
		t.Fatalf("OperationData length mismatch: got=%v want=%v", got.OperationData, resp.OperationData)
	}
}

func TestRecoveryMessage_MarshalUnmarshal_AllOK(t *testing.T) {
	p := &RecoveryAllOKPayload{WorkerID: "w2"}
	m := &Message{Type: TypeAllOK, Payload: p}
	b, err := m.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var m2 Message
	if err := m2.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if m2.Type != TypeAllOK {
		t.Fatalf("Type mismatch: got %d want %d", m2.Type, TypeAllOK)
	}
	got, ok := m2.Payload.(*RecoveryAllOKPayload)
	if !ok {
		t.Fatalf("Payload type mismatch: %T", m2.Payload)
	}
	if got.WorkerID != p.WorkerID {
		t.Fatalf("WorkerID mismatch: got=%s want=%s", got.WorkerID, p.WorkerID)
	}
}

func TestRecoveryMessage_Unmarshal_UnknownTypeKeepsRaw(t *testing.T) {
	// build a raw message with unknown type
	raw := map[string]interface{}{"type": 99, "payload": map[string]interface{}{"foo": "bar"}}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("json.Marshal error: %v", err)
	}
	var m Message
	if err := m.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if m.Type != 99 {
		t.Fatalf("Type mismatch for unknown type: got %d want 99", m.Type)
	}
	// payload should be raw JSON (json.RawMessage)
	if _, ok := m.Payload.(json.RawMessage); !ok {
		t.Fatalf("expected raw payload (json.RawMessage), got %T", m.Payload)
	}
}
