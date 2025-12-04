package jobsessions

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJobSessionSerializeDeserialize(t *testing.T) {
	id := uuid.New()
	session := NewJobSession(id)
	session.MarkTableAsUploaded("orders")
	session.MarkTableAsUploaded("payments")
	session.MarkStartedReceivingResponses()
	session.UpdateLastActivity(123456789)

	data, err := session.Serialize()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}
	var decoded JobSession
	if err := decoded.Deserialize(data); err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	if decoded.ID() != id {
		t.Fatalf("expected id %s got %s", id, decoded.ID())
	}
	if !decoded.IsTableUploaded("orders") || !decoded.IsTableUploaded("payments") {
		t.Fatalf("uploaded flags not preserved")
	}
	if !decoded.HasStartedReceivingResponses() {
		t.Fatalf("response start flag lost")
	}
	if got := decoded.GetLastActivity(); got < time.Now().Unix()-1 || got > time.Now().Unix()+1 {
		t.Fatalf("last activity not refreshed, got %d", got)
	}
}

func TestJobSessionsStateSerializeDeserialize(t *testing.T) {
	state := NewJobSessionsState().(*JobSessionsState)
	id1 := uuid.New()
	id2 := uuid.New()
	s1 := state.GetOrCreateSession(id1)
	s1.MarkTableAsUploaded("orders")
	s1.UpdateLastActivity(100)
	s2 := state.GetOrCreateSession(id2)
	s2.MarkStartedReceivingResponses()
	s2.UpdateLastActivity(200)

	data, err := state.Serialize()
	if err != nil {
		t.Fatalf("serialize state failed: %v", err)
	}
	newState := NewJobSessionsState().(*JobSessionsState)
	if err := newState.Deserialize(data); err != nil {
		t.Fatalf("deserialize state failed: %v", err)
	}

	if _, ok := newState.sessions[id1]; !ok {
		t.Fatalf("session %s missing after restore", id1)
	}
	if !newState.sessions[id1].IsTableUploaded("orders") {
		t.Fatalf("session %s table flag missing", id1)
	}
	if got := newState.sessions[id1].GetLastActivity(); got < time.Now().Unix()-1 || got > time.Now().Unix()+1 {
		t.Fatalf("session %s last activity not refreshed", id1)
	}
	if !newState.sessions[id2].HasStartedReceivingResponses() {
		t.Fatalf("session %s response flag missing", id2)
	}
	if got := newState.sessions[id2].GetLastActivity(); got < time.Now().Unix()-1 || got > time.Now().Unix()+1 {
		t.Fatalf("session %s last activity not refreshed", id2)
	}
}

func TestUploadTableOperationEncodeDecode(t *testing.T) {
	sessionID := uuid.New()
	op := &uploadTableOperation{
		ID:        uploadTableOperationTypeID,
		seq:       42,
		sessionID: sessionID,
		tableName: "orders",
	}
	encoded, err := op.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decodedOp, err := decodeUploadTableOperation(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	decoded := decodedOp.(*uploadTableOperation)
	if decoded.TypeID() != uploadTableOperationTypeID {
		t.Fatalf("unexpected type id: %d", decoded.TypeID())
	}
	if decoded.SeqNumber() != op.seq {
		t.Fatalf("seq mismatch: got %d want %d", decoded.SeqNumber(), op.seq)
	}
	if decoded.sessionID != sessionID {
		t.Fatalf("session mismatch: got %s want %s", decoded.sessionID, sessionID)
	}
	if decoded.tableName != op.tableName {
		t.Fatalf("table mismatch: got %s want %s", decoded.tableName, op.tableName)
	}
}

func TestStartResponsesOperationEncodeDecode(t *testing.T) {
	sessionID := uuid.New()
	op := &startResponsesOperation{
		ID:        startResponsesOperationTypeID,
		seq:       7,
		sessionID: sessionID,
	}
	encoded, err := op.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decodedOp, err := decodeStartResponsesOperation(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	decoded := decodedOp.(*startResponsesOperation)
	if decoded.TypeID() != startResponsesOperationTypeID {
		t.Fatalf("unexpected type id: %d", decoded.TypeID())
	}
	if decoded.SeqNumber() != op.seq {
		t.Fatalf("seq mismatch: got %d want %d", decoded.SeqNumber(), op.seq)
	}
	if decoded.sessionID != sessionID {
		t.Fatalf("session mismatch: got %s want %s", decoded.sessionID, sessionID)
	}
}

func TestFinishUploadOperationEncodeDecode(t *testing.T) {
	sessionID := uuid.New()
	op := &finishUploadOperation{
		ID:        finishUploadOperationTypeID,
		seq:       99,
		sessionID: sessionID,
		tableName: "payments",
	}
	encoded, err := op.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decodedOp, err := decodeFinishUploadOperation(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	decoded := decodedOp.(*finishUploadOperation)
	if decoded.TypeID() != finishUploadOperationTypeID {
		t.Fatalf("unexpected type id: %d", decoded.TypeID())
	}
	if decoded.SeqNumber() != op.seq {
		t.Fatalf("seq mismatch: got %d want %d", decoded.SeqNumber(), op.seq)
	}
	if decoded.sessionID != sessionID {
		t.Fatalf("session mismatch: got %s want %s", decoded.sessionID, sessionID)
	}
	if decoded.tableName != op.tableName {
		t.Fatalf("table mismatch: got %s want %s", decoded.tableName, op.tableName)
	}
}

func TestOperationsApplyToJobSessionsState(t *testing.T) {
	state := NewJobSessionsState().(*JobSessionsState)
	sessionID := uuid.New()

	upload := &uploadTableOperation{
		ID:        uploadTableOperationTypeID,
		seq:       1,
		sessionID: sessionID,
		tableName: "orders",
	}
	if err := state.Apply(upload); err != nil {
		t.Fatalf("apply upload op failed: %v", err)
	}
	session := state.sessions[sessionID]
	if session == nil {
		t.Fatalf("session not created after upload op")
	}
	if table, exists := session.tableState["orders"]; !exists {
		t.Fatalf("table not registered after upload op")
	} else if table.isUploaded {
		t.Fatalf("table marked uploaded too early")
	}

	start := &startResponsesOperation{
		ID:        startResponsesOperationTypeID,
		seq:       2,
		sessionID: sessionID,
	}
	if err := state.Apply(start); err != nil {
		t.Fatalf("apply start op failed: %v", err)
	}
	if !session.HasStartedReceivingResponses() {
		t.Fatalf("session did not record start of responses")
	}

	finish := &finishUploadOperation{
		ID:        finishUploadOperationTypeID,
		seq:       3,
		sessionID: sessionID,
		tableName: "orders",
	}
	if err := state.Apply(finish); err != nil {
		t.Fatalf("apply finish op failed: %v", err)
	}
	if !session.IsTableUploaded("orders") {
		t.Fatalf("table not marked uploaded after finish op")
	}
}
