package persistance

import "testing"

func TestRegisterNewMonitorOperationEncodeDecode(t *testing.T) {
	op := NewRegisterNewMonitorOperation("monitor-123").(*registerNewMonitorOperation)
	op.seq = 42

	encoded, err := op.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decodedOp, err := decodeRegisterNewMonitorEntry(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := decodedOp.(*registerNewMonitorOperation)
	if !ok {
		t.Fatalf("decoded op has unexpected type %T", decodedOp)
	}

	if decoded.TypeID() != RegisterNewMonitorOpID {
		t.Fatalf("unexpected TypeID: %d", decoded.TypeID())
	}
	if decoded.SeqNumber() != op.seq {
		t.Fatalf("unexpected seq: got %d want %d", decoded.SeqNumber(), op.seq)
	}
	if decoded.monitorID != op.monitorID {
		t.Fatalf("monitorID mismatch: got %s want %s", decoded.monitorID, op.monitorID)
	}
}

func TestRegisterNewWorkerOperationEncodeDecode(t *testing.T) {
	op := NewRegisterNewWorkerOperation("worker-9").(*registerNewWorkerOperation)
	op.seq = 7

	encoded, err := op.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decodedOp, err := decodeRegisterNewWorkerEntry(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := decodedOp.(*registerNewWorkerOperation)
	if !ok {
		t.Fatalf("decoded op has unexpected type %T", decodedOp)
	}

	if decoded.TypeID() != RegisterNewWorkerOpID {
		t.Fatalf("unexpected TypeID: %d", decoded.TypeID())
	}
	if decoded.SeqNumber() != op.seq {
		t.Fatalf("unexpected seq: got %d want %d", decoded.SeqNumber(), op.seq)
	}
	if decoded.workerID != op.workerID {
		t.Fatalf("workerID mismatch: got %s want %s", decoded.workerID, op.workerID)
	}
}
