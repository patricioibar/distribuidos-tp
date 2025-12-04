package persistance

import (
	"fmt"
	"io"
)

func init() {
	RegisterOperation(DeltaTypeID, decodeDelta)
}

// Operation represents a state change operation
//
// It must be able to encode/decode itself to/from bytes for logging purposes
// and apply itself to a State.
//
// The Encode method should produce a byte slice that starts with the TypeID byte,
// followed by the encoded data specific to the operation.
type Operation interface {
	TypeID() byte
	SeqNumber() uint64
	Encode() ([]byte, error)
	ApplyTo(State) error
}

// OperationDecoder is a function that decodes an operation from a byte slice
// It is used to register decoders for different operation types
// the byte slice passed to the decoder includes the TypeID as the first byte.
// then inlcudes the rest of the encoded operation data.
type OperationDecoder func([]byte) (Operation, error)

var operationRegistry = map[byte]OperationDecoder{}

func RegisterOperation(typeID byte, decoder OperationDecoder) {
	operationRegistry[typeID] = decoder
}

type State interface {
	Apply(Operation) error

	Serialize() ([]byte, error)

	Deserialize([]byte) error
}

type StateManager struct {
	state             State
	stateLog          StateLog
	logsSinceSnapshot int
	snapshotPeriod    int
}

func NewStateManager(state State, stateLog StateLog, snapshotPeriod int) (*StateManager, error) {
	sm := &StateManager{
		state:             state,
		stateLog:          stateLog,
		logsSinceSnapshot: 0,
		snapshotPeriod:    snapshotPeriod,
	}

	return sm, nil
}

// LoadStateManager restores the state from the latest snapshot and WAL without writing a new snapshot.
func LoadStateManager(state State, stateLog StateLog, snapshotPeriod int) (*StateManager, error) {
	sm := &StateManager{
		state:             state,
		stateLog:          stateLog,
		logsSinceSnapshot: 0,
		snapshotPeriod:    snapshotPeriod,
	}
	if err := sm.Restore(); err != nil {
		return nil, err
	}
	return sm, nil
}

// Applies an operation to the current internal state of the logger(used to persist state changes and take snapshots) and logs it
func (sm *StateManager) Log(op Operation) error {
	entry, err := op.Encode()
	if err != nil {
		return err
	}
	err = sm.stateLog.Append(entry)
	if err != nil {
		return err
	}
	err = sm.stateLog.Commit()
	if err != nil {
		return err
	}
	err = op.ApplyTo(sm.state)
	if err != nil {
		return err
	}
	sm.logsSinceSnapshot++
	if sm.logsSinceSnapshot >= sm.snapshotPeriod {
		snapshotData, err := sm.state.Serialize()
		if err != nil {
			return err
		}
		_, err = sm.stateLog.WriteSnapshot(snapshotData)
		if err != nil {
			return err
		}
		sm.logsSinceSnapshot = 0
		err = sm.stateLog.RotateWAL()
		if err != nil {
			return err
		}
	}

	return nil
}

func (sm *StateManager) Close() error {
	return sm.stateLog.Close()
}

func (sm *StateManager) GetState() State {
	return sm.state
}

func (sm *StateManager) Restore() error {
	snapshotData, walIt, err := sm.stateLog.Restore()
	if err != nil {
		return err
	}

	if snapshotData != nil {
		err = sm.state.Deserialize(snapshotData)
		if err != nil {
			return err
		}
	}

	for {
		entry, err := walIt.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if entry == nil {
			break
		}
		op, err := sm.decodeOperation(entry)
		if err != nil {
			return err
		}
		err = sm.state.Apply(op)
		if err != nil {
			return err
		}
	}
	// close iterator file handles
	_ = walIt.Close()
	return nil
}

func (sm *StateManager) GetOperationWithSeqNumber(seqNumber uint64) (Operation, error) {
	walIt, err := sm.stateLog.GetLogsIterator()
	if err != nil {
		return nil, err
	}

	defer func() { _ = walIt.Close() }()

	for {
		entry, err := walIt.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if entry == nil {
			break
		}
		op, err := sm.decodeOperation(entry)
		if err != nil {
			return nil, err
		}
		if op.SeqNumber() == seqNumber {
			return op, nil
		}
	}
	return nil, nil
}

func (sm *StateManager) decodeOperation(entry []byte) (Operation, error) {
	if len(entry) == 0 {
		return nil, fmt.Errorf("empty entry")
	}
	typeID := entry[0]
	decoder, exists := operationRegistry[typeID]
	if !exists {
		return nil, fmt.Errorf("unknown operation type ID: %d", typeID)
	}
	op, err := decoder(entry)
	if err != nil {
		return nil, err
	}
	return op, nil
}
