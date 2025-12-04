package rollback

import "encoding/json"

const (
	TypeRequest  byte = 1
	TypeResponse byte = 2
	TypeAllOK    byte = 3
)

type Message struct {
	Type    byte        `json:"type"`
	Payload interface{} `json:"payload"`
}

type RecoveryRequestPayload struct {
	SequenceNumbers []uint64 `json:"sequence_numbers"`
}

type RecoveryResponsePayload struct {
	WorkerID       string                   `json:"worker_id"`
	SequenceNumber uint64                   `json:"sequence_number"`
	OperationData  map[string][]interface{} `json:"data"`
}

type RecoveryAllOKPayload struct {
	WorkerID string `json:"worker_id"`
}

// Marshal serializes the Message to JSON
func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal deserializes JSON into the Message, decoding the payload to the concrete type
func (m *Message) Unmarshal(data []byte) error {
	// Temporary wrapper to extract type and raw payload
	type temp struct {
		Type    byte            `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	var t temp
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	m.Type = t.Type

	switch m.Type {
	case TypeRequest:
		var p RecoveryRequestPayload
		if len(t.Payload) > 0 {
			if err := json.Unmarshal(t.Payload, &p); err != nil {
				return err
			}
		}
		m.Payload = &p
	case TypeResponse:
		var p RecoveryResponsePayload
		if len(t.Payload) > 0 {
			if err := json.Unmarshal(t.Payload, &p); err != nil {
				return err
			}
		}
		m.Payload = &p
	case TypeAllOK:
		var p RecoveryAllOKPayload
		if len(t.Payload) > 0 {
			if err := json.Unmarshal(t.Payload, &p); err != nil {
				return err
			}
		}
		m.Payload = &p
	default:
		// unknown type: keep raw payload as json.RawMessage
		m.Payload = t.Payload
	}
	return nil
}

// Constructors for convenience
func NewRecoveryRequest(sequenceNumbers []uint64) *Message {
	return &Message{
		Type: TypeRequest,
		Payload: &RecoveryRequestPayload{
			SequenceNumbers: sequenceNumbers,
		},
	}
}

func NewRecoveryResponse(workerID string, seq uint64, data map[string][]interface{}) *Message {
	return &Message{
		Type: TypeResponse,
		Payload: &RecoveryResponsePayload{
			WorkerID:       workerID,
			SequenceNumber: seq,
			OperationData:  data,
		},
	}
}

func NewRecoveryAllOK(workerID string) *Message {
	return &Message{
		Type: TypeAllOK,
		Payload: &RecoveryAllOKPayload{
			WorkerID: workerID,
		},
	}
}
