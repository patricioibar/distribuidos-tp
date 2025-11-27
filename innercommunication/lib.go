package innercommunication

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

type MessageType string

const (
	MsgRowsBatch      MessageType = "RowsBatch"
	MsgEndSignal      MessageType = "EndSignal"
	MsgSequenceSet	  MessageType = "SequenceSet"
	MsgAggregatedData MessageType = "AggregatedData"
)

// --- Estructuras de Datos (Payloads) ---

type RowsBatchPayload struct {
	ColumnNames []string        `json:"column_names"`
	Rows        [][]interface{} `json:"rows"`
	SeqNum      uint64          `json:"seq_num"`
}

type EndSignalPayload struct {
	WorkersDone []string `json:"workers_done"`
	SeqNum      uint64   `json:"seq_num"`
}

func (e *EndSignalPayload) AddWorkerDone(id string) {
	e.WorkersDone = append(e.WorkersDone, id)
}

// JSONRoaringBitmap es un wrapper para manejar la serialización a Base64
// automáticamente, ya que JSON no soporta binarios nativos de manera eficiente.
type JSONRoaringBitmap struct {
	*roaring.Bitmap
}

type SequenceSetPayload struct {
	Sequences *JSONRoaringBitmap `json:"sequences"` // Wrapper personalizado
	WorkerID  string             `json:"worker_id"`
}

type AggregatedDataPayload struct {
	ColumnNames []string        `json:"column_names"`
	Rows        [][]interface{} `json:"rows"`
	WorkerID	string			`json:"worker_id"`
	SeqNum      uint64          `json:"seq_num"`
}

// --- El Mensaje (Envelope) ---

// Message es la estructura que viaja por la red.
type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

// MarshalJSON para JSONRoaringBitmap: Convierte el bitmap a Base64
func (r *JSONRoaringBitmap) MarshalJSON() ([]byte, error) {
	if r.Bitmap == nil {
		return []byte("null"), nil
	}
	var buf bytes.Buffer
	_, err := r.WriteTo(&buf) // Escribe formato binario eficiente
	if err != nil {
		return nil, err
	}
	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
	return json.Marshal(encoded)
}

// UnmarshalJSON para JSONRoaringBitmap: Convierte de Base64 a bitmap
func (r *JSONRoaringBitmap) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	r.Bitmap = roaring.New()
	_, err = r.ReadFrom(bytes.NewReader(decoded))
	return err
}

func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Message) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// UnmarshalJSON para Message: Detecta el tipo y decodifica el payload correcto
func (m *Message) UnmarshalJSON(data []byte) error {
	// 1. Definimos una estructura temporal para leer el "header" (Type)
	//    y mantener el Payload como bytes crudos (RawMessage).
	type tempMessage struct {
		Type    MessageType     `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}

	var temp tempMessage
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	m.Type = temp.Type

	// 2. Switch basado en el tipo para instanciar la estructura concreta
	switch m.Type {
	case MsgRowsBatch:
		var p RowsBatchPayload
		if err := json.Unmarshal(temp.Payload, &p); err != nil {
			return err
		}
		m.Payload = &p // Usamos puntero para evitar copias grandes

	case MsgEndSignal:
		var p EndSignalPayload
		if err := json.Unmarshal(temp.Payload, &p); err != nil {
			return err
		}
		m.Payload = &p

	case MsgSequenceSet:
		var p SequenceSetPayload
		if err := json.Unmarshal(temp.Payload, &p); err != nil {
			return err
		}
		m.Payload = &p
	
	case MsgAggregatedData:
		var p AggregatedDataPayload
		if err := json.Unmarshal(temp.Payload, &p); err != nil {
			return err
		}
		m.Payload = &p

	default:
		return fmt.Errorf("tipo de mensaje desconocido: %s", m.Type)
	}

	return nil
}

func NewRowsBatch(cols []string, rows [][]interface{}, seq uint64) *Message {
	return &Message{
		Type: MsgRowsBatch,
		Payload: &RowsBatchPayload{
			ColumnNames: cols,
			Rows:        rows,
			SeqNum:      seq,
		},
	}
}

func NewEndSignal(workers []string, seq uint64) *Message {
	return &Message{
		Type: MsgEndSignal,
		Payload: &EndSignalPayload{
			WorkersDone: workers,
			SeqNum:      seq,
		},
	}
}

func NewSequenceSet(workerID string, bitmap *roaring.Bitmap) *Message {
	// Clonamos o usamos el bitmap directamente, envuelto en nuestro tipo custom
	return &Message{
		Type: MsgSequenceSet,
		Payload: &SequenceSetPayload{
			Sequences: &JSONRoaringBitmap{Bitmap: bitmap},
			WorkerID:  workerID,
		},
	}
}

func NewAggregatedData(cols []string, rows [][]interface{}, workerID string, seq uint64) *Message {
	return &Message{
		Type: MsgAggregatedData,
		Payload: &AggregatedDataPayload{
			ColumnNames: cols,
			Rows:        rows,
			WorkerID:    workerID,
			SeqNum:      seq,
		},
	}
}