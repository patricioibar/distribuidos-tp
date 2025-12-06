package jobsessions

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type tableState struct {
	isUploaded bool
}

type responseState struct {
	startedReceiving bool
	lastActivity     int64
}

// JobSession tracks upload/response progress for a job.
type JobSession struct {
	id            uuid.UUID
	tableState    map[string]*tableState
	responseState *responseState
}

// NewJobSession constructs a fresh session instance with sensible defaults.
func NewJobSession(id uuid.UUID) *JobSession {
	return &JobSession{
		id:         id,
		tableState: make(map[string]*tableState),
		responseState: &responseState{
			startedReceiving: false,
			lastActivity:     time.Now().Unix(),
		},
	}
}

// ID exposes the UUID backing the session.
func (js *JobSession) ID() uuid.UUID {
	return js.id
}

func (js *JobSession) IsTableUploaded(tableName string) bool {
	table, exists := js.tableState[tableName]
	if !exists {
		return false
	}
	return table.isUploaded
}

func (js *JobSession) IsUploadFinish() bool {
	for _, state := range js.tableState {
		if !state.isUploaded {
			return false
		}
	}
	return true
}

func (js *JobSession) AddNewTable(tableName string) {
	_, exists := js.tableState[tableName]
	if !exists {
		js.tableState[tableName] = &tableState{isUploaded: false}
	}
}

func (js *JobSession) MarkTableAsUploaded(tableName string) {
	table, exists := js.tableState[tableName]
	if !exists {
		table = &tableState{}
		js.tableState[tableName] = table
	}
	table.isUploaded = true
}

func (js *JobSession) HasStartedReceivingResponses() bool {
	return js.responseState.startedReceiving
}

func (js *JobSession) MarkStartedReceivingResponses() {
	js.responseState.startedReceiving = true
}

func (js *JobSession) UpdateLastActivity(timestamp int64) {
	js.responseState.lastActivity = timestamp
}

func (js *JobSession) GetLastActivity() int64 {
	return js.responseState.lastActivity
}

func (js *JobSession) Serialize() ([]byte, error) {
	return json.Marshal(js.toSnapshot())
}

func (js *JobSession) Deserialize(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snapshot JobSessionSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	return js.applySnapshot(snapshot)
}

func (js *JobSession) toSnapshot() JobSessionSnapshot {
	snapshot := JobSessionSnapshot{
		ID:               js.id.String(),
		TablesUploaded:   make(map[string]bool, len(js.tableState)),
		StartedReceiving: js.responseState != nil && js.responseState.startedReceiving,
	}
	if js.responseState != nil {
		snapshot.LastActivityEpoch = js.responseState.lastActivity
	}
	for table, state := range js.tableState {
		if state != nil {
			snapshot.TablesUploaded[table] = state.isUploaded
		} else {
			snapshot.TablesUploaded[table] = false
		}
	}
	return snapshot
}

func (js *JobSession) applySnapshot(snapshot JobSessionSnapshot) error {
	id, err := uuid.Parse(snapshot.ID)
	if err != nil {
		return err
	}
	js.id = id
	if js.tableState == nil {
		js.tableState = make(map[string]*tableState, len(snapshot.TablesUploaded))
	} else {
		for k := range js.tableState {
			delete(js.tableState, k)
		}
	}
	for table, uploaded := range snapshot.TablesUploaded {
		js.tableState[table] = &tableState{isUploaded: uploaded}
	}
	js.responseState = &responseState{
		startedReceiving: snapshot.StartedReceiving,
		lastActivity:     time.Now().Unix(),
	}
	return nil
}
