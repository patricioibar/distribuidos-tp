package jobsessions

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/patricioibar/distribuidos-tp/persistance"
)

// JobSessionsState tracks active sessions and implements persistance.State.
type JobSessionsState struct {
	sessions map[uuid.UUID]*JobSession
}

var _ persistance.State = (*JobSessionsState)(nil)

func NewJobSessionsState() persistance.State {
	return &JobSessionsState{
		sessions: make(map[uuid.UUID]*JobSession),
	}
}

func (jss *JobSessionsState) GetOrCreateSession(id uuid.UUID) *JobSession {
	session, exists := jss.sessions[id]
	if !exists {
		session = NewJobSession(id)
		jss.sessions[id] = session
	}
	return session
}

// UpdateSessionLastActivity refreshes the session's last activity timestamp if it exists.
func (jss *JobSessionsState) UpdateSessionLastActivity(id uuid.UUID, timestamp int64) bool {
	session, exists := jss.sessions[id]
	if !exists {
		return false
	}
	session.UpdateLastActivity(timestamp)
	return true
}

func (jss *JobSessionsState) GetSession(id uuid.UUID) (*JobSession, bool) {
	session, exists := jss.sessions[id]
	return session, exists
}

func (jss *JobSessionsState) RemoveSession(id uuid.UUID) {
	delete(jss.sessions, id)
}

func (jss *JobSessionsState) Apply(op persistance.Operation) error {
	return op.ApplyTo(jss)
}

func (jss *JobSessionsState) GetAllSessions() map[uuid.UUID]*JobSession {
	return jss.sessions
}

func (jss *JobSessionsState) Serialize() ([]byte, error) {
	snapshot := JobSessionsStateSnapshot{Sessions: make(map[string]JobSessionSnapshot, len(jss.sessions))}
	for id, session := range jss.sessions {
		snapshot.Sessions[id.String()] = session.toSnapshot()
	}
	return json.Marshal(snapshot)
}

func (jss *JobSessionsState) Deserialize(data []byte) error {
	if len(data) == 0 {
		jss.sessions = make(map[uuid.UUID]*JobSession)
		return nil
	}
	var snapshot JobSessionsStateSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	if jss.sessions == nil {
		jss.sessions = make(map[uuid.UUID]*JobSession, len(snapshot.Sessions))
	} else {
		for k := range jss.sessions {
			delete(jss.sessions, k)
		}
	}
	for idStr, sessSnap := range snapshot.Sessions {
		id, err := uuid.Parse(idStr)
		if err != nil {
			return err
		}
		session := NewJobSession(id)
		if err := session.applySnapshot(sessSnap); err != nil {
			return err
		}
		jss.sessions[id] = session
	}
	return nil
}
