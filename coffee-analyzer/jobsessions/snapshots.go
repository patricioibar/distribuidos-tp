package jobsessions

// JobSessionSnapshot captures the persisted representation of a single session.
type JobSessionSnapshot struct {
	ID                string          `json:"id"`
	TablesUploaded    map[string]bool `json:"tables_uploaded"`
	StartedReceiving  bool            `json:"started_receiving"`
	LastActivityEpoch int64           `json:"last_activity"`
}

// JobSessionsStateSnapshot captures all tracked sessions.
type JobSessionsStateSnapshot struct {
	Sessions map[string]JobSessionSnapshot `json:"sessions"`
}
