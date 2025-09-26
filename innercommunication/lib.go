package innercommunication

import (
	"encoding/json"
	"fmt"
)

type RowsBatch struct {
	JobDone     bool            `json:"job_done"`
	ColumnNames []string        `json:"column_names"`
	Rows        [][]interface{} `json:"rows"`
}

func (rb *RowsBatch) String() (string, error) {
	data, err := json.Marshal(rb)
	if err != nil {
		return "", fmt.Errorf("failed to marshal RowsBatch: %v", err)
	}
	return string(data), nil
}

func RowsBatchFromString(data string) (*RowsBatch, error) {
	var rb RowsBatch
	if err := json.Unmarshal([]byte(data), &rb); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RowsBatch: %v", err)
	}
	return &rb, nil
}

func NewEndSignal() *RowsBatch {
	return &RowsBatch{
		JobDone: true,
	}
}

func (rb *RowsBatch) IsEndSignal() bool {
	return rb.JobDone
}

func NewRowsBatch(columnNames []string, rows [][]interface{}) *RowsBatch {
	return &RowsBatch{
		JobDone:     false,
		ColumnNames: columnNames,
		Rows:        rows,
	}
}
