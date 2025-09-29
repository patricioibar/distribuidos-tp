package innercommunication

import (
	"encoding/json"
	"fmt"
)

type RowsBatch struct {
	EndSignal   bool            `json:"end_signal,omitempty"`
	ColumnNames []string        `json:"column_names,omitempty"`
	Rows        [][]interface{} `json:"rows,omitempty"`
	WorkersDone []string        `json:"workers_done,omitempty"`
}

func (rb *RowsBatch) Marshal() ([]byte, error) {
	data, err := json.Marshal(rb)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RowsBatch: %v", err)
	}
	return data, nil
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
		EndSignal: true,
	}
}

func (rb *RowsBatch) AddWorkerDone(workerId string) {
	rb.WorkersDone = append(rb.WorkersDone, workerId)
}

func (rb *RowsBatch) IsEndSignal() bool {
	return rb.EndSignal
}

func NewRowsBatch(columnNames []string, rows [][]interface{}) *RowsBatch {
	return &RowsBatch{
		EndSignal:   false,
		ColumnNames: columnNames,
		Rows:        rows,
	}
}
