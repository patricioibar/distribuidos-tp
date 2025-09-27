package filterbyyear

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func processBatch(data Batch) ([][]string, error) {

	log.Printf("Processing batch with TaskID: %s", data.TaskID)

	index_of_created_at := -1
	for i, rowKey := range data.RowKeys {
		if rowKey == "created_at" {
			index_of_created_at = i
			break
		}
	}

	if index_of_created_at == -1 {
		log.Printf("created_at column not found in RowKeys")
		return nil, fmt.Errorf("created_at column not found")
	}

	var filteredRows [][]string
	for _, row := range data.Rows {
		ts := row[index_of_created_at]
		year, _, _, err := parseTimestamp(ts)
		if err != nil {
			log.Printf("Error parsing timestamp %s: %v", ts, err)
			continue
		}
		if year == 2024 || year == 2025 {
			filteredRows = append(filteredRows, row)
		}
	}

	return filteredRows, nil
}



type Batch struct {
	TaskID   string   `json:"task_id"`
	TaskDone bool     `json:"task_done"`
	RowKeys  []string `json:"row_keys"`
	Rows     [][]string `json:"rows"`
}

func NewBatchFromJSON(jsonData string) Batch {
	var b Batch

	err := json.Unmarshal([]byte(jsonData), &b)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}
	return b
}


func parseTimestamp(ts string) (int, int, int, error) {

	layout := "2006-01-02 15:04:05"

	t, err := time.Parse(layout, ts)
	if err != nil {
		fmt.Println("Error al parsear:", err)
		return 0, 0, 0, err
	}

	return t.Year(), int(t.Month()), t.Day(), nil
}