package main

import (
	"communication"
	"encoding/json"
	"os"
)

func sendRowsTroughSocket(rows [][]string, socket communication.Socket) {
	data, err := json.Marshal(rows)
	if err != nil {
		log.Errorf("Failed to send batch: %v", err)
		return
	}

	err = socket.SendBatch(data)
	if err != nil {
		log.Errorf("Failed to send batch: %v", err)
		return
	}
}

func findColumnIdxs(table TableConfig, header []string, file os.DirEntry) []int {
	columnsIdxs := []int{}
	for _, col := range table.Columns {
		found := false
		for idx, headerCol := range header {
			if col == headerCol {
				columnsIdxs = append(columnsIdxs, idx)
				found = true
				break
			}
		}
		if !found {
			log.Fatalf("Column %s not found in file %s", col, file.Name())
		}
	}
	return columnsIdxs
}
