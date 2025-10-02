package main

import (
	"communication"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
}

func (s *ServerConnection) sendDataset(table TableConfig, dataDir string) {
	socket := communication.Socket{}
	dir := fmt.Sprintf("%s/%s", dataDir, table.Name)

	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		reader := Reader{FilePath: filepath.Join(dir, file.Name()), BatchSize: s.BatchSize}

		tableBytes, _ := json.Marshal(table.Name)
		//habria que cambiarle el nombre al metodo
		err := socket.SendBatch(tableBytes)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send table %v", err)
			return
		}

		header, _ := reader.getHeader()
		headerJson, _ := json.Marshal(header)

		columnsIdxs := findColumnIdxs(table, header, file)

		err = socket.SendBatch(headerJson)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send header %v", err)
			return
		}

		reader.SendFileTroughSocket(columnsIdxs, socket)
	}

	log.Infof("All files from directory %s sent successfully.", dir)
}

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
