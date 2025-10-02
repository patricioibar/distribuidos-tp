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

func (s *ServerConnection) getResponses() {
	socket := communication.Socket{}
	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	socket.SendGetResponsesRequest()

	for {
		_, err := socket.ReadBatch()
		if err != nil {
			if err == io.EOF {
				log.Infof("All responses received.")
			}
			log.Errorf("Error reading batch: %v", err)
			break
		}
		panic("to do: get responses")
	}
}
