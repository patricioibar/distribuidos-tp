package main

import (
	c "communication"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const responsesDir = "responses"

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
}

func (s *ServerConnection) sendDataset(table TableConfig, dataDir string) {
	socket := c.Socket{}
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
	socket := c.Socket{}
	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	socket.SendGetResponsesRequest()

	responseWriter := make(map[int]chan c.QueryResponseBatch)

	for {
		data, err := socket.ReadBatch()
		if err != nil {
			if err == io.EOF {
				log.Infof("All responses received.")
			}
			log.Errorf("Error reading batch: %v", err)
			break
		}
		var batch c.QueryResponseBatch
		err = json.Unmarshal(data, &batch)
		if err != nil {
			log.Errorf("Failed to unmarshal response: %v", err)
			continue
		}

		responseChan, exists := responseWriter[batch.QueryId]
		if !exists {
			responseChan = make(chan c.QueryResponseBatch)
			responseWriter[batch.QueryId] = responseChan
			go writeResponsesToFile(batch.QueryId, responseChan)
		}
		responseChan <- batch
	}
}

func writeResponsesToFile(queryId int, responseChan chan c.QueryResponseBatch) {
	fileName := fmt.Sprintf("%s/query_%d.csv", responsesDir, queryId)
	file, err := os.Create(fileName)
	if err != nil {
		log.Errorf("Failed to create file %s: %v", fileName, err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	firstBatch := <-responseChan
	if err := writer.Write(firstBatch.Columns); err != nil {
		log.Errorf("Failed to write header to file %s: %v", fileName, err)
		return
	}
	for _, row := range firstBatch.Rows {
		if err := writer.Write(row); err != nil {
			log.Errorf("Failed to write row to file %s: %v", fileName, err)
		}
	}

	for batch := range responseChan {
		for _, row := range batch.Rows {
			if err := writer.Write(row); err != nil {
				log.Errorf("Failed to write row to file %s: %v", fileName, err)
			}
		}
	}
}
