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

const resultsDir = "results"

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
	doneReceivingResults  chan struct{}
	resultWrittingDone    []chan struct{}
}

func NewServerConnection(config *Config) *ServerConnection {
	return &ServerConnection{
		BatchSize:             config.BatchSize,
		CoffeeAnalyzerAddress: config.CoffeeAnalyzerAddress,
		doneReceivingResults:  make(chan struct{}),
		resultWrittingDone:    []chan struct{}{},
	}
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

		columnsJson, _ := json.Marshal(table.Columns)
		err = socket.SendBatch(columnsJson)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send header %v", err)
			return
		}

		header, _ := reader.getHeader()
		columnsIdxs := findColumnIdxs(table, header, file)
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

	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			log.Errorf("Failed to create responses directory: %v", err)
			return
		}
	}

	log.Info("Requesting responses to Coffee Analyzer")
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
			log.Infof("Receiving responses for query %d", batch.QueryId)
			doneWritting := make(chan struct{})
			responseChan = make(chan c.QueryResponseBatch)
			responseWriter[batch.QueryId] = responseChan
			s.resultWrittingDone = append(s.resultWrittingDone, doneWritting)
			go writeResponsesToFile(batch.QueryId, responseChan, doneWritting)
		}
		responseChan <- batch
	}
	close(s.doneReceivingResults)
}

func writeResponsesToFile(queryId int, responseChan chan c.QueryResponseBatch, done chan struct{}) {
	fileName := fmt.Sprintf("%s/query_%d.csv", resultsDir, queryId)
	file, err := os.Create(fileName)
	if err != nil {
		log.Errorf("Failed to create file %s: %v", fileName, err)
		return
	}

	writer := csv.NewWriter(file)

	firstBatch := <-responseChan
	if err := writer.Write(firstBatch.Columns); err != nil {
		log.Errorf("Failed to write header to file %s: %v", fileName, err)
		file.Close()
		return
	}
	for _, row := range firstBatch.Rows {
		if err := writer.Write(row); err != nil {
			log.Errorf("Failed to write row to file %s: %v", fileName, err)
		}
	}
	log.Infof("query %v firstBatch columns: %+v", queryId, firstBatch.Columns)
	log.Infof("query %v firstBatch rows: %+v", queryId, firstBatch.Rows)

	for batch := range responseChan {
		if batch.Rows == nil {
			continue
		}
		if batch.EOF {
			break
		}
		for _, row := range batch.Rows {
			if err := writer.Write(row); err != nil {
				log.Errorf("Failed to write row to file %s: %v", fileName, err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			log.Errorf("Error flushing to file %s: %v", fileName, err)
		}
	}
	file.Close()
	close(done)
	log.Infof("Finished writing responses for query %d to file %s", queryId, fileName)
}

func (s *ServerConnection) WaitForResults() {
	<-s.doneReceivingResults
	for _, done := range s.resultWrittingDone {
		<-done
	}
}
