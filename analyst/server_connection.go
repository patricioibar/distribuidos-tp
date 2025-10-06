package main

import (
	c "communication"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const parentResultsDir = "results"

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

	if _, err := os.Stat(parentResultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(parentResultsDir, 0755); err != nil {
			log.Errorf("Failed to create responses directory: %v", err)
			return
		}
	}
	resultsDir := filepath.Join(parentResultsDir, time.Now().Format("2006-01-02 15:04:05"))
	if err := os.Mkdir(resultsDir, 0755); err != nil && !os.IsExist(err) {
		log.Errorf("Failed to create results directory: %v", err)
		return
	}

	log.Info("Requesting responses to Coffee Analyzer")
	socket.SendGetResponsesRequest()

	responseWriter := make(map[int]chan c.QueryResponseBatch)

	for {
		data, err := socket.ReadBatch()
		if err != nil {
			if err == io.EOF {
				log.Infof("All responses received.")
			} else {
				log.Errorf("Error reading batch: %v", err)
			}
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
			go writeResponsesToFile(resultsDir, batch.QueryId, responseChan, doneWritting)
		}
		responseChan <- batch
	}

	for _, responseChan := range responseWriter {
		close(responseChan)
	}

	close(s.doneReceivingResults)
}

func writeResponsesToFile(resultsDir string, queryId int, responseChan chan c.QueryResponseBatch, done chan struct{}) {
	fileName := fmt.Sprintf("%s/query_%d.csv", resultsDir, queryId)
	file, err := os.Create(fileName)
	if err != nil {
		log.Errorf("Failed to create file %s: %v", fileName, err)
		return
	}

	{
		firstBatch := <-responseChan
		header := joinStringArr(firstBatch.Columns, ",")
		if _, err := file.WriteString(header + "\n"); err != nil {
			log.Errorf("Failed to write header to file %s: %v", fileName, err)
			file.Close()
			return
		}
		writeRows(firstBatch.Rows, file, fileName)
		log.Infof("query %v firstBatch columns: %+v", queryId, firstBatch.Columns)
		log.Infof("query %v firstBatch rows: %+v", queryId, firstBatch.Rows)
	}

	for batch := range responseChan {
		if batch.Rows == nil {
			continue
		}
		if batch.EOF {
			break
		}
		writeRows(batch.Rows, file, fileName)
	}
	file.Close()
	close(done)
}

func writeRows(rows [][]string, file *os.File, fileName string) {
	for _, row := range rows {
		rowStr := joinStringArr(row, ",")
		if _, err := file.WriteString(rowStr + "\n"); err != nil {
			log.Errorf("Failed to write row to file %s: %v", fileName, err)
		}
	}
}

func (s *ServerConnection) WaitForResults() {
	<-s.doneReceivingResults
	for _, done := range s.resultWrittingDone {
		<-done
	}
}

func joinStringArr(arr []string, sep string) string {
	result := ""
	for i, str := range arr {
		if i > 0 {
			result += sep
		}
		result += str
	}
	return result
}
