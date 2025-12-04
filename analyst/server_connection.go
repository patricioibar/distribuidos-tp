package main

import (
	c "communication"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	uuid "github.com/google/uuid"
	"github.com/patricioibar/distribuidos-tp/bitmap"
)

const (
	parentResultsDir      = "results"
	responseRetryInterval = time.Second
	responseMaxRetryTime  = 2 * time.Minute
)

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
	doneReceivingResults  chan struct{}
	resultWrittingDone    []chan struct{}
	uuid                  uuid.UUID
}

func NewServerConnection(config *Config, restoreID *uuid.UUID) *ServerConnection {
	var jobID uuid.UUID
	if restoreID != nil {
		jobID = *restoreID
		log.Infof("Using restore UUID %s", jobID.String())
	} else {
		socket := c.Socket{}
		err := socket.Connect(config.CoffeeAnalyzerAddress)
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		defer socket.Close()
		socket.SendStartJobRequest()
		receivedUUID, err := socket.ReceiveUUID()
		if err != nil {
			log.Fatalf("Failed to receive UUID response: %v", err)
		}
		jobID = receivedUUID
	}

	return &ServerConnection{
		BatchSize:             config.BatchSize,
		CoffeeAnalyzerAddress: config.CoffeeAnalyzerAddress,
		doneReceivingResults:  make(chan struct{}),
		resultWrittingDone:    []chan struct{}{},
		uuid:                  jobID,
	}
}

func (s *ServerConnection) sendDataset(table TableConfig, dataDir string) {
	socket := c.Socket{}
	dir := fmt.Sprintf("%s/%s", dataDir, table.Name)

	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer socket.Close()
	socket.SendUUID(s.uuid)

	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
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
		}

		columnsJson, _ := json.Marshal(table.Columns)
		err = socket.SendBatch(columnsJson)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send header %v", err)
		}

		header, _ := reader.getHeader()
		columnsIdxs := findColumnIdxs(table, header, file)
		reader.SendFileTroughSocket(columnsIdxs, socket)
	}

	log.Infof("All files from directory %s sent successfully.", dir)
}

func (s *ServerConnection) getResponses() {
	resultsDir, err := s.ensureResultsDir()
	if err != nil {
		log.Errorf("Failed to prepare results directory: %v", err)
		close(s.doneReceivingResults)
		return
	}

	socket, err := s.requestResponsesSocket()
	if err != nil {
		log.Errorf("Unable to connect to Coffee Analyzer for responses: %v", err)
		close(s.doneReceivingResults)
		return
	}
	defer func() {
		if socket != nil {
			socket.Close()
		}
	}()

	responseWriter := make(map[int]chan c.QueryResponseBatch)
	receivedBatcheesPerQuery := make(map[int]*bitmap.Bitmap)

	queryStatus := make(map[int]bool)
	for i := 1; i <= 4; i++ {
		queryStatus[i] = false
	}
	allQueriesCompleted := false
	for !allQueriesCompleted {
		data, err := socket.ReadBatch()
		if err != nil {
			// if err == io.EOF {
			// 	log.Infof("All responses received.")
			// 	break
			// }
			log.Warningf("Error reading batch: %v. Attempting to reconnect.", err)
			socket.Close()
			socket = nil
			reconn, reconnErr := s.requestResponsesSocket()
			if reconnErr != nil {
				log.Errorf("Failed to reconnect after read error: %v", reconnErr)
				break
			}
			socket = reconn
			continue
		}
		var batch c.QueryResponseBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			log.Errorf("Failed to unmarshal response: %v", err)
			continue
		}

		responseChan, exists := responseWriter[batch.QueryId]
		receivedBatches, ok := receivedBatcheesPerQuery[batch.QueryId]
		if !exists {
			log.Infof("Receiving responses for query %d", batch.QueryId)
			doneWritting := make(chan struct{})
			responseChan = make(chan c.QueryResponseBatch)
			responseWriter[batch.QueryId] = responseChan
			s.resultWrittingDone = append(s.resultWrittingDone, doneWritting)
			go writeResponsesToFile(resultsDir, batch.QueryId, responseChan, doneWritting)
		}
		if !ok {
			receivedBatches = bitmap.New()
			receivedBatcheesPerQuery[batch.QueryId] = receivedBatches
		}

		if batch.EOF {
			log.Infof("All results for query %d received (EOF batch)", batch.QueryId)
			queryStatus[batch.QueryId] = true
			aux := true
			for qid, completed := range queryStatus {
				if !completed {
					log.Infof("Query %d not completed yet", qid)
					aux = false
					continue
				}
			}
			if aux {
				allQueriesCompleted = true
			}
			continue
		}

		if batch.Rows == nil {
			continue
		}

		if !receivedBatches.Contains(uint64(batch.SequenceNumber)) {
			responseChan <- batch
			receivedBatches.Add(uint64(batch.SequenceNumber))
		}
	}

	for _, responseChan := range responseWriter {
		close(responseChan)
	}

	close(s.doneReceivingResults)
}

func (s *ServerConnection) ensureResultsDir() (string, error) {
	if _, err := os.Stat(parentResultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(parentResultsDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create responses directory: %w", err)
		}
	}
	resultsDir := filepath.Join(parentResultsDir, s.uuid.String())
	if err := os.Mkdir(resultsDir, 0755); err != nil && !os.IsExist(err) {
		return "", fmt.Errorf("failed to create results directory: %w", err)
	}
	return resultsDir, nil
}

func (s *ServerConnection) requestResponsesSocket() (*c.Socket, error) {
	socket, err := s.connectWithRetry()
	if err != nil {
		return nil, err
	}
	if err := socket.SendUUID(s.uuid); err != nil {
		socket.Close()
		return nil, fmt.Errorf("failed to send UUID: %w", err)
	}
	log.Info("Requesting responses to Coffee Analyzer")
	if err := socket.SendGetResponsesRequest(); err != nil {
		socket.Close()
		return nil, fmt.Errorf("failed to request responses: %w", err)
	}
	return socket, nil
}

func (s *ServerConnection) connectWithRetry() (*c.Socket, error) {
	deadline := time.Now().Add(responseMaxRetryTime)
	var lastErr error
	attempts := 0
	for {
		socket := &c.Socket{}
		attempts++
		err := socket.Connect(s.CoffeeAnalyzerAddress)
		if err == nil {
			return socket, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("failed to connect after %s: %w", responseMaxRetryTime, lastErr)
		}
		if attempts%5 == 0 {
			log.Warningf("Failed to connect to Coffee Analyzer (%v). Retrying in %s", err, responseRetryInterval)
		}
		time.Sleep(responseRetryInterval)
	}
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
			log.Infof("All results for query %d received", queryId)
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
