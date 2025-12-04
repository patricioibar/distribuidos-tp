package main

import (
	"cofee-analyzer/jobsessions"
	responseparser "cofee-analyzer/response_parser"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"communication"

	"github.com/patricioibar/distribuidos-tp/innercommunication"
	"github.com/patricioibar/distribuidos-tp/persistance"

	"github.com/patricioibar/distribuidos-tp/middleware"

	uuid "github.com/google/uuid"
)

type CoffeeAnalyzer struct {
	Address        string
	mwAddr         string
	config         *Config
	queriesConfig  []responseparser.QueryOutput
	parser         []responseparser.ResponseParser
	jobPublisher   *middleware.Producer
	totalWorkers   int
	duplicateProb  float64
	jobsState      *persistance.StateManager
	jobsStateMutex sync.Mutex
}

const jobPublishingExchange = "JOB_SOURCE"

const jobSessionsStateLogDir = "job_sessions_state_log"

const PERSISTANCE_INTERVAL = 20
const GRACE_PERIOD_SECONDS = 60

func NewCoffeeAnalyzer(config *Config) *CoffeeAnalyzer {
	jobPublisher, _ := middleware.NewProducer(jobPublishingExchange, config.MiddlewareAddress)

	// Seed RNG for duplicated message sampling
	rand.Seed(time.Now().UnixNano())

	// Initilize persistance state manager for job sessions
	jobSessionState := jobsessions.NewJobSessionsState()
	stateLog, err := persistance.NewStateLog(jobSessionsStateLogDir)
	if err != nil {
		log.Fatalf("Failed to create state log: %v", err)
	}

	jobsState, err := persistance.LoadStateManager(jobSessionState, stateLog, PERSISTANCE_INTERVAL)
	if err != nil {
		log.Fatalf("Failed to create state manager: %v", err)
	}

	return &CoffeeAnalyzer{
		Address:        config.ListeningAddress,
		mwAddr:         config.MiddlewareAddress,
		config:         config,
		queriesConfig:  config.Queries,
		parser:         []responseparser.ResponseParser{},
		jobPublisher:   jobPublisher,
		totalWorkers:   config.TotalWorkers,
		duplicateProb:  config.DuplicateProb,
		jobsState:      jobsState,
		jobsStateMutex: sync.Mutex{},
	}
}

func (ca *CoffeeAnalyzer) Start() {
	err := ca.RestoreAndCleanupInvalidJobs()
	if err != nil {
		log.Fatalf("Failed to restore jobs state: %v", err)
	}

	listener_socket := communication.Socket{}

	ca.StartJobCleanupService()

	err = listener_socket.BindAndListen(ca.Address)
	if err != nil {
		log.Fatalf("Failed to bind and listen: %v", err)
		return
	}
	defer listener_socket.Close()

	log.Infof("Listening on %s", ca.Address)

	for {
		client_socket, err := listener_socket.Accept()
		if err != nil {
			log.Fatalf("Failed to bind and listen: %v", err)
			return
		}

		id, err := client_socket.ReceiveUUID()
		if err != nil {
			client_socket.Close()
			continue
		}
		if id == uuid.Nil {
			ca.handleNewJobRequest(client_socket)
			continue
		}

		go ca.handleConnection(client_socket, id)
	}
}

func (ca *CoffeeAnalyzer) handleConnection(s *communication.Socket, id uuid.UUID) {
	defer s.Close()

	firstBatch, err := s.ReadBatch()
	if err != nil || len(firstBatch) == 0 {
		return
	}
	if communication.IsResponseRequest(firstBatch) {
		log.Infof("Received responses request")
		ca.handleGetResponsesRequest(s, id)
		return
	}

	ca.handleTableUpload(firstBatch, s, id)
}

func (ca *CoffeeAnalyzer) handleTableUpload(firstBatch []byte, s *communication.Socket, jobID uuid.UUID) {
	defer s.Close()
	var table string
	json.Unmarshal(firstBatch, &table)

	err := ca.logUploadTable(jobID, table)
	if err != nil {
		log.Errorf("Failed to log upload table operation for job %v, table %v: %v", jobID, table, err)
		return
	}

	log.Infof("Receiving table %v for job %v", table, jobID)
	producer, _ := middleware.NewProducer(table, ca.mwAddr, jobID.String())

	var header []string
	headerJson, err := s.ReadBatch()
	if err != nil {
		log.Errorf("Error reading batch: %v", err)
		return
	}
	json.Unmarshal(headerJson, &header)
	log.Infof("Received header: %v", header)
	var seqNumber uint64 = 0
	duplicated := 0
	for {
		data, err := s.ReadBatch()
		if err != nil {
			log.Infof("Finished receiving table %v: %v", table, err)
			break
		}
		var payload [][]interface{}
		json.Unmarshal(data, &payload)
		rowsBatch := innercommunication.NewRowsBatch(header, payload, seqNumber)
		rowsBatchMarshaled, _ := rowsBatch.Marshal()
		// Send batch once
		producer.Send(rowsBatchMarshaled)
		// With probability duplicateProb, send a duplicate
		if ca.duplicateProb > 0.0 {
			if rand.Float64() < ca.duplicateProb {
				duplicated++
				producer.Send(rowsBatchMarshaled)
			}
		}
		seqNumber++
	}
	endSignal := innercommunication.NewEndSignal(nil, seqNumber)
	endSignalMarshaled, _ := endSignal.Marshal()
	producer.Send(endSignalMarshaled)
	if ca.duplicateProb > 0.0 {
		// With probability duplicateProb, send a duplicate end signal
		if rand.Float64() < ca.duplicateProb {
			producer.Send(endSignalMarshaled)
		}
	}
	producer.Close()
	log.Infof("Finished receiving table: %v", table)

	err = ca.logTableUploadFinish(jobID, table)
	ca.jobsStateMutex.Lock()
	session, ok := ca.jobsState.GetState().(*jobsessions.JobSessionsState).GetSession(jobID)
	if ok {
		if session.IsUploadFinish() {
			log.Infof("Job %v upload finished", jobID)
		}
	}
	ca.jobsStateMutex.Unlock()

	if duplicated > 0 {
		log.Infof("Sent %d duplicated batches for table %v", duplicated, table)
	}
	if err != nil {
		log.Errorf("Failed to log finish upload table operation for job %v, table %v: %v", jobID, table, err)
		return
	}
}

func (ca *CoffeeAnalyzer) handleNewJobRequest(s *communication.Socket) {
	defer s.Close()
	log.Infof("Received new job request")
	uuid := uuid.New()

	err := ca.notifyNewJobToWorkersAndWait(uuid)
	if err != nil {
		log.Errorf("Error notifying workers: %v", err)
		return
	}

	// all workers ready, analyzer can start
	s.SendUUID(uuid)
}

func (ca *CoffeeAnalyzer) notifyNewJobToWorkersAndWait(id uuid.UUID) error {
	consumer, err := middleware.NewConsumer("jobs", id.String(), ca.mwAddr)
	if err != nil {
		return err
	}
	defer func() {
		consumer.Close()
	}()

	ready := make(chan bool, 1)
	readyCount := 0
	callback := func(consumeChannel middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		readyCount++
		if readyCount == ca.totalWorkers {
			ready <- true
		}
		done <- nil
	}

	go func() {
		if err := consumer.StartConsuming(callback); err != nil {
			log.Errorf("Failed to consume workers ready notification for job %v: %v", id, err)
		}
	}()

	bytes, _ := id.MarshalBinary()
	if err := ca.jobPublisher.Send(bytes); err != nil {
		return err
	}
	<-ready
	log.Infof("All workers ready for new job %v", id)
	return nil
}

func (ca *CoffeeAnalyzer) handleGetResponsesRequest(s *communication.Socket, id uuid.UUID) {
	parser := responseparser.NewResponseParser(id, ca.queriesConfig, ca.mwAddr, ca.jobsState, &ca.jobsStateMutex)
	ca.parser = append(ca.parser, *parser)

	err := ca.logResponsesRequestStart(id)
	if err != nil {
		log.Errorf("Failed to log start responses operation for job %v: %v", id, err)
		// here we could decide to send a response to the client indicating an error, that the job in not available
		return
	}

	parser.Start(s)
}

func (ca *CoffeeAnalyzer) jobCleanupService() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		<-ticker.C
		log.Infof("Running job cleanup service")
		currentTime := time.Now().Unix()
		ca.jobsStateMutex.Lock()
		sessions := ca.jobsState.GetState().(*jobsessions.JobSessionsState).GetAllSessions()
		for id, session := range sessions {
			period := currentTime - session.GetLastActivity()
			log.Infof("Checking job session %v: time since last activity %v, upload finished: %v", id, period, session.IsUploadFinish())
			if period > GRACE_PERIOD_SECONDS && session.IsUploadFinish() {
				log.Infof("Cleaning up job session %v due to inactivity", id)
				middleware.CleanupQueues(
					id.String(),
					ca.config.MiddlewareHTTPAddress,
					ca.config.MiddlewareUsername,
					ca.config.MiddlewarePassword,
				)
				state := ca.jobsState.GetState().(*jobsessions.JobSessionsState)
				state.RemoveSession(id)
			}
		}
		ca.jobsStateMutex.Unlock()
	}
}

func (ca *CoffeeAnalyzer) StartJobCleanupService() {
	go ca.jobCleanupService()
}

func (ca *CoffeeAnalyzer) logResponsesRequestStart(id uuid.UUID) error {
	op := jobsessions.NewStartResponsesOperation(id)
	ca.jobsStateMutex.Lock()
	err := ca.jobsState.Log(op)
	ca.jobsStateMutex.Unlock()
	return err
}

func (ca *CoffeeAnalyzer) logTableUploadFinish(id uuid.UUID, tableName string) error {
	op := jobsessions.NewFinishUploadOperation(id, tableName)
	ca.jobsStateMutex.Lock()
	err := ca.jobsState.Log(op)
	ca.jobsStateMutex.Unlock()
	return err
}

func (ca *CoffeeAnalyzer) logUploadTable(id uuid.UUID, tableName string) error {
	op := jobsessions.NewUploadTableOperation(id, tableName)
	ca.jobsStateMutex.Lock()
	err := ca.jobsState.Log(op)
	ca.jobsStateMutex.Unlock()
	return err
}

func (ca *CoffeeAnalyzer) RestoreAndCleanupInvalidJobs() error {
	log.Infof("Restoring job sessions state from persistance")
	ca.jobsStateMutex.Lock()
	defer ca.jobsStateMutex.Unlock()
	err := ca.jobsState.Restore()
	if err != nil {
		log.Errorf("Failed to restore job sessions state: %v", err)
		return err
	}
	// print the restores state
	log.Infof("Restored job sessions state: %v", ca.jobsState.GetState())
	sessions := ca.jobsState.GetState().(*jobsessions.JobSessionsState).GetAllSessions()
	for id, session := range sessions {
		if !session.IsUploadFinish() {
			log.Infof("Cleaning up incomplete job session %v during restore", id)
			middleware.CleanupQueues(
				id.String(),
				ca.config.MiddlewareHTTPAddress,
				ca.config.MiddlewareUsername,
				ca.config.MiddlewarePassword,
			)
			state := ca.jobsState.GetState().(*jobsessions.JobSessionsState)
			state.RemoveSession(id)
		}
	}
	return nil
}
