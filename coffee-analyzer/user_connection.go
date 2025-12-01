package main

import (
	responseparser "cofee-analyzer/response_parser"
	"encoding/json"
	"math/rand"
	"time"

	"communication"

	"github.com/patricioibar/distribuidos-tp/innercommunication"

	"github.com/patricioibar/distribuidos-tp/middleware"

	uuid "github.com/google/uuid"
)

type CoffeeAnalyzer struct {
	Address       string
	mwAddr        string
	queriesConfig []responseparser.QueryOutput
	parser        []responseparser.ResponseParser
	jobPublisher  *middleware.Producer
	totalWorkers  int
	duplicateProb float64
}

const jobPublishingExchange = "JOB_SOURCE"

func NewCoffeeAnalyzer(config *Config) *CoffeeAnalyzer {
	jobPublisher, _ := middleware.NewProducer(jobPublishingExchange, config.MiddlewareAddress)

	// Seed RNG for duplicated message sampling
	rand.Seed(time.Now().UnixNano())

	return &CoffeeAnalyzer{
		Address:       config.ListeningAddress,
		mwAddr:        config.MiddlewareAddress,
		queriesConfig: config.Queries,
		parser:        []responseparser.ResponseParser{},
		jobPublisher:  jobPublisher,
		totalWorkers:  config.TotalWorkers,
		duplicateProb: config.DuplicateProb,
	}
}

func (ca *CoffeeAnalyzer) Start() {
	listener_socket := communication.Socket{}

	err := listener_socket.BindAndListen(ca.Address)
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
	}

	ca.handleTableUpload(firstBatch, s, id)
}

func (ca *CoffeeAnalyzer) handleTableUpload(firstBatch []byte, s *communication.Socket, jobID uuid.UUID) {
	defer s.Close()
	var table string
	json.Unmarshal(firstBatch, &table)
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
	if duplicated > 0 {
		log.Infof("Sent %d duplicated batches for table %v", duplicated, table)
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
	parser := responseparser.NewResponseParser(id, ca.queriesConfig, ca.mwAddr)
	ca.parser = append(ca.parser, *parser)
	parser.Start(s)
}
