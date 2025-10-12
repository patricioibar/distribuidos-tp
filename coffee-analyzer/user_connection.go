package main

import (
	responseparser "cofee-analyzer/response_parser"
	"encoding/json"

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
}

func NewCoffeeAnalyzer(config *Config) *CoffeeAnalyzer {
	return &CoffeeAnalyzer{
		Address:       config.ListeningAddress,
		mwAddr:        config.MiddlewareAddress,
		queriesConfig: config.Queries,
		parser:        []responseparser.ResponseParser{},
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
			go ca.handleNewJobRequest(client_socket)
		}

		go ca.handleConnection(client_socket, id)
	}
}

func (ca *CoffeeAnalyzer) handleConnection(s *communication.Socket, id uuid.UUID) {
	defer s.Close()

	firstBatch, err := s.ReadBatch()
	if err != nil {
		return
	}
	if communication.IsResponseRequest(firstBatch) {
		log.Infof("Received responses request")
		ca.handleGetResponsesRequest(s, id)
	}

	ca.handleTableUpload(firstBatch, s, id)
}

func (ca *CoffeeAnalyzer) handleTableUpload(firstBatch []byte, s *communication.Socket, id uuid.UUID) {
	defer s.Close()
	var table string
	json.Unmarshal(firstBatch, &table)
	log.Infof("Receiving table %v for job %v", table, id)
	producer, _ := middleware.NewProducer(table, ca.mwAddr)

	var header []string
	headerJson, err := s.ReadBatch()
	if err != nil {
		log.Errorf("Error reading batch: %v", err)
		return
	}
	json.Unmarshal(headerJson, &header)
	log.Infof("Received header: %v", header)
	for {
		data, err := s.ReadBatch()
		if err != nil {
			log.Infof("Finished receiving table %v: %v", table, err)
			break
		}
		var payload [][]interface{}
		json.Unmarshal(data, &payload)
		rowsBatch := innercommunication.NewRowsBatch(header, payload)
		rowsBatchMarshaled, _ := rowsBatch.Marshal()
		producer.Send(rowsBatchMarshaled)
	}
	endSignal := innercommunication.NewEndSignal()
	endSignalMarshaled, _ := endSignal.Marshal()
	producer.Send(endSignalMarshaled)
	producer.Close()
	log.Infof("Finished receiving table: %v", table)
}

func (ca *CoffeeAnalyzer) handleNewJobRequest(s *communication.Socket) {
	defer s.Close()
	log.Infof("Received new job request")
	uuid := uuid.New()
	s.SendUUID(uuid)
}

func (ca *CoffeeAnalyzer) handleGetResponsesRequest(s *communication.Socket, id uuid.UUID) {
	defer s.Close()
	parser := responseparser.NewResponseParser(id, ca.queriesConfig, ca.mwAddr)
	ca.parser = append(ca.parser, *parser)
	parser.Start(s)
}
