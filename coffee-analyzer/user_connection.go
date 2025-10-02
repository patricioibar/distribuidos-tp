package main

import (
	responseparser "cofee-analyzer/response_parser"
	"encoding/json"

	"communication"

	"github.com/patricioibar/distribuidos-tp/innercommunication"

	"github.com/patricioibar/distribuidos-tp/middleware"
)

type CoffeeAnalyzer struct {
	Address string
	mwAddr  string
	parser  *responseparser.ResponseParser
}

func NewCoffeeAnalyzer(config *Config) *CoffeeAnalyzer {
	parser := responseparser.NewResponseParser(config.Queries, config.MiddlewareAddress)
	return &CoffeeAnalyzer{
		Address: config.ListeningAddress,
		mwAddr:  config.MiddlewareAddress,
		parser:  parser,
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

		log.Infof("Accepted connection")

		go ca.handleConnection(client_socket)
	}
}

func (ca *CoffeeAnalyzer) handleConnection(s *communication.Socket) {
	defer s.Close()

	firstBatch, err := s.ReadBatch()
	if err != nil {
		log.Errorf("Error reading batch: %v", err)
		return
	}
	if communication.IsResponseRequest(firstBatch) {
		log.Infof("Received responses request")
		ca.handleGetResponsesRequest(s)
	}

	ca.handleTableUpload(firstBatch, s)
}

func (ca *CoffeeAnalyzer) handleTableUpload(firstBatch []byte, s *communication.Socket) {
	var table string
	json.Unmarshal(firstBatch, &table)
	log.Infof("Received table: %v", table)
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

func (ca *CoffeeAnalyzer) handleGetResponsesRequest(s *communication.Socket) {
	ca.parser.Start(s)
}
