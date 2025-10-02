package main

import (
	"encoding/json"

	"communication"

	"github.com/patricioibar/distribuidos-tp/innercommunication"

	"github.com/patricioibar/distribuidos-tp/middleware"
)

type UserConnection struct {
	Address string
	mwAddr  string
}

func (u *UserConnection) Start() {
	listener_socket := communication.Socket{}

	err := listener_socket.BindAndListen(u.Address)
	if err != nil {
		log.Fatalf("Failed to bind and listen: %v", err)
		return
	}
	defer listener_socket.Close()

	log.Infof("Listening on %s", u.Address)

	for {
		client_socket, err := listener_socket.Accept()
		if err != nil {
			log.Fatalf("Failed to bind and listen: %v", err)
			return
		}

		log.Infof("Accepted connection")

		go handleConnection(client_socket, u.mwAddr)
	}
}

func handleConnection(s *communication.Socket, mwAddr string) {
	defer s.Close()

	var table string
	tableBytes, err := s.ReadBatch()
	if err != nil {
		log.Errorf("Error reading batch: %v", err)
		return
	}
	json.Unmarshal(tableBytes, &table)
	log.Infof("Received table: %v", table)
	producer, _ := middleware.NewProducer(table, mwAddr)

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
			log.Errorf("Error reading batch: %v", err)
			break
		}

		/*message := communication.Message{
			Type: "",
			Data: nil,
		}*/
		var payload [][]interface{}

		json.Unmarshal(data, &payload)
		/*
			switch message.Type {
			case "TransactionItem":
				var items []communication.TransactionItem
				err := json.Unmarshal(message.Data, &items)
				if err != nil {
					log.Errorf("Failed to unmarshal TransactionItem: %v", err)
					continue
				}
				log.Infof("Received %d TransactionItems", len(items))
				// Process TransactionItems as needed

			case "User":
				var users []communication.User
				err := json.Unmarshal(message.Data, &users)
				if err != nil {
					log.Errorf("Failed to unmarshal User: %v", err)
					continue
				}
				log.Infof("Received %d Users", len(users))
				// Process Users as needed

			case "MenuItem":
				var menuItems []communication.MenuItem
				err := json.Unmarshal(message.Data, &menuItems)
				if err != nil {
					log.Errorf("Failed to unmarshal MenuItem: %v", err)
					continue
				}
				log.Infof("Received %d MenuItems", len(menuItems))
				// Process MenuItems as needed

			case "Transaction":
				var transactions []communication.Transaction
				err := json.Unmarshal(message.Data, &transactions)
				if err != nil {
					log.Errorf("Failed to unmarshal Transaction: %v", err)
					continue
				}
				log.Infof("Received %d Transactions", len(transactions))
				// Process Transactions as needed

			default:
				log.Errorf("Unknown message type: %s", message.Type)
			}*/
		// log.Infof("Received payload: %+v", payload)

		rowsBatch := innercommunication.NewRowsBatch(header, payload)
		rowsBatchMarshaled, _ := rowsBatch.Marshal()
		producer.Send(rowsBatchMarshaled)
	}
}
