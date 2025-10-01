package main

import (
	"encoding/json"

	"communication"
)

type UserConnection struct {
	Address string
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

		go handleConnection(client_socket)
	}
}

func handleConnection(s *communication.Socket) {
	defer s.Close()

	for {
		data, err := s.ReadBatch()
		if err != nil {
			log.Errorf("Error reading batch: %v", err)
			break
		}

		message := communication.Message{
			Type: "",
			Data: nil,
		}

		json.Unmarshal(data, &message)

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
		}
	}
}
