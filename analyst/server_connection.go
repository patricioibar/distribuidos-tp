package main

import (
	"communication"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
}

func (s *ServerConnection) sendDataset(dir string, v interface{}) {
	socket := communication.Socket{}

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
		batchCount := 0
		end := false
		//var messageType string

		dirJson, _ := json.Marshal(dir)
		//habria que cambiarle el nombre al metodo
		err := socket.SendBatch(dirJson)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send dir %v", err)
			return
		}

		header, _ := reader.getHeader()
		headerJson, _ := json.Marshal(header)
		fmt.Printf("Header: %s\n", header)
		fmt.Printf("Header: %s\n", headerJson)

		//habria que cambiarle el nombre al metodo
		err = socket.SendBatch(headerJson)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to send header %v", err)
			return
		}

		for {
			/*
				switch v.(type) {
				case *[]communication.TransactionItem:
					v = &[]communication.TransactionItem{}
					messageType = "TransactionItem"
				case *[]communication.User:
					v = &[]communication.User{}
					messageType = "User"
				case *[]communication.MenuItem:
					v = &[]communication.MenuItem{}
					messageType = "MenuItem"
				case *[]communication.Transaction:
					v = &[]communication.Transaction{}
					messageType = "Transaction"
				}*/
			fmt.Println(batchCount)

			rows, err := reader.getBatch(batchCount)
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to read batch %v", err)
				return
			}

			if err == io.EOF {
				end = true
			}

			data, err := json.Marshal(rows)
			if err != nil {
				log.Fatalf("Failed to send batch: %v", err)
				return
			}
			/*
				message := communication.Message{
					Type: messageType,
					Data: data,
				}*/

			//fmt.Printf("Unmarshalled data: %+v\n", message)
			fmt.Printf("Unmarshalled data: %+v\n", rows)
			//data, err = json.Marshal(data)
			/*if err != nil {
				log.Fatalf("Failed to marshal batch: %v", err)
				return
			}*/

			fmt.Printf("Marshalled data: %+v\n", data)
			fmt.Printf("Data len: %d\n", len(data))

			//fmt.Printf("Marshalled data: %+v\n", data)

			err = socket.SendBatch(data)
			if err != nil {
				log.Fatalf("Failed to send batch: %v", err)
				return
			}

			log.Infof("Batch sent successfully")
			batchCount += s.BatchSize

			if end {
				break
			}
		}

		log.Infof("File sent successfully.")
	}

	log.Infof("All files from directory %s sent successfully.", dir)
}
