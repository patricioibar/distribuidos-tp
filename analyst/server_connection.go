package main

import (
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
	/*socket := communication.Socket{}

	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()*/

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
		var messageType string
		for {
			switch v.(type) {
			case *[]TransactionItem:
				v = &[]TransactionItem{}
				messageType = "TransactionItem"
			case *[]User:
				v = &[]User{}
				messageType = "User"
			case *[]MenuItem:
				v = &[]MenuItem{}
				messageType = "MenuItem"
			case *[]Transaction:
				v = &[]Transaction{}
				messageType = "Transaction"
			}
			fmt.Println(batchCount)

			err := reader.getBatch(batchCount, v)
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to read batch %v", err)
				return
			}

			if err == io.EOF {
				end = true
			}

			data, err := json.Marshal(v)
			if err != nil {
				log.Fatalf("Failed to send batch: %v", err)
				return
			}

			message := Message{
				Type: messageType,
				Data: data,
			}

			fmt.Printf("Unmarshalled data: %+v\n", message)

			data, err = json.Marshal(message)
			if err != nil {
				log.Fatalf("Failed to marshal batch: %v", err)
				return
			}

			fmt.Printf("Marshalled data: %+v\n", data)
			fmt.Printf("Data len: %d\n", len(data))

			//fmt.Printf("Marshalled data: %+v\n", data)

			/*err = socket.SendBatch(data)
			if err != nil {
				log.Fatalf("Failed to send batch: %v", err)
				return
			}*/

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
