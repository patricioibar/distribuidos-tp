package main

import (
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

		for {
			switch v.(type) {
			case *[]TransactionItem:
				v = &[]TransactionItem{}
			case *[]User:
				v = &[]User{}
			case *[]MenuItem:
				v = &[]MenuItem{}
			case *[]Transaction:
				v = &[]Transaction{}
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

			fmt.Printf("Unmarshalled data: %+v\n", v)

			/*data, err := json.Marshal(v)
			if err != nil {
				log.Fatalf("Failed to send batch: %v", err)
				return
			}*/

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
