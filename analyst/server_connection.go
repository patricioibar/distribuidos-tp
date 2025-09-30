package main

import (
	"fmt"
	"io"
)

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
}

func (s *ServerConnection) sendDataset(filePath string, v interface{}) {
	/*socket := communication.Socket{}

	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()*/

	reader := Reader{FilePath: filePath, BatchSize: s.BatchSize}
	batchCount := 0
	end := false

	for {
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
