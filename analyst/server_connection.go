package analyst

import (
	"encoding/json"
	"io"

	"github.com/patricioibar/distribuidos-tp/communication"
)

type ServerConnection struct {
	BatchSize             int
	CoffeeAnalyzerAddress string
}

func (s *ServerConnection) sendDataset(filePath string, fileType FileType) {
	socket := communication.Socket{}

	err := socket.Connect(s.CoffeeAnalyzerAddress)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	reader := Reader{FilePath: filePath, BatchSize: s.BatchSize}
	batchCount := 0
	var data interface{}
	end := false

	for {

		switch fileType {
		case Transactions:
			data = []Transaction{}
		case TransactionItems:
			data = []TransactionItem{}
		case Users:
			data = []User{}
		case MenuItems:
			data = []MenuItem{}
		}

		err := reader.getBatch(batchCount, &data)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read batch %v", err)
			return
		}

		if err == io.EOF {
			end = true
		}

		data, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("Failed to send batch: %v", err)
			return
		}

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
