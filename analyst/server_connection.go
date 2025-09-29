package analyst

import (
	"encoding/json"
	"io"
	"net"

	"github.com/patricioibar/distribuidos-tp/communication"
)

type ServerConnection struct {
	Conn      net.Conn
	BatchSize int
}

func (s *ServerConnection) sendDataset(serverAddr, filePath string) {
	socket := communication.Socket{}

	err := socket.Connect(serverAddr)
	if err != nil {
		//log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	reader := Reader{FilePath: filePath, BatchSize: s.BatchSize}
	batchCount := 0
	var transactions []Transaction

	for {
		err := reader.getBatch(batchCount, &transactions)
		if err != nil && err != io.EOF {
			//log.Fatalf("Failed to read chunk %d: %v", seq, err)
			return
		}
		// Send chunk length (uint32) + sequence number (uint32) + chunk data

		//err = socket.SendChunk(seq, len(chunk), chunk)
		data, err := json.Marshal(transactions)
		if err != nil {
			//log.Fatalf("Failed to send chunk %d: %v", seq, err)
			return
		}

		err = socket.SendBatch(data)
		if err != nil {
			//log.Fatalf("Failed to send chunk %d: %v", seq, err)
			return
		}

		// Wait for server ack (sequence number, uint32)

		/*ackSeq, err := socket.RecvAck()
		if ackSeq != seq {
			//log.Fatalf("Ack sequence mismatch: got %d, expected %d", ackSeq, seq)
			return
		}*/

		//log.Infof("Batch %d sent successfully", seq)
		batchCount += s.BatchSize
	}

	//fmt.Println("File sent successfully.")
	//log.Infof("File sent successfully.")
}
