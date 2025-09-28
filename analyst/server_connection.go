package analyst

import (
	"io"
	"net"
)

type ServerConnection struct {
	Conn      net.Conn
	ChunkSize int
}

func (s *ServerConnection) sendDataset(serverAddr, filePath string) {
	socket := Socket{}

	err := socket.Connect(serverAddr)
	if err != nil {
		//log.Fatalf("Failed to connect: %v", err)
		return
	}
	defer socket.Close()

	reader := Reader{FilePath: filePath, ChunkSize: s.ChunkSize}
	numChunks, err := reader.getChunksCount()
	if err != nil {
		//log.Fatalf("Failed to get file info: %v", err)
		return
	}

	// Send number of chunks (uint32)
	err = socket.SendChunksCount(numChunks)
	if err != nil {
		//log.Fatalf("Failed to send chunks count: %v", err)
		return
	}

	for seq := uint32(0); seq < uint32(numChunks); seq++ {
		chunk, err := reader.getChunk(int(seq))
		if err != nil && err != io.EOF {
			//log.Fatalf("Failed to read chunk %d: %v", seq, err)
			return
		}
		// Send chunk length (uint32) + sequence number (uint32) + chunk data

		err = socket.SendChunk(seq, len(chunk), chunk)
		if err != nil {
			//log.Fatalf("Failed to send chunk %d: %v", seq, err)
			return
		}

		// Wait for server ack (sequence number, uint32)

		ackSeq := socket.RecvAck()
		if ackSeq != seq {
			//log.Fatalf("Ack sequence mismatch: got %d, expected %d", ackSeq, seq)
			return
		}

		//log.Infof("Chunk %d sent successfully", seq)
	}

	//fmt.Println("File sent successfully.")
	//log.Fatalf("File sent successfully.")
}
