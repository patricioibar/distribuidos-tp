package analyst

import (
	"io"
	"os"
)

type Reader struct {
	FilePath  string
	ChunkSize int
}

func (r *Reader) getChunksCount() (int, error) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		//log("Failed to open file:", err)
		return 0, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		//log("Failed to stat file:", err)
		return 0, err
	}
	fileSize := int(fileInfo.Size())
	numChunks := (fileSize + r.ChunkSize - 1) / r.ChunkSize

	return numChunks, nil
}

func (r *Reader) getChunk(seqNumber int) ([]byte, error) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := int64(r.ChunkSize * seqNumber)
	buf := make([]byte, r.ChunkSize)

	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buf[:n], nil
}
