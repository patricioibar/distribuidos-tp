package main

import (
	"encoding/csv"
	"io"
	"os"

	"communication"
)

type Reader struct {
	FilePath  string
	BatchSize int
}

func (r *Reader) getHeader() ([]string, error) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (r *Reader) SendFileTroughSocket(columnsIdxs []int, socket communication.Socket) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", r.FilePath, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, _ = reader.Read() // Skip header

	eof := false
	errCount := 0
	rows := [][]string{}
	for {
		for i := 0; i < r.BatchSize; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				eof = true
				break
			}
			if err != nil {
				errCount++
				if errCount > 5 {
					log.Fatalf("Failed to read record from file %s: %v", r.FilePath, err)
					return
				}
				continue
			}
			filtered := make([]string, len(columnsIdxs))
			skip := false
			for i, idx := range columnsIdxs {
				if idx < 0 || idx >= len(record) {
					skip = true
					break
				}
				if record[idx] == "" {
					filtered[i] = "NULL"
					continue
				}
				filtered[i] = record[idx]
			}
			if skip {
				continue
			}
			rows = append(rows, filtered)
		}

		if len(rows) != 0 {
			sendRowsTroughSocket(rows, socket)
			rows = [][]string{}
		}

		if eof {
			break
		}
	}
}
