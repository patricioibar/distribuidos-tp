package filter

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const maxBatchBufferSize = 100

var log = logging.MustGetLogger("log")

type FilterWorker struct {
	input  mw.MessageMiddleware
	output mw.MessageMiddleware
	filterFunction mw.OnMessageCallback
	batchChan chan ic.RowsBatch
	closeChan chan struct{}
}


func NewFilter(input mw.MessageMiddleware, output mw.MessageMiddleware, filterType string) *FilterWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	filterFunction, err := getFilterFunction(batchChan, filterType)
	if err != nil {
		log.Fatalf("Failed to get filter function: %v", err)
	}

	return &FilterWorker{
		input:        input,
		output:       output,
		filterFunction: filterFunction,
		batchChan:   batchChan,
		closeChan:   make(chan struct{}),
	}
}


func getFilterFunction(batchChan chan ic.RowsBatch, filterType string) (mw.OnMessageCallback, error) {
	switch filterType {
	case "byYear":
		return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
			// funcion de filtrado

			jsonData := string(consumeChannel.Body)
			var batch ic.RowsBatch
			if err := json.Unmarshal([]byte(jsonData), &batch); err != nil {
				log.Errorf("Failed to unmarshal message: %v", err)
				done <- nil
				return
			}

			if len(batch.Rows) == 0 {
				log.Warning("Received empty batch")
				done <- nil
				return
			}

			filteredBatch, err := filterRowsByYear(batch)

			if err != nil {
				log.Errorf("Failed to filter rows by year: %v", err)
				done <- &mw.MessageMiddlewareError{
					Code: mw.MessageMiddlewareMessageError,
					Msg: "Failed to aggregate rows: " + err.Error(),
				}
				return
			}

			batchChan <- filteredBatch
			done <- nil
		}, nil
	default:
		return nil, errors.New("unknown filter type")
	}
}


func (f *FilterWorker) Start() {
	if err := f.input.StartConsuming(f.filterFunction); err != nil {
		// f.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	for {
		select {
			case <-f.closeChan:
				return
			case batch := <-f.batchChan:
				data, err := json.Marshal(batch)
				if err != nil {
					log.Errorf("Failed to marshal batch: %v", err)
					continue
				}
				if err := f.output.Send(data); err != nil {
					log.Errorf("Failed to send message: %v", err)
				}
		}
	}
}



func (f *FilterWorker) Close() {
	if err := f.input.StopConsuming(); err != nil {
		log.Errorf("Failed to stop consuming messages: %v", err)
	}
	if err := f.output.StopConsuming(); err != nil {
		log.Errorf("Failed to stop producing messages: %v", err)
	}

	close(f.closeChan)

}


func filterRowsByYear(batch ic.RowsBatch) (ic.RowsBatch, error) {
	var filteredRows [][]interface{}

	indexYear := -1 
	for i, header := range batch.ColumnNames {
		if header == "year" {
			indexYear = i
			break
		}
	}

	if indexYear == -1 {
		return ic.RowsBatch{}, errors.New("year column not found")
	}

	monthsOfFirstSemester := map[time.Month]bool{
		time.January:   true,
		time.February:  true,
		time.March:     true,
		time.April:     true,
		time.May:       true,
		time.June:      true,
	}

	for _, row := range batch.Rows {
		if len(row) <= indexYear {
			return ic.RowsBatch{}, errors.New("row does not have enough columns")
		}

		tsVal, ok := row[indexYear].(string)
		if !ok {
			return ic.RowsBatch{}, errors.New("year column is not a string")
		}
		timestamp, err := parseTimestamp(tsVal)
		if err != nil {
			return ic.RowsBatch{}, err
		}
		
		if timestamp.Year() == 2024 || timestamp.Year() == 2025 {
			if monthsOfFirstSemester[timestamp.Month()] {
				row = append(row, "FirstSemester")
			} else {
				row = append(row, "SecondSemester")
			}
			filteredRows = append(filteredRows, row)
		}
	}
	batch.ColumnNames = append(batch.ColumnNames, "semester")
	filteredBatch := ic.RowsBatch{
		ColumnNames: batch.ColumnNames,
		JobDone:     batch.JobDone,
		Rows:        filteredRows,
	}
	return filteredBatch, nil
}

func parseTimestamp (timestampStr string) (time.Time, error) {
	layout := "2006-01-02 15:04:05"
	return time.Parse(layout, timestampStr)
}