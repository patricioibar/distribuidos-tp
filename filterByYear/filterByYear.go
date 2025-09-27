package filterbyyear

import (
	"encoding/json"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const maxBatchBufferSize = 100

type FilterWorker struct {
	input  mw.MessageMiddleware
	output mw.MessageMiddleware
	filterFunction mw.OnMessageCallback
	batchChan chan ic.RowsBatch
	closeChan chan struct{}
}


func NewFilterByYear(input mw.MessageMiddleware, output mw.MessageMiddleware) *FilterWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	filterByYear := getFilterFunction(batchChan)

	return &FilterWorker{
		input:        input,
		output:       output,
		filterFunction: filterByYear,
		batchChan:   batchChan,
		closeChan:   make(chan struct{}),
	}
}


func getFilterFunction(batchChan chan ic.RowsBatch) mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		// funcion de filtrado
		var filteredBatch ic.RowsBatch


		batchChan <- filteredBatch
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
