package filter

import (
	"encoding/json"
	"errors"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const maxBatchBufferSize = 100

var log = logging.MustGetLogger("log")

type FilterWorker struct {
	filterId string
	workersCount int
	input  mw.MessageMiddleware
	output mw.MessageMiddleware
	filterFunction mw.OnMessageCallback
	batchChan chan ic.RowsBatch
	closeChan chan struct{}
}


func NewFilter(workerID string, input mw.MessageMiddleware, output mw.MessageMiddleware, filterType string) *FilterWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	fw := &FilterWorker{
		filterId: 	workerID,
		input:        input,
		output:       output,
		filterFunction: nil,
		batchChan:   batchChan,
		closeChan:   make(chan struct{}),
	}

	filterFunction, err := fw.getFilterFunction(batchChan, filterType)
	if err != nil {
		log.Fatalf("Failed to get filter function: %v", err)
	}

	fw.filterFunction = filterFunction

	return fw
}


func (f *FilterWorker) getFilterFunction(batchChan chan ic.RowsBatch, filterType string) (mw.OnMessageCallback, error) {
	var filterFunction func(ic.RowsBatch) (ic.RowsBatch, error)
	switch filterType {
	case "TbyYear":
		filterFunction = filterRowsByYear
	case "TbyHour":
		filterFunction = filterRowsByHour
	case "TbyAmount":
		filterFunction = filterRowsByTransactionAmount
	case "TIbyYear":
		filterFunction = filterTransactionItemsByYear
	default:
		return nil, errors.New("unknown filter type")
	}

	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		jsonData := string(consumeChannel.Body)
		var batch ic.RowsBatch
		if err := json.Unmarshal([]byte(jsonData), &batch); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if len(batch.Rows) != 0 {
			filteredBatch, err := filterFunction(batch)
			if err != nil {
				log.Errorf("Failed to filter rows by year: %v", err)
				done <- &mw.MessageMiddlewareError{
					Code: mw.MessageMiddlewareMessageError,
					Msg: "Failed to filter rows: " + err.Error(),
				}
			}
			batchChan <- filteredBatch
			done <- nil
		}

		if batch.IsEndSignal() {
			f.handleEndSignal(&batch)
		}

		done <- nil

	}, nil
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
	log.Info("FilterWorker shutdown initiated...")
	
	// First, signal the worker to stop processing new messages
	select {
	case <-f.closeChan:
		// Already closed
		log.Debug("FilterWorker already closed.")
		return
	default:
		close(f.closeChan)
	}
	
	log.Debug("Stop signal sent to FilterWorker.")
	
	// Stop consuming new messages from input
	if f.input != nil {
		if err := f.input.StopConsuming(); err != nil {
			log.Errorf("Failed to stop input consuming: %v", err)
		} else {
			log.Debug("Input consumer stopped.")
		}
	}
	
	// Stop the output producer
	if f.output != nil {
		if err := f.output.StopConsuming(); err != nil {
			log.Errorf("Failed to stop output producer: %v", err)
		} else {
			log.Debug("Output producer stopped.")
		}
	}
	
	log.Info("FilterWorker shutdown completed.")
}

func (f *FilterWorker) handleEndSignal(batch *ic.RowsBatch) {

	log.Debugf("Worker received end signal. Task ended.")

	batch.AddWorkerDone(f.filterId)

	if len(batch.WorkersDone) == f.workersCount {
		log.Debug("All workers done. Sending end signal to next stage.")
		endSignal, _ := ic.NewEndSignal().Marshal()
		f.output.Send(endSignal)
		return
	}

	endBatch := ic.RowsBatch{
		EndSignal:  batch.EndSignal,
		WorkersDone: batch.WorkersDone,
		Rows: nil,
		ColumnNames: nil,
	}
	endSignal, _ := endBatch.Marshal()
	f.input.Send(endSignal)
}