package filter

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const maxBatchBufferSize = 100

var log = logging.MustGetLogger("log")

type FilterWorker struct {
	filterId       string
	workersCount   int
	input          mw.MessageMiddleware
	output         mw.MessageMiddleware
	filterFunction mw.OnMessageCallback
	batchChan      chan ic.RowsBatch
	closeChan      chan struct{}
	StopOnce       sync.Once
}

func NewFilter(workerID string, input mw.MessageMiddleware, output mw.MessageMiddleware, filterType string, workersCount int) *FilterWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	fw := &FilterWorker{
		filterId:       workerID,
		workersCount:   workersCount,
		input:          input,
		output:         output,
		filterFunction: nil,
		batchChan:      batchChan,
		closeChan:      make(chan struct{}),
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
			if err == nil {
				log.Debugf("Filter %s processed batch: %d input rows -> %d output rows", f.filterId, len(batch.Rows), len(filteredBatch.Rows))
				if len(filteredBatch.Rows) > 0 {
					batchChan <- filteredBatch
				} else {
					log.Debugf("Filter %s: No rows passed the filter criteria", f.filterId)
				}
			}
		}

		if batch.IsEndSignal() {
			f.handleEndSignal(&batch)
			close(f.closeChan)
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
			f.Close()
			return
		case batch := <-f.batchChan:
			data, err := json.Marshal(batch)
			if err != nil {
				log.Errorf("Failed to marshal batch: %v", err)
				continue
			}
			log.Debugf("Filter %s sending %d filtered rows to output exchange", f.filterId, len(batch.Rows))
			if err := f.output.Send(data); err != nil {
				log.Errorf("Failed to send message: %v", err)
			} else {
				log.Debugf("Filter %s successfully sent batch to output exchange", f.filterId)
			}
		}
	}
}

func (f *FilterWorker) Close() {
	log.Info("FilterWorker shutdown initiated...")

	// Stop consuming new messages from input
	f.StopOnce.Do(func() {
		if f.input != nil {
			if err := f.input.Close(); err != nil {
				log.Errorf("Failed to stop input consuming: %v", err)
			} else {
				log.Debug("Input consumer stopped.")
			}
		}

		// Stop the output producer
		if f.output != nil {
			if err := f.output.Close(); err != nil {
				log.Errorf("Failed to stop output producer: %v", err)
			} else {
				log.Debug("Output producer stopped.")
			}
		}

		log.Info("FilterWorker shutdown completed.")
	})
}

func (f *FilterWorker) handleEndSignal(batch *ic.RowsBatch) {

	log.Debugf("Worker received end signal. Task ended.")

	batch.AddWorkerDone(f.filterId)

	if len(batch.WorkersDone) == f.workersCount {
		log.Info("All workers done. Sending end signal to next stage.")
		endSignal, _ := ic.NewEndSignal().Marshal()
		f.output.Send(endSignal)
		return
	}

	endBatch := ic.RowsBatch{
		EndSignal:   batch.EndSignal,
		WorkersDone: batch.WorkersDone,
		Rows:        nil,
		ColumnNames: nil,
	}
	endSignal, _ := endBatch.Marshal()
	f.input.Send(endSignal)
}
