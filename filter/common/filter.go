package filter

import (
	"errors"
	"slices"
	"sync"

	"github.com/op/go-logging"
	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

type FilterWorker struct {
	filterId        string
	workersCount    int
	input           mw.MessageMiddleware
	output          mw.MessageMiddleware
	filterFunction  mw.OnMessageCallback
	totallyFiltered *bitmap.Bitmap
	seenBatches     *bitmap.Bitmap
	stopOnce        sync.Once
	removeFromMap   chan string
	jobId           string
}

func NewFilter(
	workerID string,
	input mw.MessageMiddleware,
	output mw.MessageMiddleware,
	filterType string,
	workersCount int,
	removeFromMap chan string,
	jobId string,
) *FilterWorker {
	fw := &FilterWorker{
		filterId:        workerID,
		workersCount:    workersCount,
		input:           input,
		output:          output,
		filterFunction:  nil,
		totallyFiltered: bitmap.New(),
		seenBatches:     bitmap.New(),
		removeFromMap:   removeFromMap,
		jobId:           jobId,
	}

	filterFunction, err := fw.getFilterFunction(filterType)
	if err != nil {
		log.Fatalf("Failed to get filter function: %v", err)
	}

	fw.filterFunction = filterFunction

	return fw
}

func (f *FilterWorker) getFilterFunction(filterType string) (mw.OnMessageCallback, error) {
	var filterFunction func(*ic.RowsBatchPayload) (*ic.Message, error)
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
		var receivedMsg ic.Message
		if err := receivedMsg.Unmarshal([]byte(jsonData)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		switch p := receivedMsg.Payload.(type) {
		case *ic.RowsBatchPayload:
			f.filterBatch(p, filterFunction)
			done <- nil

		case *ic.SequenceSetPayload:
			f.totallyFiltered.Or(p.Sequences.Bitmap)

		case *ic.EndSignalPayload:
			shouldAck := f.handleEndSignal(p)
			if shouldAck {
				done <- nil
			} else {
				done <- &mw.MessageMiddlewareError{Code: 0, Msg: "end signal contains this worker already"}
				// sleep?
			}

		default:
			log.Errorf("Unexpected message type: %s", receivedMsg.Type)
			done <- nil
		}

	}, nil
}

func (f *FilterWorker) filterBatch(p *ic.RowsBatchPayload, filterFunction func(*ic.RowsBatchPayload) (*ic.Message, error)) {
	if f.seenBatches.Contains(p.SeqNum) {
		log.Debugf("Batch %d already processed, skipping.", p.SeqNum)
		return
	}

	f.seenBatches.Add(p.SeqNum)
	if len(p.Rows) == 0 {
		return
	}
	filteredBatch, err := filterFunction(p)
	if err != nil {
		log.Errorf("Failed to filter batch: %v", err)
		return
	}
	filteredBatchPayload := filteredBatch.Payload.(*ic.RowsBatchPayload)
	log.Debugf("Filter %s processed batch: %d input rows -> %d output rows", f.filterId, len(p.Rows), len(filteredBatchPayload.Rows))

	if len(filteredBatchPayload.Rows) > 0 {
		batchBytes, err := filteredBatch.Marshal()
		if err != nil {
			log.Errorf("Failed to marshal batch: %v", err)
		}
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to send message: %v", err)
		}

	} else {
		log.Debugf("Filter %s: No rows passed the filter criteria", f.filterId)
		f.totallyFiltered.Add(p.SeqNum)
	}
}

func (f *FilterWorker) Start() {
	// blocks until all workers are done
	if err := f.input.StartConsuming(f.filterFunction); err != nil {
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	f.Close()
}

func (f *FilterWorker) Close() {
	f.stopOnce.Do(func() {
		// Close input
		log.Info("FilterWorker shutdown initiated...")
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

		f.removeFromMap <- f.jobId

		log.Info("FilterWorker shutdown completed.")
	})
}

func (f *FilterWorker) handleEndSignal(payload *ic.EndSignalPayload) bool {
	if slices.Contains(payload.WorkersDone, f.filterId) {
		// This worker has already sent its end signal (duplicated message, ignore)
		return false
	}
	log.Debugf("Worker received end signal. Task ended.")

	if f.totallyFiltered.GetCardinality() > 0 {
		totallyFilteredMsg := ic.NewSequenceSet(f.filterId, f.totallyFiltered)
		batchBytes, _ := totallyFilteredMsg.Marshal()
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to send message: %v", err)
		}
		// cleaning totallyFiltered so it doesnt send
		// duplicated messages to next stage
		f.totallyFiltered = bitmap.New()
	}

	payload.AddWorkerDone(f.filterId)

	if len(payload.WorkersDone) == f.workersCount {
		log.Info("All workers done. Sending end signal to next stage and deleting input.")
		endSignal := ic.NewEndSignal(nil, payload.SeqNum)
		batchBytes, _ := endSignal.Marshal()
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to propagate end signal message: %v", err)
		}
		f.input.Delete()
		return true
	}

	endBatch := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := endBatch.Marshal()
	f.input.Send(endSignal)
	return true
}
