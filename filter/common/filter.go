package filter

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/op/go-logging"
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
	totallyFiltered *roaring.Bitmap
	closeChan       chan struct{}
	closeOnce       sync.Once
	StopOnce        sync.Once
}

func NewFilter(workerID string, input mw.MessageMiddleware, output mw.MessageMiddleware, filterType string, workersCount int) *FilterWorker {
	fw := &FilterWorker{
		filterId:        workerID,
		workersCount:    workersCount,
		input:           input,
		output:          output,
		filterFunction:  nil,
		totallyFiltered: roaring.New(),
		closeChan:       make(chan struct{}),
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

			if len(p.Rows) != 0 {
				filteredBatch, err := filterFunction(p)
				if err == nil {
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
			}

			done <- nil

		case *ic.SequenceSetPayload:
			f.totallyFiltered.Or(p.Sequences.Bitmap)

		case *ic.EndSignalPayload:
			f.handleEndSignal(p)
			done <- nil
			f.closeOnce.Do(func() { close(f.closeChan) })

		default:
			fmt.Println("Tipo de payload desconocido")
			done <- nil
		}

	}, nil
}

func (f *FilterWorker) Start() {
	if err := f.input.StartConsuming(f.filterFunction); err != nil {
		// f.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	<-f.closeChan
	f.Close()
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
		f.closeOnce.Do(func() { close(f.closeChan) })

		log.Info("FilterWorker shutdown completed.")
	})
}

func (f *FilterWorker) handleEndSignal(payload *ic.EndSignalPayload) {
	if slices.Contains(payload.WorkersDone, f.filterId) {
		// This worker has already sent its end signal (duplicated message, ignore)
		return
	}
	log.Debugf("Worker received end signal. Task ended.")

	if f.totallyFiltered.GetCardinality() > 0 {
		totallyFilteredMsg := ic.NewSequenceSet(f.filterId, f.totallyFiltered)
		batchBytes, _ := totallyFilteredMsg.Marshal()
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to send message: %v", err)
		}
	}

	payload.AddWorkerDone(f.filterId)

	if len(payload.WorkersDone) == f.workersCount {
		log.Info("All workers done. Sending end signal to next stage.")
		endSignal := ic.NewEndSignal(nil, payload.SeqNum)
		batchBytes, _ := endSignal.Marshal()
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to propagate end signal message: %v", err)
		}
		return
	}

	endBatch := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := endBatch.Marshal()
	f.input.Send(endSignal)
}
