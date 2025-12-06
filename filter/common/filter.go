package filter

import (
	"errors"
	"os"
	"slices"
	"sync"

	"github.com/op/go-logging"
	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"

	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

const StateRoot = ".state"

var log = logging.MustGetLogger("log")

type FilterWorker struct {
	filterId       string
	workersCount   int
	input          mw.MessageMiddleware
	output         mw.MessageMiddleware
	filterFunction mw.OnMessageCallback
	stateManager   *pers.StateManager
	stopOnce       sync.Once
	removeFromMap  chan string
	jobId          string
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
		filterId:       workerID,
		workersCount:   workersCount,
		input:          input,
		output:         output,
		filterFunction: nil,
		removeFromMap:  removeFromMap,
		jobId:          jobId,
	}

	// Initialize persistent state for this filter using jobId as directory
	// state dir: <StateRoot>/<jobId>
	sm, _ := fw.initStateManager(jobId)
	fw.stateManager = sm
	// StateManager is required for FilterWorker to function. If init failed,
	// abort early — the caller should not create a FilterWorker without
	// persistent state available.
	if fw.stateManager == nil {
		log.Fatalf("StateManager not initialized for job %s", jobId)
	}

	filterFunction, err := fw.getFilterFunction(filterType)
	if err != nil {
		log.Fatalf("Failed to get filter function: %v", err)
	}

	fw.filterFunction = filterFunction

	return fw
}

func (f *FilterWorker) filterState() *FilterState {
	st := f.stateManager.GetState()
	if fs, ok := st.(*FilterState); ok {
		return fs
	}
	return nil
}

// initStateManager initializes persistent state for a job. It returns the
// StateManager and the FilterState that was restored (or nil if none).
func (f *FilterWorker) initStateManager(jobId string) (*pers.StateManager, *FilterState) {
	stateDir := StateRoot + "/" + jobId

	recover := false
	if _, err := os.Stat(stateDir); err == nil {
		recover = true
	} else if !os.IsNotExist(err) {
		log.Errorf("Failed to stat state dir %s: %v", stateDir, err)
		return nil, nil
	}

	stateLog, err := pers.NewStateLog(stateDir)
	if err != nil {
		log.Errorf("Failed to create state log for job %s: %v", jobId, err)
		return nil, nil
	}

	// create an empty FilterState and a StateManager, then let the StateManager
	// perform the restore (it will read the latest snapshot + WAL and apply ops)
	fs := NewFilterState()
	sm, err := pers.NewStateManager(fs, stateLog, 100)
	if err != nil {
		log.Errorf("Failed to create state manager for job %s: %v", jobId, err)
		_ = stateLog.Close()
		return nil, nil
	}

	if !recover {
		return sm, fs
	}

	if err := sm.Restore(); err != nil {
		log.Errorf("Failed to restore state for job %s: %v", jobId, err)
		_ = sm.Close()
		return nil, nil
	}
	return sm, fs
}

func (f *FilterWorker) seenBitmap() *bitmap.Bitmap {
	fs := f.filterState()
	if fs.SeenBatches == nil {
		fs.SeenBatches = bitmap.New()
	}
	return fs.SeenBatches
}

func (f *FilterWorker) totallyFilteredBitmap() *bitmap.Bitmap {
	fs := f.filterState()
	if fs.TotallyFiltered == nil {
		fs.TotallyFiltered = bitmap.New()
	}
	return fs.TotallyFiltered
}

func (f *FilterWorker) handleSequenceSet(p *ic.SequenceSetPayload) {
	if p == nil || p.Sequences == nil || p.Sequences.Bitmap == nil {
		return
	}
	vals := p.Sequences.Bitmap.ToArray()
	if len(vals) == 0 {
		return
	}
	op := NewAddBitmapOp(0, targetTotallyFiltered, vals)
	if err := f.stateManager.Log(op); err != nil {
		log.Errorf("Failed to persist merged totallyFiltered sequences: %v", err)
	}
}

func (f *FilterWorker) persistSeen(seq uint64) {
	op := NewAddBitmapOp(seq, targetSeenBatches, []uint64{seq})
	if err := f.stateManager.Log(op); err != nil {
		log.Errorf("Failed to persist seen batch op: %v", err)
	}
}

func (f *FilterWorker) persistTotallyFiltered(seq uint64) {
	op := NewAddBitmapOp(seq, targetTotallyFiltered, []uint64{seq})
	if err := f.stateManager.Log(op); err != nil {
		log.Errorf("Failed to persist totally filtered op: %v", err)
	}
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
	dupEOFCount := 0
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
			f.handleSequenceSet(p)
			done <- nil

		case *ic.EndSignalPayload:
			shouldAck := f.handleEndSignal(p)
			if shouldAck {
				done <- nil
			} else {
				dupEOFCount++
				if dupEOFCount%500 == 0 {
					log.Warningf("Received %d Duplicated End Signal", dupEOFCount)
				}
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
	if f.seenBitmap().Contains(p.SeqNum) {
		log.Debugf("Batch %d already processed, skipping.", p.SeqNum)
		return
	}

	if len(p.Rows) == 0 {
		f.persistTotallyFiltered(p.SeqNum)
		return
	}

	filteredBatch, err := filterFunction(p)
	if err != nil {
		log.Warningf("Error filtering batch %d: %v", p.SeqNum, err)
		f.persistTotallyFiltered(p.SeqNum)
		return
	}
	filteredBatchPayload := filteredBatch.Payload.(*ic.RowsBatchPayload)
	// log.Debugf("Filter %s processed batch: %d input rows -> %d output rows", f.filterId, len(p.Rows), len(filteredBatchPayload.Rows))

	if len(filteredBatchPayload.Rows) > 0 {
		batchBytes, err := filteredBatch.Marshal()
		if err != nil {
			log.Errorf("Failed to marshal batch: %v", err)
		}
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to send message: %v", err)
		}
		f.persistSeen(p.SeqNum)

	} else {
		f.persistTotallyFiltered(p.SeqNum)
	}
}

func (f *FilterWorker) Start() {
	// blocks until all workers are done
	log.Infof("FilterWorker starting message consumption for job %s", f.jobId)
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

		// close state manager
		if err := f.stateManager.Close(); err != nil {
			log.Errorf("Failed to close state manager: %v", err)
		}

		// remove state directory
		stateDir := StateRoot + "/" + f.jobId
		if err := os.RemoveAll(stateDir); err != nil {
			log.Errorf("Failed to remove state directory %s: %v", stateDir, err)
		} else {
			log.Debugf("Removed state directory %s", stateDir)
		}

		f.removeFromMap <- f.jobId

		log.Info("FilterWorker shutdown completed.")
	})
}

func (f *FilterWorker) handleEndSignal(payload *ic.EndSignalPayload) bool {
	if slices.Contains(payload.WorkersDone, f.filterId) {
		return false
	}

	log.Debugf("Worker received end signal. Task ended.")

	if f.totallyFilteredBitmap().GetCardinality() > 0 {
		totallyFilteredMsg := ic.NewSequenceSet(f.filterId, f.totallyFilteredBitmap())
		batchBytes, _ := totallyFilteredMsg.Marshal()
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to send message: %v", err)
		}
	}

	payload.AddWorkerDone(f.filterId)

	if len(payload.WorkersDone) == f.workersCount {
		log.Info("All workers done. Sending end signal to next stage and deleting input.")
		endSignal := ic.NewEndSignal(nil, payload.SeqNum)
		batchBytes, err := endSignal.Marshal()
		if err != nil {
			log.Errorf("Failed to marshal end signal message: %v", err)
		}
		if err := f.output.Send(batchBytes); err != nil {
			log.Errorf("Failed to propagate end signal message: %v", err)
		}
		f.input.Delete()
		log.Debug("Input deleted.")
		return true
	}

	endBatch := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := endBatch.Marshal()
	f.input.Send(endSignal)
	return true
}
