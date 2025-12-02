package common

import (
	"os"
	"sync"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

const StateRoot = ".state"

type JoinerWorker struct {
	Config        *Config
	leftInput     mw.MessageMiddleware
	rightInput    mw.MessageMiddleware
	output        mw.MessageMiddleware
	jobID         string
	state         *pers.StateManager
	removeFromMap chan string
	closeOnce     sync.Once
}

func NewJoinerWorker(
	config *Config,
	leftInput mw.MessageMiddleware,
	rightInput mw.MessageMiddleware,
	output mw.MessageMiddleware,
	jobID string,
	removeFromMap chan string,
) *JoinerWorker {
	sm := initStateManager(jobID)
	if sm == nil {
		log.Errorf("StateManager not initialized for job %s", jobID)
		return nil
	}
	return &JoinerWorker{
		Config:        config,
		leftInput:     leftInput,
		rightInput:    rightInput,
		output:        output,
		jobID:         jobID,
		removeFromMap: removeFromMap,
		state:         sm,
	}
}

func initStateManager(jobId string) *pers.StateManager {
	stateDir := StateRoot + "/" + jobId

	recover := false
	if _, err := os.Stat(stateDir); err == nil {
		recover = true
	} else if !os.IsNotExist(err) {
		log.Errorf("Failed to stat state dir %s: %v", stateDir, err)
		return nil
	}

	stateLog, err := pers.NewStateLog(stateDir)
	if err != nil {
		log.Errorf("Failed to create state log for job %s: %v", jobId, err)
		return nil
	}

	fs := NewJoinerState()
	sm, err := pers.NewStateManager(fs, stateLog, 100)
	if err != nil {
		log.Errorf("Failed to create state manager for job %s: %v", jobId, err)
		_ = stateLog.Close()
		return nil
	}

	if !recover {
		return sm
	}

	if err := sm.Restore(); err != nil {
		log.Errorf("Failed to restore state for job %s: %v", jobId, err)
		_ = sm.Close()
		return nil
	}
	return sm
}

func (jw *JoinerWorker) filterState() *JoinerState {
	state, ok := jw.state.GetState().(*JoinerState)
	if ok {
		return state
	}
	return nil
}

func (jw *JoinerWorker) rightSeqRecv() *bitmap.Bitmap {
	js := jw.filterState()
	if js.RightSeqRecv == nil {
		js.RightSeqRecv = bitmap.New()
	}
	return js.RightSeqRecv
}

func (jw *JoinerWorker) leftSeqRecv() *bitmap.Bitmap {
	js := jw.filterState()
	if js.LeftSeqRecv == nil {
		js.LeftSeqRecv = bitmap.New()
	}
	return js.LeftSeqRecv
}

func (jw *JoinerWorker) rightCache() *TableCache {
	js := jw.filterState()
	return js.RightCache
}

func (jw *JoinerWorker) persistRightSeqNum(seqNum uint64) {
	op := NewAddBitmapOp(seqNum, targetRightSeqRecv, []uint64{seqNum})
	if err := jw.state.Log(op); err != nil {
		log.Errorf("Failed to persist right seqNum %d: %v", seqNum, err)
	}
}

func (jw *JoinerWorker) persistLeftSeqNum(seqNum uint64) {
	op := NewAddBitmapOp(seqNum, targetLeftSeqRecv, []uint64{seqNum})
	if err := jw.state.Log(op); err != nil {
		log.Errorf("Failed to persist left seqNum %d: %v", seqNum, err)
	}
}

func (jw *JoinerWorker) persistRightCache(seqNum uint64, columns []string, rows [][]interface{}) {
	op := NewAddRightCacheOp(seqNum, columns, rows)
	if err := jw.state.Log(op); err != nil {
		log.Errorf("Failed to persist right cache: %v", err)
	}
}

func (jw *JoinerWorker) Start() {
	jw.innerStart()
	jw.Close()
}

func (jw *JoinerWorker) Close() {
	jw.closeOnce.Do(func() {
		jw.leftInput.Close()
		jw.rightInput.Close()
		jw.output.Close()
		jw.state.Close()

		// remove state directory
		stateDir := StateRoot + "/" + jw.jobID
		if err := os.RemoveAll(stateDir); err != nil {
			log.Errorf("Failed to remove state directory %s: %v", stateDir, err)
		} else {
			log.Debugf("Removed state directory %s", stateDir)
		}

		if jw.removeFromMap != nil {
			jw.removeFromMap <- jw.jobID
		}
		log.Debugf("Worker %s closed", jw.Config.WorkerId)
	})
}

func (jw *JoinerWorker) innerStart() {
	// blocks until right input queue is closed
	if err := jw.rightInput.StartConsuming(jw.rightCallback()); err != nil {
		log.Errorf("Failed to start consuming right input messages for job %s: %v", jw.jobID, err)
		return
	}
	log.Debugf("%s received all right input, starting left input", jw.Config.WorkerId)
	// log.Debugf("Right cache: %v", jw.rightCache)
	// blocks until left input queue is closed
	if err := jw.leftInput.StartConsuming(jw.leftCallback()); err != nil {
		log.Errorf("Failed to start consuming left input messages for job %s: %v", jw.jobID, err)
	}
}

func (jw *JoinerWorker) rightCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		// Ensure we always notify the caller exactly once when the callback ends.
		defer func() { done <- nil }()
		jsonStr := string(consumeChannel.Body)
		var receivedMsg ic.Message
		if err := receivedMsg.Unmarshal([]byte(jsonStr)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch p := receivedMsg.Payload.(type) {
		case *ic.RowsBatchPayload:
			jw.addNewRightBatch(p)

		case *ic.EndSignalPayload:
			jw.rightInput.Delete()

		case *ic.SequenceSetPayload:
			jw.handleRightSequenceSet(p)

		default:
			log.Warningf("Unexpected payload type in right input: %T", p)
		}

		// done is sent by the deferred function above
	}
}

func (jw *JoinerWorker) handleRightSequenceSet(p *ic.SequenceSetPayload) {
	if p == nil || p.Sequences == nil || p.Sequences.Bitmap == nil {
		return
	}
	vals := p.Sequences.Bitmap.ToArray()
	if len(vals) == 0 {
		return
	}
	op := NewAddBitmapOp(0, targetRightSeqRecv, vals)
	if err := jw.state.Log(op); err != nil {
		log.Errorf("Failed to persist merged right sequences: %v", err)
	}
}

func (jw *JoinerWorker) handleLeftSequenceSet(p *ic.SequenceSetPayload) {
	if p == nil || p.Sequences == nil || p.Sequences.Bitmap == nil {
		return
	}
	vals := p.Sequences.Bitmap.ToArray()
	if len(vals) == 0 {
		return
	}
	op := NewAddBitmapOp(0, targetLeftSeqRecv, vals)
	if err := jw.state.Log(op); err != nil {
		log.Errorf("Failed to persist merged left sequences: %v", err)
	}
}

func (jw *JoinerWorker) addNewRightBatch(p *ic.RowsBatchPayload) {
	if jw.rightCache() == nil {
		jw.persistRightCache(p.SeqNum, p.ColumnNames, nil)
	}

	if jw.rightSeqRecv().Contains(p.SeqNum) {
		log.Debugf("Duplicate right batch with SeqNum %d received, ignoring", p.SeqNum)
		return
	}
	jw.persistRightSeqNum(p.SeqNum)

	if jw.rightCache() != nil && len(p.Rows) != 0 {
		jw.persistRightCache(p.SeqNum, nil, p.Rows)
	}
}

func (jw *JoinerWorker) leftCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		// Ensure we always notify the caller exactly once when the callback ends.
		defer func() { done <- nil }()
		jsonStr := string(consumeChannel.Body)
		var receivedMsg ic.Message
		if err := receivedMsg.Unmarshal([]byte(jsonStr)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch p := receivedMsg.Payload.(type) {
		case *ic.RowsBatchPayload:
			jw.processLeftRowsBatch(p)

		case *ic.EndSignalPayload:
			jw.sendJoinedSequenceSet()
			jw.propagateLeftEndSignal(p)

		case *ic.SequenceSetPayload:
			jw.handleLeftSequenceSet(p)

		default:
			log.Warningf("Unexpected payload type in right input: %T", p)
		}

		// done is sent by the deferred function above
	}
}

func (jw *JoinerWorker) processLeftRowsBatch(p *ic.RowsBatchPayload) {
	if jw.leftSeqRecv().Contains(p.SeqNum) {
		log.Debugf("Duplicate left batch with SeqNum %d received, ignoring", p.SeqNum)
		return
	}

	if len(p.Rows) == 0 {
		jw.persistLeftSeqNum(p.SeqNum)
		return
	}

	joinedBatch := jw.joinBatch(p)
	if len(joinedBatch) == 0 {
		jw.persistLeftSeqNum(p.SeqNum)
		return
	}

	jw.sendJoinedBatch(joinedBatch, p.SeqNum)
}

func (jw *JoinerWorker) sendJoinedSequenceSet() {
	if jw.leftSeqRecv().GetCardinality() == 0 {
		return
	}
	msg := ic.NewSequenceSet(jw.Config.WorkerId, jw.leftSeqRecv())
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal sequence set: %v", err)
		return
	}
	log.Debugf("Sending sequence set, bytes: %d", len(data))
	if err := jw.output.Send(data); err != nil {
		log.Errorf("Failed to send sequence set: %v", err)
	}
}

func (jw *JoinerWorker) propagateLeftEndSignal(payload *ic.EndSignalPayload) {
	log.Debugf("Worker %s done, propagating end signal", jw.Config.WorkerId)

	payload.AddWorkerDone(jw.Config.WorkerId)

	if len(payload.WorkersDone) == jw.Config.WorkersCount {
		log.Debugf("All workers done")
		endSignal, _ := ic.NewEndSignal(nil, payload.SeqNum).Marshal()
		jw.output.Send(endSignal)
		jw.leftInput.Delete()
		return
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	jw.leftInput.Send(endSignal) // re-enqueue the end signal with updated workers done
}

func (jw *JoinerWorker) joinBatch(batch *ic.RowsBatchPayload) [][]interface{} {
	if jw.rightCache() == nil {
		log.Errorf("Right cache is nil, cannot join")
		return nil
	}

	joinKeyIndexLeft := findColumnIndex(jw.Config.JoinKey, batch.ColumnNames)
	joinKeyIndexRight := findColumnIndex(jw.Config.JoinKey, jw.rightCache().Columns)

	if joinKeyIndexLeft == -1 || joinKeyIndexRight == -1 {
		log.Errorf("Join key (%s) not found in columns", jw.Config.JoinKey)
		log.Errorf("Left columns: %v", batch.ColumnNames)
		log.Errorf("Right columns: %v", jw.rightCache().Columns)
		return nil
	}

	joinedRows := make([][]interface{}, 0)

	for _, leftRow := range batch.Rows {
		if leftRow == nil || len(leftRow) <= joinKeyIndexLeft {
			continue
		}
		leftKey := leftRow[joinKeyIndexLeft]

		for _, rightRow := range jw.rightCache().Rows {
			if rightRow == nil || len(rightRow) <= joinKeyIndexRight {
				log.Errorf("Invalid right row: %v", rightRow)
				continue
			}
			rightKey := rightRow[joinKeyIndexRight]

			if keyMatches(leftKey, rightKey) {
				jw.addJoinedRow(&joinedRows, batch.ColumnNames, leftRow, rightRow)
			}
		}
	}
	return joinedRows
}

func keyMatches(leftKey interface{}, rightKey interface{}) bool {
	// Try to compare as int
	leftInt, leftIntOk := toInt(leftKey)
	rightInt, rightIntOk := toInt(rightKey)
	if leftIntOk && rightIntOk {
		return leftInt == rightInt
	}

	// Try to compare as float
	leftFloat, leftFloatOk := toFloat(leftKey)
	rightFloat, rightFloatOk := toFloat(rightKey)
	if leftFloatOk && rightFloatOk {
		return leftFloat == rightFloat
	}

	// Fallback to string comparison
	return toString(leftKey) == toString(rightKey)
}

func (jw *JoinerWorker) addJoinedRow(joinedRows *[][]interface{}, columnNames []string, leftRow []interface{}, rightRow []interface{}) {
	joinedRow := make([]interface{}, len(jw.Config.OutputColumns))
	for i, col := range jw.Config.OutputColumns {
		leftColIndex := findColumnIndex(col, columnNames)
		if leftColIndex != -1 {
			joinedRow[i] = leftRow[leftColIndex]
			continue
		}
		rightColIndex := findColumnIndex(col, jw.rightCache().Columns)
		if rightColIndex != -1 {
			joinedRow[i] = rightRow[rightColIndex]
		} else {
			joinedRow[i] = nil
		}
	}
	*joinedRows = append(*joinedRows, joinedRow)
}

func (jw *JoinerWorker) sendJoinedBatch(joinedBatch [][]interface{}, seqNum uint64) {
	batch := ic.NewRowsBatch(jw.Config.OutputColumns, joinedBatch, seqNum)
	batchBytes, err := batch.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal joined batch: %v", err)
		return
	}
	log.Debugf("Sending joined batch, rows: %d, bytes: %d", len(joinedBatch), len(batchBytes))
	if err := jw.output.Send(batchBytes); err != nil {
		log.Errorf("Failed to send joined batch: %v", err)
	}
}
