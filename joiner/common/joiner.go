package common

import (
	"sync"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

type JoinerWorker struct {
	Config        *Config
	leftInput     mw.MessageMiddleware
	rightInput    mw.MessageMiddleware
	output        mw.MessageMiddleware
	rightCache    *TableCache
	leftSeqRecv   *bitmap.Bitmap
	jobID         string
	removeFromMap chan string
	closeOnce     sync.Once
	rightSeqRecv  *bitmap.Bitmap
}

func NewJoinerWorker(
	config *Config,
	leftInput mw.MessageMiddleware,
	rightInput mw.MessageMiddleware,
	output mw.MessageMiddleware,
	jobID string,
	removeFromMap chan string,
) *JoinerWorker {
	return &JoinerWorker{
		Config:        config,
		leftInput:     leftInput,
		rightInput:    rightInput,
		output:        output,
		rightCache:    nil,
		leftSeqRecv:   bitmap.New(),
		jobID:         jobID,
		removeFromMap: removeFromMap,
		rightSeqRecv:  bitmap.New(),
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
			jw.rightSeqRecv.Or(p.Sequences.Bitmap)

		default:
			log.Warningf("Unexpected payload type in right input: %T", p)
		}

		// done is sent by the deferred function above
	}
}

func (jw *JoinerWorker) addNewRightBatch(p *ic.RowsBatchPayload) {
	var err error
	if jw.rightCache == nil {
		jw.rightCache, err = NewTableCache(p.ColumnNames)
		if err != nil {
			log.Errorf("Failed to create right cache: %v", err)
		}
	}

	if jw.rightSeqRecv.Contains(p.SeqNum) {
		log.Debugf("Duplicate right batch with SeqNum %d received, ignoring", p.SeqNum)
		return
	}
	jw.rightSeqRecv.Add(p.SeqNum)

	if jw.rightCache != nil && len(p.Rows) != 0 {
		for _, row := range p.Rows {
			if err := jw.rightCache.AddRow(row); err != nil {
				log.Errorf("Failed to add row to cache: %v", err)
			}
		}
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
			jw.leftSeqRecv.Or(p.Sequences.Bitmap)

		default:
			log.Warningf("Unexpected payload type in right input: %T", p)
		}

		// done is sent by the deferred function above
	}
}

func (jw *JoinerWorker) processLeftRowsBatch(p *ic.RowsBatchPayload) {
	if jw.leftSeqRecv.Contains(p.SeqNum) {
		log.Debugf("Duplicate left batch with SeqNum %d received, ignoring", p.SeqNum)
		return
	}

	if len(p.Rows) == 0 {
		jw.leftSeqRecv.Add(p.SeqNum)
		return
	}

	joinedBatch := jw.joinBatch(p)
	if len(joinedBatch) == 0 {
		jw.leftSeqRecv.Add(p.SeqNum)
		return
	}

	jw.sendJoinedBatch(joinedBatch, p.SeqNum)
}

func (jw *JoinerWorker) sendJoinedSequenceSet() {
	if jw.leftSeqRecv.GetCardinality() == 0 {
		return
	}
	msg := ic.NewSequenceSet(jw.Config.WorkerId, jw.leftSeqRecv)
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
	if jw.rightCache == nil {
		log.Errorf("Right cache is nil, cannot join")
		return nil
	}

	joinKeyIndexLeft := findColumnIndex(jw.Config.JoinKey, batch.ColumnNames)
	joinKeyIndexRight := findColumnIndex(jw.Config.JoinKey, jw.rightCache.Columns)

	if joinKeyIndexLeft == -1 || joinKeyIndexRight == -1 {
		log.Errorf("Join key (%s) not found in columns", jw.Config.JoinKey)
		log.Errorf("Left columns: %v", batch.ColumnNames)
		log.Errorf("Right columns: %v", jw.rightCache.Columns)
		return nil
	}

	joinedRows := make([][]interface{}, 0)

	for _, leftRow := range batch.Rows {
		if leftRow == nil || len(leftRow) <= joinKeyIndexLeft {
			continue
		}
		leftKey := leftRow[joinKeyIndexLeft]

		for _, rightRow := range jw.rightCache.Rows {
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
		rightColIndex := findColumnIndex(col, jw.rightCache.Columns)
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
