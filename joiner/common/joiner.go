package common

import (
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

type JoinerWorker struct {
	Config     *Config
	leftInput  mw.MessageMiddleware
	rightInput mw.MessageMiddleware
	output     mw.MessageMiddleware
	rightCache *TableCache
	joinedRows [][]interface{}
	rightDone  chan struct{}
	closeChan  chan struct{}
}

func NewJoinerWorker(
	config *Config,
	leftInput mw.MessageMiddleware,
	rightInput mw.MessageMiddleware,
	output mw.MessageMiddleware,
) *JoinerWorker {
	return &JoinerWorker{
		Config:     config,
		leftInput:  leftInput,
		rightInput: rightInput,
		output:     output,
		closeChan:  make(chan struct{}),
		rightDone:  make(chan struct{}),
		rightCache: nil,
		joinedRows: make([][]interface{}, 0),
	}
}

func (jw *JoinerWorker) Start() {
	go func() {
		jw.innerStart()
	}()

	<-jw.closeChan
}

func (jw *JoinerWorker) Close() {
	jw.leftInput.Close()
	jw.rightInput.Close()
	jw.output.Close()
	close(jw.closeChan)
	log.Debugf("Worker %s closed", jw.Config.WorkerId)
}

func (jw *JoinerWorker) innerStart() {
	jw.rightInput.StartConsuming(jw.rightCallback())
	<-jw.rightDone
	log.Debugf("%s received all right input, starting left input", jw.Config.WorkerId)
	log.Debugf("Right cache: %v", jw.rightCache)
	jw.leftInput.StartConsuming(jw.leftCallback())
}

func (jw *JoinerWorker) rightCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		defer func() { done <- nil }()

		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		if jw.rightCache == nil {
			jw.rightCache, err = NewTableCache(batch.ColumnNames)
			if err != nil {
				log.Errorf("Failed to create right cache: %v", err)
			}
		}

		if jw.rightCache != nil && len(batch.Rows) != 0 {
			for _, row := range batch.Rows {
				if err := jw.rightCache.AddRow(row); err != nil {
					log.Errorf("Failed to add row to cache: %v", err)
				}
			}
		}

		if batch.IsEndSignal() {
			jw.rightDone <- struct{}{}
		}
	}
}

func (jw *JoinerWorker) leftCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if len(batch.Rows) != 0 {
			jw.joinBatch(batch)
		}

		done <- nil
		if batch.IsEndSignal() {
			jw.sendJoinedResults()
			jw.propagateLeftEndSignal(batch)
			jw.Close()
		}
	}
}

func (jw *JoinerWorker) propagateLeftEndSignal(batch *ic.RowsBatch) {
	log.Debugf("Worker %s done, propagating end signal", jw.Config.WorkerId)

	batch.AddWorkerDone(jw.Config.WorkerId)

	if len(batch.WorkersDone) == jw.Config.WorkersCount {
		log.Debugf("All workers done")
		endSignal, _ := ic.NewEndSignal().Marshal()
		jw.output.Send(endSignal)
		return
	}

	endSignal, _ := batch.Marshal()
	jw.leftInput.Send(endSignal) // re-enqueue the end signal with updated workers done
}

func (jw *JoinerWorker) joinBatch(batch *ic.RowsBatch) {
	if jw.rightCache == nil {
		log.Errorf("Right cache is nil, cannot join")
		return
	}

	joinKeyIndexLeft := findColumnIndex(jw.Config.JoinKey, batch.ColumnNames)
	joinKeyIndexRight := findColumnIndex(jw.Config.JoinKey, jw.rightCache.Columns)

	if joinKeyIndexLeft == -1 || joinKeyIndexRight == -1 {
		log.Errorf("Join key (%s) not found in columns", jw.Config.JoinKey)
		log.Errorf("Left columns: %v", batch.ColumnNames)
		log.Errorf("Right columns: %v", jw.rightCache.Columns)
		return
	}

	for _, leftRow := range batch.Rows {
		if leftRow == nil || len(leftRow) <= joinKeyIndexLeft {
			log.Errorf("Invalid left row: %v", leftRow)
			continue
		}
		leftKey := leftRow[joinKeyIndexLeft]

		for _, rightRow := range jw.rightCache.Rows {
			if rightRow == nil || len(rightRow) <= joinKeyIndexRight {
				log.Errorf("Invalid right row: %v", rightRow)
				continue
			}
			rightKey := rightRow[joinKeyIndexRight]

			if leftKey == rightKey {
				jw.addJoinedRow(batch, leftRow, rightRow)
			}
		}
	}
}

func (jw *JoinerWorker) addJoinedRow(batch *ic.RowsBatch, leftRow []interface{}, rightRow []interface{}) {
	joinedRow := make([]interface{}, len(jw.Config.OutputColumns))
	for i, col := range jw.Config.OutputColumns {
		leftColIndex := findColumnIndex(col, batch.ColumnNames)
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
	jw.joinedRows = append(jw.joinedRows, joinedRow)
}

func (jw *JoinerWorker) sendJoinedResults() {
	if len(jw.joinedRows) == 0 {
		log.Debugf("No joined rows to send")
		return
	}

	batchSize := jw.Config.BatchSize
	currentBatch := &ic.RowsBatch{
		ColumnNames: jw.Config.OutputColumns,
		Rows:        make([][]interface{}, 0),
		WorkersDone: make([]string, 0),
	}

	for i, row := range jw.joinedRows {
		currentBatch.Rows = append(currentBatch.Rows, row)

		if (i+1)%batchSize == 0 {
			jw.sendBatch(currentBatch)
			currentBatch.Rows = make([][]interface{}, 0)
		}
	}

	if len(currentBatch.Rows) > 0 {
		jw.sendBatch(currentBatch)
	}
}

func (jw *JoinerWorker) sendBatch(batch *ic.RowsBatch) {
	log.Debugf("Worker %s sending batch of rows %d", jw.Config.WorkerId, len(batch.Rows))
	data, err := batch.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
		return
	}

	if err := jw.output.Send(data); err != nil {
		log.Errorf("Failed to send batch: %v", err)
	}
}
