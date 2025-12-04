package common

import (
	pers "aggregator/common/persistence"
	"aggregator/common/rollback"
	"slices"
	"time"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (aw *AggregatorWorker) aggregatorsDone() []string {
	persState, _ := aw.state.GetState().(*pers.PersistentState)
	return persState.AggregatorsDone
}

func (aw *AggregatorWorker) receivedAggregatorBatchesFor(aggregatorID string) *bitmap.Bitmap {
	persState, _ := aw.state.GetState().(*pers.PersistentState)
	return persState.ReceivedAggregatorBatchesFor(aggregatorID)
}

func (aw *AggregatorWorker) reducerMessageCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		// log.Debugf("Worker %s received message: %s", aw.Config.WorkerId, string(consumeChannel.Body))
		jsonStr := string(consumeChannel.Body)
		var receivedMessage ic.Message
		if err := receivedMessage.Unmarshal([]byte(jsonStr)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		switch p := receivedMessage.Payload.(type) {
		case *ic.AggregatedDataPayload:
			aw.reduceAggregatedData(p)
			done <- nil

		case *ic.SequenceSetPayload:
			log.Debugf("Reducer %s received sequence set from worker %s", aw.Config.WorkerId, p.WorkerID)
			// an aggregator is done sending its data
			// and sent the set of processed batches
			aw.updateProcessedSeqHandlingDuplicates(p)

			log.Debugf("Reducer %s has %d/%d aggregators done", aw.Config.WorkerId, len(aw.aggregatorsDone()), aw.Config.WorkersCount)
			if len(aw.aggregatorsDone()) == aw.Config.WorkersCount {
				aw.rollbackDuplicatedData()
				log.Debugf("All aggregators data has been successfully reduced, sending processed batches")
				aw.reducerPassToNextStage()
			}

			done <- nil

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
			done <- nil
		}
	}
}

func (aw *AggregatorWorker) reducerPassToNextStage() {
	retainedData := aw.dataRetainer.RetainData(
		aw.Config.GroupBy,
		aw.Config.Aggregations,
		aw.aggregatedData(),
	)
	nextBatchToSend := aw.sendRetainedData(retainedData)

	// to be consistent with other workers, remove all processed batches up to nextSeq
	// and notify those batches have been processed
	aw.processedBatches().RemoveRange(0, nextBatchToSend) // medio feo pero sirve
	log.Debug("Sending processed batches")
	aw.sendProcessedBatches()
	eofMsg, _ := ic.NewEndSignal([]string{}, nextBatchToSend).Marshal()
	if err := aw.output.Send(eofMsg); err != nil {
		log.Errorf("Failed to send end signal message: %v", err)
	}
	log.Debug("Deleting input")
	aw.deleteInputForAggregators()
	aw.input.Delete()
	close(aw.closeChan)
}

func (aw *AggregatorWorker) deleteInputForAggregators() {
	deleteInputQueue, err := mw.ResumeConsumer(
		aw.Config.QueryName+"-aggregator-input",
		"",
		aw.Config.MiddlewareAddress,
		aw.jobID,
	)
	if err != nil {
		log.Errorf("[%s] Failed to create delete input queue consumer: %v", aw.jobID, err)
	}
	deleteInputQueue.Delete()
	deleteInputQueue.Close()
}

func (aw *AggregatorWorker) updateProcessedSeqHandlingDuplicates(p *ic.SequenceSetPayload) {
	if slices.Contains(aw.aggregatorsDone(), p.WorkerID) {
		log.Warningf(
			"%s received duplicated sequence set from worker %s in job %s",
			aw.Config.WorkerId, p.WorkerID, aw.jobID,
		)
		return
	}

	intersection := bitmap.And(aw.processedBatches(), p.Sequences.Bitmap)
	if intersection.GetCardinality() != 0 {
		log.Warningf(
			"%s received duplicated data for job %s! Received %d duplicated sequence numbers.",
			aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(),
		)
	}
	var seq uint64
	if p.Sequences.Bitmap.GetCardinality() > 0 {
		seq = p.Sequences.Bitmap.Maximum()
	} else {
		seq = 0
	}
	op := pers.NewAddAggregatorDoneOp(
		seq,
		p.WorkerID,
		p.Sequences.Bitmap,
	)
	if err := aw.state.Log(op); err != nil {
		log.Errorf("Failed to log AddAggregatorDoneOp for worker %s in job %s: %v", p.WorkerID, aw.jobID, err)
		return
	}
}

func (aw *AggregatorWorker) reduceAggregatedData(p *ic.AggregatedDataPayload) {
	// check for duplicated batch from same worker
	if slices.Contains(aw.aggregatorsDone(), p.WorkerID) {
		return
	}
	if aw.receivedAggregatorBatchesFor(p.WorkerID).Contains(p.SeqNum) {
		log.Warningf(
			"%s received duplicated aggregated data from worker %s for job %s! Sequence number: %d. Ignoring batch.",
			aw.Config.WorkerId, p.WorkerID, aw.jobID, p.SeqNum,
		)
		return
	}

	batch := ic.NewRowsBatch(p.ColumnNames, p.Rows, p.SeqNum)
	aw.aggregateBatch(batch.Payload.(*ic.RowsBatchPayload), p.WorkerID)
}

func (aw *AggregatorWorker) rollbackDuplicatedData() {
	if !aw.areThereDuplicatedBatches() {
		// no duplicated data to rollback
		rollback.SendAllOkToEveryAggregator(
			aw.Config.MiddlewareAddress,
			aw.jobID,
			aw.aggregatorsDone(),
		)
		return
	}

	doneRecovering := make(chan struct{})
	go aw.receiveRecoveryResponses(doneRecovering)
	aw.sendRecoveryRequests()

	for {
		select {
		case <-doneRecovering:
			log.Infof("[%s] All duplicated batches recovered", aw.jobID)
			return
		case <-time.After(2 * time.Second):
			aw.showRemainingDuplicatedBatches()
			aw.sendRecoveryRequests()
		}
	}
}

func (aw *AggregatorWorker) sendRecoveryRequests() {
	for aggregatorID, duplicatedBatches := range aw.state.GetState().(*pers.PersistentState).DuplicatedBatches {
		if duplicatedBatches.GetCardinality() > 0 {
			log.Debugf("[%s] Rolling back %d duplicated batches from aggregator %s", aw.jobID, duplicatedBatches.GetCardinality(), aggregatorID)
			rollback.AskForLogsFromDuplicatedBatches(
				aggregatorID,
				aw.Config.MiddlewareAddress,
				aw.jobID,
				duplicatedBatches,
			)
		} else {
			log.Debugf("[%s] No duplicated batches to rollback from aggregator %s", aw.jobID, aggregatorID)
			rollback.SendAllOkToWorker(
				aggregatorID,
				aw.Config.MiddlewareAddress,
				aw.jobID,
			)
		}
	}
}

func (aw *AggregatorWorker) duplicatedBatchesFor(aggregatorID string) *bitmap.Bitmap {
	persState, _ := aw.state.GetState().(*pers.PersistentState)
	return persState.DuplicatedBatchesFor(aggregatorID)
}

func (aw *AggregatorWorker) receiveRecoveryResponses(doneRecovering chan struct{}) {
	recoveryResponses, err := mw.NewConsumer(
		aw.Config.WorkerId,
		rollback.RecoveryResponsesSourceName(aw.Config.WorkerId),
		aw.Config.MiddlewareAddress,
		aw.jobID,
	)
	if err != nil {
		log.Errorf("Failed to create recovery queue consumer for job %s: %v.\nDuplicated batches cannot be rolled back", aw.jobID, err)
		close(doneRecovering)
		return
	}
	log.Debugf("[%s] Waiting for recovery responses for job...", aw.jobID)
	callback := aw.recoveryResponsesCallback(doneRecovering)
	go recoveryResponses.StartConsuming(callback)
	<-doneRecovering
	recoveryResponses.Delete()
	recoveryResponses.Close()
}

func (aw *AggregatorWorker) areThereDuplicatedBatches() bool {
	for _, duplicatedBatches := range aw.state.GetState().(*pers.PersistentState).DuplicatedBatches {
		if duplicatedBatches.GetCardinality() > 0 {
			return true
		}
	}
	return false
}

func (aw *AggregatorWorker) recoveryResponsesCallback(doneRecovering chan struct{}) mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		defer func() { done <- nil }()
		// log.Debugf("Worker %s received message: %s", aw.Config.WorkerId, string(consumeChannel.Body))
		jsonStr := string(consumeChannel.Body)
		var receivedMessage rollback.Message
		if err := receivedMessage.Unmarshal([]byte(jsonStr)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch p := receivedMessage.Payload.(type) {
		case *rollback.RecoveryResponsePayload:
			if !aw.duplicatedBatchesFor(p.WorkerID).Contains(p.SequenceNumber) {
				// duplicated recovery response
				log.Debugf("[%s] Received duplicated recovery response for batch %d from worker %s", aw.jobID, p.SequenceNumber, p.WorkerID)
				return
			}

			log.Debugf("[%s] Reverting duplicated batch %d from worker %s", aw.jobID, p.SequenceNumber, p.WorkerID)
			operation := pers.NewRevertBatchAggregationOp(p.SequenceNumber, p.WorkerID, p.OperationData)
			if err := aw.state.Log(operation); err != nil {
				log.Errorf("Failed to log recovered batch %d from worker %s for job %s: %v", p.SequenceNumber, p.WorkerID, aw.jobID, err)
			}

			if aw.duplicatedBatchesFor(p.WorkerID).GetCardinality() == 0 {
				log.Infof("[%s] All duplicated batches recovered from worker %s", aw.jobID, p.WorkerID)
				rollback.SendAllOkToWorker(
					p.WorkerID,
					aw.Config.MiddlewareAddress,
					aw.jobID,
				)
				aw.showRemainingDuplicatedBatches()
			}

			if !aw.areThereDuplicatedBatches() {
				log.Infof("[%s] All recovery responses received for job", aw.jobID)
				// just in case, double check
				rollback.SendAllOkToEveryAggregator(
					aw.Config.MiddlewareAddress,
					aw.jobID,
					aw.aggregatorsDone(),
				)
				close(doneRecovering)
			}

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
		}
	}
}

func (aw *AggregatorWorker) showRemainingDuplicatedBatches() {
	for _, aggregatorID := range aw.aggregatorsDone() {
		duplicatedBatches := aw.duplicatedBatchesFor(aggregatorID)
		if duplicatedBatches.GetCardinality() > 0 {
			log.Debugf(
				"[%s] Still %d duplicated batches remaining from worker %s: %v",
				aw.jobID,
				duplicatedBatches.GetCardinality(),
				aggregatorID,
				duplicatedBatches.ToArray(),
			)
		}
	}
}
