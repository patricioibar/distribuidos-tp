package common

import (
	pers "aggregator/common/persistence"
	"aggregator/common/rollback"
	"slices"

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
			if p.SeqNum%50 == 0 {
				log.Debugf("Reducer %s processed aggregated data batch with seq %d", aw.Config.WorkerId, p.SeqNum)
			}
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
	aw.input.Delete()
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
			"%s received duplicated data for job %s! Received %d duplicated sequence numbers: %s",
			aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(), intersection.String(),
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
	recoveryQueueDeclared := false
	var duplicatedMessages uint64 = 0
	var recoveryResponses *mw.Consumer
	var err error
	for aggregatorID, duplicatedBatches := range aw.state.GetState().(*pers.PersistentState).DuplicatedBatches {
		if duplicatedBatches.GetCardinality() > 0 {
			log.Infof("Rolling back %d duplicated batches from aggregator %s for job %s", duplicatedBatches.GetCardinality(), aggregatorID, aw.jobID)
			if !recoveryQueueDeclared {
				recoveryResponses, err = mw.NewConsumer(
					aw.Config.WorkerId,
					rollback.RecoveryResponsesSourceName(aw.Config.WorkerId),
					aw.Config.MiddlewareAddress,
					aw.jobID,
				)
				if err != nil {
					log.Errorf("Failed to create recovery queue consumer for job %s: %v.\nDuplicated batches cannot be rolled back", aw.jobID, err)
					return
				}
				recoveryQueueDeclared = true
			}
			duplicatedMessages += duplicatedBatches.GetCardinality()
			rollback.AskForLogsFromDuplicatedBatches(
				aw.Config.WorkerId,
				aw.Config.MiddlewareAddress,
				aw.jobID,
				duplicatedBatches,
			)
		}
	}
	if !recoveryQueueDeclared {
		// no duplicated data to rollback
		rollback.SendAllOkToEveryAggregator(
			aw.Config.WorkerId,
			aw.Config.MiddlewareAddress,
			aw.jobID,
			aw.aggregatorsDone(),
		)
		return
	}
	log.Infof("Waiting for recovery responses for job %s", aw.jobID)
	aw.receiveRecoveryResponses(recoveryResponses, duplicatedMessages)
}

func (aw *AggregatorWorker) duplicatedBatchesFor(aggregatorID string) *bitmap.Bitmap {
	persState, _ := aw.state.GetState().(*pers.PersistentState)
	return persState.DuplicatedBatchesFor(aggregatorID)
}

func (aw *AggregatorWorker) receiveRecoveryResponses(recoveryResponses *mw.Consumer, duplicatedMessages uint64) {
	callback := func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
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
				return
			}

			operation := pers.NewReceiveAggregatorBatchOp(p.SequenceNumber, p.WorkerID, p.OperationData)
			if err := aw.state.Log(operation); err != nil {
				log.Errorf("Failed to log recovered batch %d from worker %s for job %s: %v", p.SequenceNumber, p.WorkerID, aw.jobID, err)
			}

			if aw.duplicatedBatchesFor(p.WorkerID).GetCardinality() == 0 {
				log.Infof("All duplicated batches recovered from worker %s for job %s", p.WorkerID, aw.jobID)
				rollback.SendAllOkToWorker(
					aw.Config.WorkerId,
					aw.Config.MiddlewareAddress,
					aw.jobID,
					p.WorkerID,
				)
				return
			}

			duplicatedMessages--
			if duplicatedMessages == 0 {
				log.Infof("All recovery responses received for job %s", aw.jobID)
				rollback.SendAllOkToEveryAggregator(
					aw.Config.WorkerId,
					aw.Config.MiddlewareAddress,
					aw.jobID,
					aw.aggregatorsDone(),
				)
				recoveryResponses.Delete()
			}

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
		}
	}

	// blocks until queue is deleted
	recoveryResponses.StartConsuming(callback)
	recoveryResponses.Close()
}
