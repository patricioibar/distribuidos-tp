package common

import (
	pers "aggregator/common/persistence"
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
				log.Debugf("All aggregators have sent their sequence sets, sending processed batches")
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
	rollbackNeeded := false
	for aggregatorID, duplicatedBatches := range aw.state.GetState().(*pers.PersistentState).DuplicatedBatches {
		if duplicatedBatches.GetCardinality() > 0 {
			log.Infof("Rolling back %d duplicated batches from aggregator %s", duplicatedBatches.GetCardinality(), aggregatorID)
			rollbackNeeded = true
		}
	}
	if rollbackNeeded {
		panic("TO DO: implement rollback for duplicated data in reducer")
	}
}
