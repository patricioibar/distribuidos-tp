package common

import (
	"slices"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

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
			aw.updateProcessedSeqHandlingDuplicates(p)

			if len(aw.aggregatorsDone) == aw.Config.WorkersCount {
				// All aggregators have sent their sequence sets, we can send the processed batches
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
		aw.reducedData,
	)
	aw.sendRetainedData(retainedData)

	// to be consistent with other workers, remove all processed batches up to nextSeq
	// and notify those batches have been processed
	aw.processedBatches.RemoveRange(0, aw.nextBatchToSend)
	aw.sendProcessedBatches()
	eofMsg, _ := ic.NewEndSignal([]string{}, aw.nextBatchToSend).Marshal()
	if err := aw.output.Send(eofMsg); err != nil {
		log.Errorf("Failed to send end signal message: %v", err)
	}
	aw.input.Delete()
}

func (aw *AggregatorWorker) updateProcessedSeqHandlingDuplicates(p *ic.SequenceSetPayload) {
	if slices.Contains(aw.aggregatorsDone, p.WorkerID) {
		log.Warningf(
			"%s received duplicated sequence set from worker %s in job %s",
			aw.Config.WorkerId, p.WorkerID, aw.jobID,
		)
		return
	}

	intersection := bitmap.And(aw.processedBatches, p.Sequences.Bitmap)
	if intersection.GetCardinality() != 0 {
		log.Warningf(
			"%s received duplicated data for job %s! Received %d duplicated sequence numbers: %s",
			aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(), intersection.String(),
		)

		aw.rollbackDataProcess(intersection.ToArray(), p.WorkerID)
	}
	aw.processedBatches.Or(p.Sequences.Bitmap)
	aw.aggregatorsDone = append(aw.aggregatorsDone, p.WorkerID)
}

func (aw *AggregatorWorker) reduceAggregatedData(p *ic.AggregatedDataPayload) {
	// check for duplicated batch from same worker
	if _, ok := aw.rcvedAggData[p.WorkerID]; !ok {
		aw.rcvedAggData[p.WorkerID] = bitmap.New()
	}
	if aw.rcvedAggData[p.WorkerID].Contains(p.SeqNum) {
		log.Warningf(
			"%s received duplicated aggregated data from worker %s for job %s! Sequence number: %d. Ignoring batch.",
			aw.Config.WorkerId, p.WorkerID, aw.jobID, p.SeqNum,
		)
		return
	}
	aw.rcvedAggData[p.WorkerID].Add(p.SeqNum)

	if len(p.Rows) != 0 {
		batch := ic.NewRowsBatch(p.ColumnNames, p.Rows, p.SeqNum)
		aw.aggregateBatch(batch.Payload.(*ic.RowsBatchPayload))
	}
}

func (aw *AggregatorWorker) rollbackDataProcess(seqNums []uint64, workerID string) {
	panic("TO DO: implement rollback for duplicated data in reducer")
}
