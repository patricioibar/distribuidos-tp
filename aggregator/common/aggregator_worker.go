package common

import (
	"slices"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (aw *AggregatorWorker) aggregatorMessageCallback() mw.OnMessageCallback {
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
		case *ic.RowsBatchPayload:
			aw.aggregateNewRowsBatch(p, done)

			done <- nil

		case *ic.SequenceSetPayload:
			intersection := roaring.And(aw.processedBatches, p.Sequences.Bitmap)
			if intersection.GetCardinality() != 0 {
				log.Warningf(
					"%s received duplicated data for job %s! Received %d duplicated sequence numbers: %s",
					aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(), intersection.String(),
				)
			}
			// these are batches without data, just marking as processed
			aw.processedBatches.Or(p.Sequences.Bitmap)
			done <- nil

		case *ic.EndSignalPayload:
			aw.aggregatorPassToNextStage(p)
			done <- nil
			close(aw.closeChan)

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
			done <- nil
		}
	}
}

func (aw *AggregatorWorker) aggregatorPassToNextStage(p *ic.EndSignalPayload) {
	retainedData := aw.dataRetainer.RetainData(
		aw.Config.GroupBy,
		aw.Config.Aggregations,
		aw.reducedData,
	)
	aw.sendRetainedData(retainedData)
	aw.sendProcessedBatches()
	aw.PropagateEndSignal(p)
}

func (aw *AggregatorWorker) aggregateNewRowsBatch(p *ic.RowsBatchPayload, done chan *mw.MessageMiddlewareError) {
	if aw.processedBatches.Contains(p.SeqNum) {
		log.Warningf(
			"%s received duplicated data for job %s! Sequence number: %d. Ignoring batch.",
			aw.Config.WorkerId, aw.jobID, p.SeqNum,
		)
		done <- nil
		return
	}

	if len(p.Rows) != 0 {
		aw.aggregateBatch(p)
	}
	aw.processedBatches.Add(p.SeqNum)
}

func (aw *AggregatorWorker) PropagateEndSignal(payload *ic.EndSignalPayload) {
	if slices.Contains(payload.WorkersDone, aw.Config.WorkerId) {
		// This worker has already sent its end signal (duplicated message, ignore)
		return
	}
	log.Debugf("Worker %s done, propagating end signal", aw.Config.WorkerId)

	payload.AddWorkerDone(aw.Config.WorkerId)

	if len(payload.WorkersDone) == aw.Config.WorkersCount {
		log.Debugf("All workers done")
		// endSignal, _ := ic.NewEndSignal(nil, payload.SeqNum).Marshal()
		// aw.output.Send(endSignal)
		return
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
}
