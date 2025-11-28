package common

import (
	"slices"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (aw *AggregatorWorker) aggregatorMessageCallback() mw.OnMessageCallback {
	sendDataOnce := sync.Once{}
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
			aw.aggregateNewRowsBatch(p)
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
			shouldAck := aw.aggregatorPassToNextStage(p, &sendDataOnce)
			if shouldAck {
				done <- nil
			} else {
				// duplicated end signal, requeue
				done <- &mw.MessageMiddlewareError{Code: 0, Msg: "end signal contains this worker already"}
			}

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
			done <- nil
		}
	}
}

func (aw *AggregatorWorker) aggregatorPassToNextStage(p *ic.EndSignalPayload, sendDataOnce *sync.Once) bool {
	if slices.Contains(p.WorkersDone, aw.Config.WorkerId) {
		// This worker has already sent its end signal (duplicated message, ignore)
		return false
	}
	sendDataOnce.Do(func() {
		retainedData := aw.dataRetainer.RetainData(
			aw.Config.GroupBy,
			aw.Config.Aggregations,
			aw.reducedData,
		)
		aw.sendRetainedData(retainedData)
		aw.sendProcessedBatches()
	})
	aw.PropagateEndSignal(p)
	return true
}

func (aw *AggregatorWorker) aggregateNewRowsBatch(p *ic.RowsBatchPayload) {
	if aw.processedBatches.Contains(p.SeqNum) {
		log.Warningf(
			"%s received duplicated data for job %s! Sequence number: %d. Ignoring batch.",
			aw.Config.WorkerId, aw.jobID, p.SeqNum,
		)
		return
	}

	if len(p.Rows) != 0 {
		aw.aggregateBatch(p)
	}
	aw.processedBatches.Add(p.SeqNum)
}

func (aw *AggregatorWorker) PropagateEndSignal(payload *ic.EndSignalPayload) {
	log.Debugf("Worker %s done, propagating end signal", aw.Config.WorkerId)

	payload.AddWorkerDone(aw.Config.WorkerId)

	if len(payload.WorkersDone) == aw.Config.WorkersCount {
		log.Debugf("All workers done. Deleting input queue.")
		// endSignal, _ := ic.NewEndSignal(nil, payload.SeqNum).Marshal()
		// aw.output.Send(endSignal)
		aw.input.Delete()
		return
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
}
