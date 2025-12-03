package common

import (
	pers "aggregator/common/persistence"
	"aggregator/common/rollback"
	"slices"
	"sync"

	"github.com/patricioibar/distribuidos-tp/bitmap"
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
			intersection := bitmap.And(aw.processedBatches(), p.Sequences.Bitmap)
			if intersection.GetCardinality() != 0 {
				log.Warningf(
					"%s received duplicated data for job %s! Received %d duplicated sequence numbers: %s",
					aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(), intersection.String(),
				)
			}
			// these are batches without data, just marking as processed
			if p.Sequences.Bitmap.GetCardinality() > 0 {
				seq := p.Sequences.Bitmap.Maximum()
				op := pers.NewAddEmptyBatchesOp(seq, p.Sequences.Bitmap)
				if err := aw.state.Log(op); err != nil {
					log.Errorf("Failed to mark batch %d as empty: %v", seq, err)
				}
			}
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
			aw.aggregatedData(),
		)
		aw.sendRetainedData(retainedData)
		aw.sendProcessedBatches()
	})
	aw.PropagateEndSignal(p)
	return true
}

func (aw *AggregatorWorker) aggregateNewRowsBatch(p *ic.RowsBatchPayload) {
	if aw.processedBatches().Contains(p.SeqNum) {
		log.Warningf(
			"%s received duplicated data for job %s! Sequence number: %d. Ignoring batch.",
			aw.Config.WorkerId, aw.jobID, p.SeqNum,
		)
		return
	}

	if len(p.Rows) == 0 {
		bm := bitmap.New()
		bm.Add(p.SeqNum)
		op := pers.NewAddEmptyBatchesOp(p.SeqNum, bm)
		if err := aw.state.Log(op); err != nil {
			log.Errorf("Failed to mark batch %d as empty: %v", p.SeqNum, err)
			return
		}
	} else {
		// Aggregate batch data and persist state
		// also saves p.SeqNum as processed
		aw.aggregateBatch(p, aw.Config.WorkerId)
	}
}

func (aw *AggregatorWorker) PropagateEndSignal(payload *ic.EndSignalPayload) {
	log.Debugf("Worker %s done, propagating end signal", aw.Config.WorkerId)

	payload.AddWorkerDone(aw.Config.WorkerId)

	if len(payload.WorkersDone) == aw.Config.WorkersCount {
		log.Debugf("All workers done. Deleting input queue.")
		// endSignal, _ := ic.NewEndSignal(nil, payload.SeqNum).Marshal()
		// aw.output.Send(endSignal)
		aw.waitForRecoveryRequests()
		aw.input.Delete()
		return
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
}

func (aw *AggregatorWorker) waitForRecoveryRequests() {
	consumer, err := mw.NewConsumer(
		aw.Config.WorkerId,
		rollback.RecoveryResponsesSourceName(aw.Config.WorkerId),
		aw.Config.MiddlewareAddress,
		aw.jobID,
	)
	if err != nil {
		log.Errorf("Failed to create recovery responses consumer for job %s: %v", aw.jobID, err)
		return
	}

	callback := func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		defer func() { done <- nil }()
		// log.Debugf("Worker %s received message: %s", aw.Config.WorkerId, string(consumeChannel.Body))
		jsonStr := string(consumeChannel.Body)
		var receivedMessage rollback.Message
		if err := receivedMessage.Unmarshal([]byte(jsonStr)); err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch receivedMessage.Type {
		case rollback.TypeRequest:
			p, ok := receivedMessage.Payload.(*rollback.RecoveryRequestPayload)
			if !ok {
				log.Errorf("Invalid payload for rollback request")
				return
			}

			for _, seqNum := range p.SequenceNumbers {
				op, err := aw.state.GetOperationWithSeqNumber(seqNum)
				if err != nil {
					log.Errorf("Failed to get operation for seq %d: %v", seqNum, err)
					continue
				}

				opCasted, ok := op.(*pers.AggregateBatchOp)
				if !ok {
					log.Errorf("Operation for seq %d is not an AggregateBatchOp", seqNum)
					continue
				}

				rollback.SendOperationLogResponse(
					aw.Config.WorkerId,
					aw.Config.MiddlewareAddress,
					aw.jobID,
					opCasted,
				)
			}

		case rollback.TypeAllOK:
			log.Debugf("Received All OK for recovery requests.")
			consumer.Delete()

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
		}
	}

	// blocks until queue is deleted
	consumer.StartConsuming(callback)
	consumer.Close()
}
