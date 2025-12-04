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
	aw.PropagateEndSignal(p)
	sendDataOnce.Do(func() {
		retainedData := aw.dataRetainer.RetainData(
			aw.Config.GroupBy,
			aw.Config.Aggregations,
			aw.aggregatedData(),
		)
		aw.sendRetainedData(retainedData)

		consumer, producer, shouldReturn := createRecoveryProducerAndConsumer(aw)
		if shouldReturn {
			return
		}

		aw.sendProcessedBatches()
		aw.waitForRecoveryRequests(producer, consumer)
		close(aw.closeChan)
	})
	log.Debugf("[%s] Done.", aw.jobID)
	return true
}

func createRecoveryProducerAndConsumer(aw *AggregatorWorker) (*mw.Consumer, *mw.Producer, bool) {
	consumer, err := mw.NewConsumer(
		aw.Config.WorkerId,
		rollback.RecoveryRequestsSourceName(aw.Config.WorkerId),
		aw.Config.MiddlewareAddress,
		aw.jobID,
	)
	if err != nil {
		log.Errorf("Failed to create recovery responses consumer for job %s: %v", aw.jobID, err)
		return nil, nil, true
	}
	producer := rollback.GetRecoveryResponseProducer(
		aw.Config.WorkerId,
		aw.Config.MiddlewareAddress,
		aw.jobID,
	)
	if producer == nil {
		log.Errorf("Failed to create recovery responses producer for job %s", aw.jobID)
		return nil, nil, true
	}
	return consumer, producer, false
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

func (aw *AggregatorWorker) PropagateEndSignal(payload *ic.EndSignalPayload) bool {
	log.Debugf("Worker %s done, propagating end signal", aw.Config.WorkerId)

	payload.AddWorkerDone(aw.Config.WorkerId)

	if len(payload.WorkersDone) == aw.Config.WorkersCount {
		return true
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
	return false
}

func (aw *AggregatorWorker) waitForRecoveryRequests(responsesProducer *mw.Producer, requestConsumer *mw.Consumer) {
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
			log.Debugf("[%s] Received recovery request for %d batches.", aw.jobID, len(p.SequenceNumbers))

			var opCasted *pers.AggregateBatchOp
			for _, seqNum := range p.SequenceNumbers {

				op, err := aw.state.GetOperationWithSeqNumber(seqNum)
				if err != nil {
					log.Errorf("Failed to get operation for seq %d: %v", seqNum, err)
					opCasted = pers.NewAggregateBatchOp(seqNum, map[string][]interface{}{})
				} else {
					opCasted, ok = op.(*pers.AggregateBatchOp)
					if !ok {
						log.Debugf("[%s] Operation for seq %d is not an AggregateBatchOp, sending empty AggregateBatchOp instead", aw.jobID, seqNum)
						opCasted = pers.NewAggregateBatchOp(seqNum, map[string][]interface{}{})
					}
				}

				rollback.SendOperationLogResponse(
					aw.Config.WorkerId,
					aw.Config.MiddlewareAddress,
					aw.jobID,
					opCasted,
					responsesProducer,
				)
			}
			log.Debugf("[%s] All recovery batches sent.", aw.jobID)

		case rollback.TypeAllOK:
			requestConsumer.Delete()

		default:
			log.Errorf("Unexpected message type: %s", receivedMessage.Type)
		}
	}

	// blocks until queue is deleted
	log.Debugf("Worker %s waiting for recovery requests for job %s...", aw.Config.WorkerId, aw.jobID)
	requestConsumer.StartConsuming(callback)
	log.Debugf("[%s] Received All OK for recovery requests.", aw.jobID)
	requestConsumer.Close()
	responsesProducer.Close()
}
