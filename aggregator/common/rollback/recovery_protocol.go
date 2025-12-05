package rollback

import (
	"aggregator/common/persistence"

	"github.com/op/go-logging"
	"github.com/patricioibar/distribuidos-tp/bitmap"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

func getWorkerPrefix(workerID string) string {
	if workerID == "" {
		return ""
	}
	i := 0
	for i < len(workerID) && workerID[i] != '-' {
		i++
	}
	prefix := workerID[:i]
	if prefix == "" {
		return ""
	}
	return prefix
}

func RecoveryResponsesSourceName(reducerID string) string {
	prefix := getWorkerPrefix(reducerID)
	return prefix + "-recovery-responses"
}

func RecoveryRequestsSourceName(aggregatorID string) string {
	return aggregatorID + "-recovery-requests"
}

func SendAllOkToEveryAggregator(addr string, jobID string, aggregatorsDone []string) {
	for _, aggregatorID := range aggregatorsDone {
		SendAllOkToWorker(aggregatorID, addr, jobID)
	}
}

// func AskForLogsFromDuplicatedBatches(aggregatorID string, addr string, jobID string, duplicatedBatches *bitmap.Bitmap) {
// 	producer := getRecoveryRequestProducer(aggregatorID, addr, jobID)
// 	if producer == nil {
// 		return
// 	}
// 	defer producer.Close()
// 	msg, _ := NewRecoveryRequest(duplicatedBatches.ToArray()).Marshal()
// 	if err := producer.Send(msg); err != nil {
// 		log.Errorf("Failed to publish recovery request message: %v", err)
// 	}
// }

func AskForLogsFromDuplicatedBatches(aggregatorID string, addr string, jobID string, duplicatedBatches *bitmap.Bitmap) {
	producer := getRecoveryRequestProducer(aggregatorID, addr, jobID)
	if producer == nil {
		return
	}
	defer producer.Close()
	arr := duplicatedBatches.ToArray()
	for _, dupSeq := range arr {
		msg, _ := NewRecoveryRequest([]uint64{dupSeq}).Marshal()
		if err := producer.Send(msg); err != nil {
			log.Errorf("Failed to publish recovery request message: %v", err)
		}
	}
}

func SendAllOkToWorker(aggregatorID string, addr string, jobID string) {
	producer := getRecoveryRequestProducer(aggregatorID, addr, jobID)
	if producer == nil {
		return
	}
	defer producer.Close()
	msg, _ := NewRecoveryAllOK(aggregatorID).Marshal()
	if err := producer.Send(msg); err != nil {
		log.Errorf("Failed to publish recovery request message: %v", err)
	}
}

func getRecoveryRequestProducer(aggregatorID string, addr string, jobID string) *mw.Producer {
	producer, err := mw.NewProducer(
		RecoveryRequestsSourceName(aggregatorID),
		addr,
		jobID,
	)
	if err != nil {
		log.Errorf("Failed to create recovery requests producer for job %s: %v", jobID, err)
		return nil
	}
	return producer
}

func GetRecoveryResponseProducer(aggregatorID string, addr string, jobID string) *mw.Producer {
	producer, err := mw.NewProducer(
		RecoveryResponsesSourceName(aggregatorID),
		addr,
		jobID,
	)
	if err != nil {
		log.Errorf("Failed to create recovery requests producer: %v", err)
		return nil
	}
	return producer
}

func SendOperationLogResponse(aggregatorID string, addr string, jobID string, op *persistence.AggregateBatchOp, producer *mw.Producer) {
	msg, _ := NewRecoveryResponse(aggregatorID, op.SeqNumber(), op.Data).Marshal()
	if err := producer.Send(msg); err != nil {
		log.Errorf("Failed to publish recovery request message: %v", err)
	}
}
