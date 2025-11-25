package common

import (
	a "aggregator/common/aggFunctions"
	dr "aggregator/common/dataRetainer"
	"encoding/json"
	"slices"
	"strings"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config           *Config
	input            mw.MessageMiddleware
	output           mw.MessageMiddleware
	callback         mw.OnMessageCallback
	reducedData      map[string][]a.Aggregation
	dataRetainer     dr.DataRetainer
	processedBatches *roaring.Bitmap
	closeChan        chan struct{}
	removeFromMap    chan string
	jobID            string
	closeOnce        sync.Once
}

func NewAggregatorWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware, jobID string, removeFromMap chan string) *AggregatorWorker {
	reducedData := make(map[string][]a.Aggregation)
	aggregator := AggregatorWorker{
		Config:           config,
		input:            input,
		output:           output,
		reducedData:      reducedData,
		dataRetainer:     dr.NewDataRetainer(config.Retainings),
		processedBatches: roaring.New(),
		closeChan:        make(chan struct{}),
		removeFromMap:    removeFromMap,
		jobID:            jobID,
		closeOnce:        sync.Once{},
	}

	aggregator.callback = aggregator.messageCallback()

	return &aggregator
}

func (aw *AggregatorWorker) Start() {
	if err := aw.input.StartConsuming(aw.callback); err != nil {
		aw.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	<-aw.closeChan
	aw.Close()
}

func (aw *AggregatorWorker) Close() {
	aw.closeOnce.Do(func() {
		log.Info("Closing worker...")
		if err := aw.input.Close(); err != nil {
			log.Errorf("Failed to close input: %v", err)
		}
		if err := aw.output.Close(); err != nil {
			log.Errorf("Failed to close output: %v", err)
		}
		aw.removeFromMap <- aw.jobID
		log.Info("Successfully closed worker")
	})
}

func (aw *AggregatorWorker) messageCallback() mw.OnMessageCallback {
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
			if len(p.Rows) != 0 {
				aw.aggregateBatch(p)
			}
			aw.processedBatches.Add(p.SeqNum)

			done <- nil

		case *ic.EndSignalPayload:
			retainedData := aw.dataRetainer.RetainData(
				aw.Config.GroupBy,
				aw.Config.Aggregations,
				aw.reducedData,
			)
			aw.sendRetainedData(retainedData)
			aw.sendProcessedBatches()
			aw.PropagateEndSignal(p)
			done <- nil
			close(aw.closeChan)

		case *ic.SequenceSetPayload:
			intersection := roaring.And(aw.processedBatches, p.Sequences.Bitmap)
			if intersection.GetCardinality() != 0 {
				log.Warningf(
					"%s received duplicated data for job %s! Received %d duplicated sequence numbers: %s",
					aw.Config.WorkerId, aw.jobID, intersection.GetCardinality(), intersection.String(),
				)

				// TODO handle duplicated data case
			} else {
				aw.processedBatches.Or(p.Sequences.Bitmap)
			}
			done <- nil

		default:
			log.Errorf("Unknown message payload type: %T", p)
			done <- nil
		}
	}
}

func (aw *AggregatorWorker) sendProcessedBatches() {
	seqSetMsg := ic.NewSequenceSet(aw.Config.WorkerId, aw.processedBatches)
	seqSetBytes, err := seqSetMsg.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal sequence set message: %v", err)
		return
	}
	if err := aw.output.Send(seqSetBytes); err != nil {
		log.Errorf("Failed to send processed batches message: %v", err)
	}
}

func (aw *AggregatorWorker) sendRetainedData(differentFormats []dr.RetainedData) {
	log.Debugf("Worker %s sending retained data, different formats: %d", aw.Config.WorkerId, len(differentFormats))
	uniqueKeyColumns := make(map[string]struct{})
	uniqueAggregations := make(map[string]struct{})
	for _, dataInfo := range differentFormats {
		for _, col := range dataInfo.KeyColumns {
			uniqueKeyColumns[col] = struct{}{}
		}
		for _, agg := range dataInfo.Aggregations {
			str, _ := json.Marshal(agg)
			uniqueAggregations[string(str)] = struct{}{}
		}
	}
	var outputKeyColumns []string
	var outputAggregations []a.AggConfig
	for col := range uniqueKeyColumns {
		outputKeyColumns = append(outputKeyColumns, col)
	}
	for aggStr := range uniqueAggregations {
		var agg a.AggConfig
		if err := json.Unmarshal([]byte(aggStr), &agg); err != nil {
			log.Errorf("Failed to unmarshal aggregation: %v", err)
			continue
		}
		outputAggregations = append(outputAggregations, agg)
	}

	for _, dataInfo := range differentFormats {
		log.Debugf(
			"key_columns: %v, aggregations: %v, total groups: %d",
			dataInfo.KeyColumns, dataInfo.Aggregations, len(dataInfo.Data),
		)
		log.Debugf("datainfo data: %v", dataInfo.Data)

		aggsColnames := aggsAsColNames(dataInfo.Aggregations)

		dataBatch := dr.RetainedData{
			KeyColumns:   outputKeyColumns,
			Aggregations: outputAggregations,
			Data:         make([][]interface{}, 0),
		}

		batchSize := aw.Config.BatchSize

		i := 0
		for _, row := range dataInfo.Data {
			if i >= batchSize {
				aw.sendDataBatch(dataBatch)

				dataBatch.Data = make([][]interface{}, 0)
				i = 0
			}

			formatedRow := make([]interface{}, len(outputKeyColumns)+len(outputAggregations))

			for j, col := range outputKeyColumns {
				colIndex := getIndex(col, dataInfo.KeyColumns)
				if colIndex != -1 {
					formatedRow[j] = row[colIndex]
				} else {
					formatedRow[j] = nil
				}
			}

			for j, agg := range outputAggregations {
				aggIndex := getIndex(agg.Col, aggsColnames)
				if aggIndex != -1 {
					formatedRow[len(outputKeyColumns)+j] = row[len(dataInfo.KeyColumns)+aggIndex]
				} else {
					formatedRow[len(outputKeyColumns)+j] = nil
				}
			}
			log.Debugf("Formatted row: %v", formatedRow)
			dataBatch.Data = append(dataBatch.Data, formatedRow)
			i++
		}

		if len(dataBatch.Data) > 0 {
			aw.sendDataBatch(dataBatch)

		}
	}
}

func (aw *AggregatorWorker) aggregateBatch(batch *ic.RowsBatchPayload) {

	groupByIndexes := getGroupByColIndexes(aw.Config, batch)
	for _, idx := range groupByIndexes {
		if idx == -1 {
			log.Errorf("One of the group by columns not found in batch columns: %v", batch.ColumnNames)
			return
		}
	}
	aggIndexes := getAggColIndexes(aw.Config, batch)
	for _, idx := range aggIndexes {
		if idx == -1 {
			log.Errorf("One of the aggregation columns not found in batch columns: %v", batch.ColumnNames)
			return
		}
	}

	for _, row := range batch.Rows {
		if len(row) != len(batch.ColumnNames) {
			// ignore row
			log.Warningf("Row length %d does not match column names length %d, ignoring row", len(row), len(batch.ColumnNames))
			continue
		}

		if hasNil(groupByIndexes, row) || hasNil(aggIndexesToSlice(aggIndexes, aw.Config.Aggregations), row) {
			log.Debugf("skipping row with nil: %v", row)
			continue
		}

		key := getGroupByKey(groupByIndexes, row)

		if _, exists := aw.reducedData[key]; !exists {
			aw.reducedData[key] = make([]a.Aggregation, len(aw.Config.Aggregations))
			for i, agg := range aw.Config.Aggregations {
				aw.reducedData[key][i] = a.NewAggregation(agg.Func)
				log.Debugf("Initialized aggregation %s for key %s", agg.Func, key)
			}
		}

		for i, agg := range aw.Config.Aggregations {
			idx := aggIndexes[agg.Col]
			aw.reducedData[key][i] = aw.reducedData[key][i].Add(row[idx])
		}
	}
}

func aggIndexesToSlice(aggIndexes map[string]int, aggs []a.AggConfig) []int {
	indexes := make([]int, len(aggs))
	for i, agg := range aggs {
		indexes[i] = aggIndexes[agg.Col]
	}
	return indexes
}

func hasNil(groupByIndexes []int, row []interface{}) bool {
	for _, idx := range groupByIndexes {
		value := row[idx]
		if value == nil {
			return true
		}
		if strValue, ok := value.(string); ok {
			strValue = strings.TrimSpace(strings.ToUpper(strValue))
			if strValue == "" || strValue == "NULL" {
				return true
			}
		}
	}
	return false
}

func (aw *AggregatorWorker) sendDataBatch(data dr.RetainedData) {
	batch := getBatchFromAggregatedRows(
		data.KeyColumns,
		data.Aggregations,
		aw.Config.IsReducer,
		&data.Data,
	)
	msg := ic.NewRowsBatch(batch.ColumnNames, batch.Rows, 0)
	batchBytes, err := msg.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
	}

	// log.Debugf("Sending data batch: %v", string(batchBytes))
	if err := aw.output.Send(batchBytes); err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
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
		endSignal, _ := ic.NewEndSignal(nil, payload.SeqNum).Marshal()
		aw.output.Send(endSignal)
		return
	}

	msg := ic.NewEndSignal(payload.WorkersDone, payload.SeqNum)
	endSignal, _ := msg.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
}
