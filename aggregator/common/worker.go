package common

import (
	a "aggregator/common/aggFunctions"
	dr "aggregator/common/dataRetainer"
	p "aggregator/common/persistence"
	"encoding/json"
	"sync"

	"github.com/op/go-logging"
	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
	"github.com/patricioibar/distribuidos-tp/persistance"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config       *Config
	input        mw.MessageMiddleware
	output       mw.MessageMiddleware
	callback     mw.OnMessageCallback
	dataRetainer dr.DataRetainer
	jobID        string

	state *persistance.StateManager

	// Closing synchronization
	removeFromMap chan string
	closeOnce     sync.Once
}

func NewAggregatorWorker(
	config *Config,
	input mw.MessageMiddleware,
	output mw.MessageMiddleware,
	jobID string,
	removeFromMap chan string,
	nextBatchSeq uint64,
) *AggregatorWorker {
	sm := p.InitStateManager(jobID)
	if sm == nil {
		log.Errorf("StateManager not initialized for job %s", jobID)
		return nil
	}
	aggregator := AggregatorWorker{
		Config:        config,
		input:         input,
		output:        output,
		dataRetainer:  dr.NewDataRetainer(config.Retainings),
		removeFromMap: removeFromMap,
		jobID:         jobID,
		closeOnce:     sync.Once{},
		state:         sm,
	}

	if config.IsReducer {
		aggregator.callback = aggregator.reducerMessageCallback()
	} else {
		aggregator.callback = aggregator.aggregatorMessageCallback()
	}

	return &aggregator
}

func (aw *AggregatorWorker) Start() {
	// blocks until queue is deleted or closed
	if err := aw.input.StartConsuming(aw.callback); err != nil {
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

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

func (aw *AggregatorWorker) processedBatches() *bitmap.Bitmap {
	persState, _ := aw.state.GetState().(*p.PersistentState)
	return persState.AggregatedBatches
}

func (aw *AggregatorWorker) emptyBatches() *bitmap.Bitmap {
	persState, _ := aw.state.GetState().(*p.PersistentState)
	return persState.EmptyBatches
}

func (aw *AggregatorWorker) aggregatedData() map[string][]a.Aggregation {
	persState, _ := aw.state.GetState().(*p.PersistentState)
	data := persState.AggregatedData
	formatedData := make(map[string][]a.Aggregation)
	for key, aggInterfaces := range data {
		aggs := make([]a.Aggregation, len(aggInterfaces))
		for i, aggInterface := range aggInterfaces {
			if i >= len(aw.Config.Aggregations) {
				log.Errorf("Mismatch in number of aggregations for key %s: expected at least %d, got %d", key, i+1, len(aw.Config.Aggregations))
				continue
			}
			agg := a.NewAggregation(aw.Config.Aggregations[i].Func)
			agg.Set(aggInterface)
			aggs[i] = agg

		}
		formatedData[key] = aggs
	}
	return formatedData
}

func (aw *AggregatorWorker) sendProcessedBatches() {
	batches := bitmap.New()
	batches.Or(aw.processedBatches())
	batches.Or(aw.emptyBatches())
	seqSetMsg := ic.NewSequenceSet(aw.Config.WorkerId, batches)
	seqSetBytes, err := seqSetMsg.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal sequence set message: %v", err)
		return
	}
	if err := aw.output.Send(seqSetBytes); err != nil {
		log.Errorf("Failed to send processed batches message: %v", err)
	}
}

func (aw *AggregatorWorker) sendRetainedData(differentFormats []dr.RetainedData) uint64 {
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
	var seq uint64 = 0
	for _, dataInfo := range differentFormats {
		log.Debugf(
			"key_columns: %v, aggregations: %v, total groups: %d",
			dataInfo.KeyColumns, dataInfo.Aggregations, len(dataInfo.Data),
		)
		// log.Debugf("datainfo data: %v", dataInfo.Data)

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
				aw.sendAggregatedDataBatch(dataBatch, seq)
				seq++
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
			// log.Debugf("Formatted row: %v", formatedRow)
			dataBatch.Data = append(dataBatch.Data, formatedRow)
			i++
		}

		if len(dataBatch.Data) > 0 {
			aw.sendAggregatedDataBatch(dataBatch, seq)
			seq++
		}
	}
	return seq
}

func (aw *AggregatorWorker) aggregateBatch(batch *ic.RowsBatchPayload, workerID string) {

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

	reducedData := make(map[string][]a.Aggregation)
	for _, row := range batch.Rows {
		if len(row) != len(batch.ColumnNames) {
			// ignore row
			log.Warningf("Row length %d does not match column names length %d, ignoring row", len(row), len(batch.ColumnNames))
			continue
		}

		if hasNil(groupByIndexes, row) || hasNil(aggIndexesToSlice(aggIndexes, aw.Config.Aggregations), row) {
			// log.Debugf("skipping row with nil: %v", row)
			continue
		}

		key := getGroupByKey(groupByIndexes, row)

		if _, exists := reducedData[key]; !exists {
			reducedData[key] = make([]a.Aggregation, len(aw.Config.Aggregations))
			for i, agg := range aw.Config.Aggregations {
				reducedData[key][i] = a.NewAggregation(agg.Func)
				// log.Debugf("Initialized aggregation %s for key %s", agg.Func, key)
			}
		}

		for i, agg := range aw.Config.Aggregations {
			idx := aggIndexes[agg.Col]
			reducedData[key][i] = reducedData[key][i].Add(row[idx])
		}
	}

	aw.persistReducedData(batch.SeqNum, reducedData, workerID)
}

func (aw *AggregatorWorker) persistReducedData(seq uint64, reducedData map[string][]a.Aggregation, workerID string) {
	data := make(map[string][]interface{})
	for key, aggs := range reducedData {
		row := make([]interface{}, len(aggs))
		for i, agg := range aggs {
			row[i] = agg.Result()
		}
		data[key] = row
	}

	var op persistance.Operation
	if aw.Config.IsReducer {
		op = p.NewReceiveAggregatorBatchOp(seq, workerID, data)
	} else {
		op = p.NewAggregateBatchOp(seq, data)
	}
	if err := aw.state.Log(op); err != nil {
		log.Errorf("Failed to apply aggregate batch op for seq %d: %v", seq, err)
	}
}

func (aw *AggregatorWorker) sendAggregatedDataBatch(data dr.RetainedData, seq uint64) {
	batch := getBatchFromAggregatedRows(
		data.KeyColumns,
		data.Aggregations,
		aw.Config.IsReducer,
		&data.Data,
	)
	var msg *ic.Message
	if aw.Config.IsReducer {
		msg = ic.NewRowsBatch(batch.ColumnNames, batch.Rows, seq)
	} else {
		msg = ic.NewAggregatedData(batch.ColumnNames, batch.Rows, aw.Config.WorkerId, seq)
	}
	batchBytes, err := msg.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
		return
	}

	// log.Debugf("Sending data batch: %v", string(batchBytes))
	if err := aw.output.Send(batchBytes); err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
}
