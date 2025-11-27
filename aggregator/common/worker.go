package common

import (
	a "aggregator/common/aggFunctions"
	dr "aggregator/common/dataRetainer"
	"encoding/json"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config   *Config
	input    mw.MessageMiddleware
	output   mw.MessageMiddleware
	callback mw.OnMessageCallback
	jobID    string

	// Aggregated data
	reducedData      map[string][]a.Aggregation
	dataRetainer     dr.DataRetainer
	processedBatches *roaring.Bitmap
	nextBatchSeq     uint64

	// Reducer specific fields
	rcvedAggData    map[string]*roaring.Bitmap
	aggregatorsDone []string

	// Closing synchronization
	closeChan     chan struct{}
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
		rcvedAggData:     nil,
		aggregatorsDone:  nil,
		nextBatchSeq:     nextBatchSeq,
	}

	if config.IsReducer {
		aggregator.callback = aggregator.reducerMessageCallback()
		aggregator.rcvedAggData = make(map[string]*roaring.Bitmap)
		aggregator.aggregatorsDone = make([]string, 0)
	} else {
		aggregator.callback = aggregator.aggregatorMessageCallback()
	}

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
				if seq >= aw.nextBatchSeq {
					aw.sendAggregatedDataBatch(dataBatch, seq)
				}

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
			log.Debugf("Formatted row: %v", formatedRow)
			dataBatch.Data = append(dataBatch.Data, formatedRow)
			i++
		}

		if len(dataBatch.Data) > 0 {
			aw.sendAggregatedDataBatch(dataBatch, seq)

		}
	}
	return seq + 1
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
	}

	// log.Debugf("Sending data batch: %v", string(batchBytes))
	if err := aw.output.Send(batchBytes); err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
}
