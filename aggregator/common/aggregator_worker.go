package common

import (
	a "aggregator/common/aggFunctions"
	dr "aggregator/common/dataRetainer"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config       *Config
	input        mw.MessageMiddleware
	output       mw.MessageMiddleware
	callback     mw.OnMessageCallback
	reducedData  map[string][]a.Aggregation
	dataRetainer dr.DataRetainer
	closeChan    chan struct{}
}

func NewAggregatorWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware) *AggregatorWorker {
	reducedData := make(map[string][]a.Aggregation)
	aggregator := AggregatorWorker{
		Config:       config,
		input:        input,
		output:       output,
		reducedData:  reducedData,
		dataRetainer: dr.NewDataRetainer(config.Retainings),
		closeChan:    make(chan struct{}),
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
}

func (aw *AggregatorWorker) Close() {
	if err := aw.input.Close(); err != nil {
		log.Errorf("Failed to close input: %v", err)
	}
	if err := aw.output.Close(); err != nil {
		log.Errorf("Failed to close output: %v", err)
	}
	close(aw.closeChan)
}

func (aw *AggregatorWorker) messageCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		log.Debugf("Worker %s received message: %s", aw.Config.WorkerId, string(consumeChannel.Body))
		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if len(batch.Rows) != 0 {
			aw.aggregateBatch(batch)
		}

		done <- nil

		if batch.IsEndSignal() {
			retainedData := aw.dataRetainer.RetainData(
				aw.Config.GroupBy,
				aw.Config.Aggregations,
				aw.reducedData,
			)
			aw.sendRetainedData(retainedData)
			aw.PropagateEndSignal(batch)
			aw.Close()
		}

	}
}

func (aw *AggregatorWorker) sendRetainedData(differentFormats []dr.RetainedData) {
	for _, dataInfo := range differentFormats {
		log.Debugf(
			"Worker %s sending reduced data, total groups: %d",
			aw.Config.WorkerId, len(dataInfo.Data),
		)

		dataBatch := dr.RetainedData{
			KeyColumns:   dataInfo.KeyColumns,
			Aggregations: dataInfo.Aggregations,
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

			dataBatch.Data = append(dataBatch.Data, row)
			i++
		}

		if len(dataBatch.Data) > 0 {
			aw.sendDataBatch(dataBatch)

		}
	}
}

func (aw *AggregatorWorker) aggregateBatch(batch *ic.RowsBatch) {

	groupByIndexes := getGroupByColIndexes(aw.Config, batch)

	aggIndexes := getAggColIndexes(aw.Config, batch)

	for _, row := range batch.Rows {
		if len(row) != len(batch.ColumnNames) {
			// ignore row
			log.Warningf("Row length %d does not match column names length %d, ignoring row", len(row), len(batch.ColumnNames))
			continue
		}

		if aw.Config.DropNa {
			hasNil := false
			for _, idx := range groupByIndexes {
				if row[idx] == nil {
					hasNil = true
					break
				}
			}
			if hasNil {
				continue
			}
		}

		key := getGroupByKey(groupByIndexes, row)

		if _, exists := aw.reducedData[key]; !exists {
			aw.reducedData[key] = make([]a.Aggregation, len(aw.Config.Aggregations))
			for i, agg := range aw.Config.Aggregations {
				aw.reducedData[key][i] = a.NewAggregation(agg.Func)
			}
		}

		for i, agg := range aw.Config.Aggregations {
			idx := aggIndexes[agg.Col]
			aw.reducedData[key][i] = aw.reducedData[key][i].Add(row[idx])
		}
	}
}

func (aw *AggregatorWorker) sendDataBatch(data dr.RetainedData) {
	batch := getBatchFromAggregatedRows(
		data.KeyColumns,
		data.Aggregations,
		aw.Config.IsReducer,
		&data.Data,
	)

	batchBytes, err := batch.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
	}

	log.Debugf("Sending data batch: %v", string(batchBytes))
	if err := aw.output.Send(batchBytes); err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
}

func (aw *AggregatorWorker) PropagateEndSignal(batch *ic.RowsBatch) {
	log.Debugf("Worker %s done, propagating end signal", aw.Config.WorkerId)

	batch.AddWorkerDone(aw.Config.WorkerId)

	if len(batch.WorkersDone) == aw.Config.WorkersCount {
		log.Debugf("All workers done")
		endSignal, _ := ic.NewEndSignal().Marshal()
		aw.output.Send(endSignal)
		return
	}

	endSignal, _ := batch.Marshal()
	aw.input.Send(endSignal) // re-enqueue the end signal with updated workers done
}
