package common

import (
	a "aggregator/common/aggFunctions"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config      *Config
	input       mw.MessageMiddleware
	output      mw.MessageMiddleware
	callback    mw.OnMessageCallback
	reducedData map[string][]a.Aggregation
	closeChan   chan struct{}
}

func NewAggregatorWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware) *AggregatorWorker {
	reducedData := make(map[string][]a.Aggregation)
	aggregator := AggregatorWorker{
		Config:      config,
		input:       input,
		output:      output,
		reducedData: reducedData,
		closeChan:   make(chan struct{}),
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
	println("#### Closing worker:", aw.Config.WorkerId)
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
		done <- nil
		log.Debugf("Worker %s received message: %s", aw.Config.WorkerId, string(consumeChannel.Body))
		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		if len(batch.Rows) != 0 {
			aw.aggregateBatch(batch)
		}

		if batch.IsEndSignal() {
			aw.SendReducedData()
			aw.PropagateEndSignal(batch)
		}

		println("#### Exiting callback")
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

func (aw *AggregatorWorker) SendReducedData() {
	log.Debugf("Worker %s sending reduced data, total groups: %d", aw.Config.WorkerId, len(aw.reducedData))

	batchSize := aw.Config.BatchSize

	reducedDataBatch := make(map[string][]a.Aggregation)
	i := 0
	for key, aggs := range aw.reducedData {
		if i >= batchSize {
			aw.sendReducedDataBatch(reducedDataBatch)

			reducedDataBatch = make(map[string][]a.Aggregation)
			i = 0
		}

		reducedDataBatch[key] = aggs
		i++
	}

	if len(reducedDataBatch) > 0 {
		aw.sendReducedDataBatch(reducedDataBatch)

	}

}

func (aw *AggregatorWorker) sendReducedDataBatch(groupedData map[string][]a.Aggregation) {
	rows := getAggregatedRowsFromGroupedData(&groupedData)
	batch := getBatchFromAggregatedRows(aw.Config, rows)

	data, err := batch.Marshal()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
	}

	log.Debugf("Sending data batch: %v", string(data))
	if err := aw.output.Send(data); err != nil {
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
