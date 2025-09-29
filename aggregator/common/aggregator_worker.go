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
		defer func() { done <- nil }() // Acknowledge message after processing

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

	data, err := batch.String()
	if err != nil {
		log.Errorf("Failed to marshal batch: %v", err)
	}
	if err := aw.output.Send([]byte(data)); err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
}
