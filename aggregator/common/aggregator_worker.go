package common

import (
	a "aggregator/common/aggFunctions"

	"github.com/op/go-logging"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const maxBatchBufferSize = 100

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config    *Config
	input     mw.MessageMiddleware
	output    mw.MessageMiddleware
	callback  mw.OnMessageCallback
	batchChan chan ic.RowsBatch
	closeChan chan struct{}
}

func NewAggregatorWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware) *AggregatorWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	onMessage := aggregatorMessageCallback(config, batchChan)

	return &AggregatorWorker{
		Config:    config,
		input:     input,
		output:    output,
		callback:  onMessage,
		batchChan: batchChan,
		closeChan: make(chan struct{}),
	}
}

func (a *AggregatorWorker) Start() {
	if err := a.input.StartConsuming(a.callback); err != nil {
		a.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	for {
		select {
		case <-a.closeChan:
			return
		case batch := <-a.batchChan:
			data, err := batch.String()
			if err != nil {
				log.Errorf("Failed to marshal batch: %v", err)
				continue
			}
			if err := a.output.Send([]byte(data)); err != nil {
				log.Errorf("Failed to send message: %v", err)
			}
		}
	}
}

func (a *AggregatorWorker) Close() {
	if err := a.input.Close(); err != nil {
		log.Errorf("Failed to close input: %v", err)
	}
	if err := a.output.Close(); err != nil {
		log.Errorf("Failed to close output: %v", err)
	}
	close(a.closeChan)
}

func aggregatorMessageCallback(config *Config, batchChan chan ic.RowsBatch) mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {

		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			// no mando un error por el chan acá,
			// porque si lo mando el mensaje se reencola y puede que
			// vuelva a fallar en bucle para siempre
			done <- nil
			return
		}

		if len(batch.Rows) == 0 {
			done <- nil
			return
		}

		aggregatedRows, err := aggregateRows(batch, config)

		if err != nil {
			log.Errorf("Failed to aggregate rows: %v", err)
			done <- nil
			return
		}

		aggregatedBatch := getBatchFromAggregatedRows(config, aggregatedRows)

		batchChan <- *aggregatedBatch
		done <- nil
	}
}

func aggregateRows(batch *ic.RowsBatch, config *Config) (*[][]interface{}, error) {

	groupByIndexes := getGroupByColIndexes(config, batch)

	aggIndexes := getAggColIndexes(config, batch)

	// key: group by values concatenated, value: aggregations list
	groupedData := make(map[string][]a.Aggregation)
	for _, row := range batch.Rows {
		if len(row) != len(batch.ColumnNames) {
			// ignore row
			log.Warningf("Row length %d does not match column names length %d, ignoring row", len(row), len(batch.ColumnNames))
			continue
		}

		key := getGroupByKey(groupByIndexes, row)

		if _, exists := groupedData[key]; !exists {
			groupedData[key] = make([]a.Aggregation, len(config.Aggregations))
			for i, agg := range config.Aggregations {
				groupedData[key][i] = a.NewAggregation(agg.Func)
			}
		}

		for i, agg := range config.Aggregations {
			idx := aggIndexes[agg.Col]
			groupedData[key][i] = groupedData[key][i].Add(row[idx])
		}
	}

	result := getAggregatedRowsFromGroupedData(&groupedData)

	return result, nil
}
