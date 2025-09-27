package common

import (
	a "aggregator/common/aggFunctions"
	"fmt"

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
	onMessage mw.OnMessageCallback
	batchChan chan ic.RowsBatch
	closeChan chan struct{}
}

func NewAggregatorWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware) *AggregatorWorker {
	batchChan := make(chan ic.RowsBatch, maxBatchBufferSize)
	onMessage := onMessageFromConfig(config, batchChan)

	return &AggregatorWorker{
		Config:    config,
		input:     input,
		output:    output,
		onMessage: onMessage,
		batchChan: batchChan,
		closeChan: make(chan struct{}),
	}
}

func (a *AggregatorWorker) Start() {
	if err := a.input.StartConsuming(a.onMessage); err != nil {
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
	if err := a.input.StopConsuming(); err != nil {
		log.Errorf("Failed to stop consuming messages: %v", err)
	}
	if err := a.output.StopConsuming(); err != nil {
		log.Errorf("Failed to stop producing messages: %v", err)
	}
	close(a.closeChan)
}

func onMessageFromConfig(config *Config, batchChan chan ic.RowsBatch) mw.OnMessageCallback {
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

		batchChan <- aggregatedBatch
		done <- nil
	}
}

func aggregateRows(batch *ic.RowsBatch, config *Config) ([][]interface{}, error) {

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

	result := getAggregatedRowsFromGroupedData(groupedData)

	return result, nil
}

func getAggregatedRowsFromGroupedData(groupedData map[string][]a.Aggregation) [][]interface{} {
	var result [][]interface{}
	for key, aggs := range groupedData {
		row := []interface{}{key}
		for _, agg := range aggs {
			row = append(row, agg.Result())
		}
		result = append(result, row)
	}
	return result
}

func getAggColIndexes(config *Config, batch *ic.RowsBatch) map[string]int {
	var aggColIndexes map[string]int = make(map[string]int)
	for _, agg := range config.Aggregations {
		for i, colName := range batch.ColumnNames {
			if colName == agg.Col {
				aggColIndexes[agg.Col] = i
				break
			}
		}
	}
	return aggColIndexes
}

func getGroupByColIndexes(config *Config, batch *ic.RowsBatch) []int {
	var groupByIndexes []int
	for _, groupByCol := range config.GroupBy {
		for i, colName := range batch.ColumnNames {
			if colName == groupByCol {
				groupByIndexes = append(groupByIndexes, i)
				break
			}
		}
	}
	return groupByIndexes
}

func getGroupByKey(groupByIndexes []int, row []interface{}) string {
	var keyParts []string
	for _, idx := range groupByIndexes {
		stringKey := fmt.Sprintf("%v", row[idx])
		keyParts = append(keyParts, stringKey)
	}
	return joinParts(keyParts, "-")
}

func joinParts(keyParts []string, separator string) string {
	key := ""
	if len(keyParts) > 0 {
		key = keyParts[0]
		for i := 1; i < len(keyParts); i++ {
			key += separator + keyParts[i]
		}
	}
	return key
}

func getBatchFromAggregatedRows(config *Config, aggregatedRows [][]interface{}) ic.RowsBatch {
	var aggregatedColumnNames []string

	groupedColName := joinParts(config.GroupBy, "-")
	aggregatedColumnNames = append(aggregatedColumnNames, groupedColName)

	for _, agg := range config.Aggregations {
		aggColName := joinParts([]string{agg.Func, agg.Col}, "_")
		aggregatedColumnNames = append(aggregatedColumnNames, aggColName)
	}

	return ic.RowsBatch{
		ColumnNames: aggregatedColumnNames,
		Rows:        aggregatedRows,
	}
}
