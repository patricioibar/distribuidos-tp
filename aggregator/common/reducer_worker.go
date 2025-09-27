package common

import (
	a "aggregator/common/aggFunctions"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

type ReducerWorker struct {
	Config      *Config
	input       mw.MessageMiddleware
	output      mw.MessageMiddleware
	callback    mw.OnMessageCallback
	reducedData map[string][]a.Aggregation
	closeChan   chan struct{}
}

func NewReducerWorker(config *Config, input mw.MessageMiddleware, output mw.MessageMiddleware) *ReducerWorker {
	reducedData := make(map[string][]a.Aggregation)
	reducer := ReducerWorker{
		Config:      config,
		input:       input,
		output:      output,
		reducedData: reducedData,
		closeChan:   make(chan struct{}),
	}

	reducer.callback = reducer.reducerMessageCallback()

	return &reducer
}

func (r *ReducerWorker) Start() {
	if err := r.input.StartConsuming(r.callback); err != nil {
		r.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	<-r.closeChan
}

func (r *ReducerWorker) Close() {
	if err := r.input.Close(); err != nil {
		log.Errorf("Failed to close input: %v", err)
	}
	if err := r.output.Close(); err != nil {
		log.Errorf("Failed to close output: %v", err)
	}
	close(r.closeChan)
}

func (reducer *ReducerWorker) reducerMessageCallback() mw.OnMessageCallback {
	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {

		defer func() { done <- nil }() // Acknowledge message after processing

		jsonStr := string(consumeChannel.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		if len(batch.Rows) != 0 {
			reducer.reduceBatch(batch)
		}

		if batch.IsEndSignal() {
			reducer.SendReducedData()
			return
		}
	}
}

func (r *ReducerWorker) reduceBatch(batch *ic.RowsBatch) {

	groupByIndexes := getGroupByColIndexes(r.Config, batch)

	aggIndexes := getAggColIndexes(r.Config, batch)

	for _, row := range batch.Rows {
		if len(row) != len(batch.ColumnNames) {
			// ignore row
			log.Warningf("Row length %d does not match column names length %d, ignoring row", len(row), len(batch.ColumnNames))
			continue
		}

		key := getGroupByKey(groupByIndexes, row)

		if _, exists := r.reducedData[key]; !exists {
			r.reducedData[key] = make([]a.Aggregation, len(r.Config.Aggregations))
			for i, agg := range r.Config.Aggregations {
				r.reducedData[key][i] = a.NewAggregation(agg.Func)
			}
		}

		for i, agg := range r.Config.Aggregations {
			idx := aggIndexes[agg.Col]
			r.reducedData[key][i] = r.reducedData[key][i].Add(row[idx])
		}
	}
}

func (r *ReducerWorker) SendReducedData() {
	panic("unimplemented")
}
