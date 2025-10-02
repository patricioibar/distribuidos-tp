package integration

import (
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func SimulateProcessing(
	testName string,
	rabbitAddr string,
	rightInput []ic.RowsBatch,
	leftInput []ic.RowsBatch,
	expectedOutputBatches int,
) []string {
	conn := mw.GetConnection(rabbitAddr)
	defer conn.Close()

	rightInputName := "r_input_" + testName
	r_producer, _ := mw.NewProducer(rightInputName, rabbitAddr)

	leftInputName := "l_input_" + testName
	l_producer, _ := mw.NewProducer(leftInputName, rabbitAddr)

	outputName := "output_" + testName
	consumer, _ := mw.NewConsumer("test_consumer_"+testName, outputName, rabbitAddr)

	var result []string
	msgReceived := make(chan struct{}, expectedOutputBatches)
	callback := func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		println("Received output message:", string(consumeChannel.Body))
		result = append(result, string(consumeChannel.Body))
		done <- nil
		msgReceived <- struct{}{}
	}
	consumer.StartConsuming(callback)
	defer consumer.Close()

	for _, batch := range rightInput {
		data, _ := batch.Marshal()
		r_producer.Send(data)
	}
	for _, batch := range leftInput {
		data, _ := batch.Marshal()
		l_producer.Send(data)
	}

	for i := 0; i < expectedOutputBatches; i++ {
		<-msgReceived
	}

	return result
}

func AssertIsEndSignal(t *testing.T, end_signal string) {
	got, err := ic.RowsBatchFromString(string(end_signal))
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if !got.IsEndSignal() {
		t.Fatalf("Expected end signal, got %v", got)
	}
}

func AssertOutputMatches(
	t *testing.T,
	expectedColumns []string,
	expectedRows [][]interface{},
	gotString ...string,
) {
	var gotBatches []*ic.RowsBatch
	for _, s := range gotString {
		batch, err := ic.RowsBatchFromString(s)
		if err != nil {
			t.Fatalf("Failed to unmarshal output: %v", err)
		}

		if len(batch.ColumnNames) != len(expectedColumns) {
			t.Fatalf("Expected %d column names, got %d", len(expectedColumns), len(batch.ColumnNames))
		}

		for _, col := range expectedColumns {
			found := false
			for _, gotCol := range batch.ColumnNames {
				if col == gotCol {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Expected column name %s not found in output", col)
			}
		}

		gotBatches = append(gotBatches, batch)
	}

	mergedRows := [][]interface{}{}
	for _, batch := range gotBatches {
		mergedRows = append(mergedRows, batch.Rows...)
	}

	for _, expectedRow := range expectedRows {
		found := false
		for _, gotRow := range mergedRows {
			match := true
			for i := range expectedRow {
				if expectedRow[i] != gotRow[i] {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected row %v not found in output", expectedRow)
		}
	}
}
