package integration

import (
	"os/exec"
	"strings"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func SimulateProcessing(
	testName string,
	rabbitAddr string,
	input []ic.RowsBatch,
	expectedOutputBatches int,
) []string {
	conn := mw.GetConnection(rabbitAddr)
	defer conn.Close()

	sourceName := "input_" + testName
	producer, _ := mw.NewProducer(sourceName, rabbitAddr)

	outputName := "output_" + testName
	consumer, _ := mw.NewConsumer("test_consumer_"+testName, outputName, rabbitAddr)

	var result []string
	msgReceived := make(chan struct{}, expectedOutputBatches)
	callback := func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		result = append(result, string(consumeChannel.Body))
		done <- nil
		msgReceived <- struct{}{}
	}
	consumer.StartConsuming(callback)
	defer consumer.Close()

	for _, batch := range input {
		data, _ := batch.Marshal()
		producer.Send(data)
	}

	for i := 0; i < expectedOutputBatches; i++ {
		<-msgReceived
	}

	return result
}

func RunBackgroundCmd(cmdStr string) chan error {
	cmdArr := strings.Split(cmdStr, " ")
	proc := exec.Command(cmdArr[0], cmdArr[1:]...)
	dockerErr := make(chan error)
	go func() {
		dockerErr <- proc.Run()
	}()
	return dockerErr
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

func AssertBatchesMatch(t *testing.T, expectedString string, gotString string) (*ic.RowsBatch, error) {
	expectedBatch, _ := ic.RowsBatchFromString(expectedString)
	got, err := ic.RowsBatchFromString(gotString)
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if len(got.Rows) != len(expectedBatch.Rows) {
		t.Fatalf("Expected %d rows, got %d", len(expectedBatch.Rows), len(got.Rows))
	}
	if len(got.ColumnNames) != len(expectedBatch.ColumnNames) {
		t.Fatalf("Expected %d column names, got %d", len(expectedBatch.ColumnNames), len(got.ColumnNames))
	}

	for _, col := range expectedBatch.ColumnNames {
		found := false
		for _, gotCol := range got.ColumnNames {
			if col == gotCol {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected column name %s not found in output", col)
		}
	}

	for _, expectedRow := range expectedBatch.Rows {
		found := false
		for _, gotRow := range got.Rows {
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
	return got, err
}
