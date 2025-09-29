package common_test

import (
	"aggregator/common"
	"encoding/json"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

type StubProducer struct {
	sentMessages [][]byte
	newMessage   chan struct{}
}

func newStubProducer() *StubProducer {
	return &StubProducer{sentMessages: make([][]byte, 0), newMessage: make(chan struct{}, 10)}
}

func (s *StubProducer) waitForAMessage() {
	<-s.newMessage
}

func (s *StubProducer) Send(message []byte) (error *mw.MessageMiddlewareError) {
	s.sentMessages = append(s.sentMessages, message)
	s.newMessage <- struct{}{}
	return nil
}

func (s *StubProducer) StartConsuming(onMessageCallback mw.OnMessageCallback) (error *mw.MessageMiddlewareError) {
	return nil
}

func (s *StubProducer) StopConsuming() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubProducer) Close() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubProducer) Delete() (error *mw.MessageMiddlewareError) { return nil }

type StubConsumer struct {
	onMessage mw.OnMessageCallback
	started   chan struct{}
}

func newStubConsumer() *StubConsumer {
	return &StubConsumer{started: make(chan struct{}, 10)}
}

func (s *StubConsumer) waitForStart() {
	<-s.started
}

func (s *StubConsumer) StartConsuming(onMessageCallback mw.OnMessageCallback) (error *mw.MessageMiddlewareError) {
	s.onMessage = onMessageCallback
	s.started <- struct{}{}
	return nil
}

func (s *StubConsumer) InjectMessage(message []byte, doneChan chan *mw.MessageMiddlewareError) {
	s.onMessage(mw.MiddlewareMessage{Body: message}, doneChan)
}

func (s *StubConsumer) SimulateMessage(message []byte) {
	doneChan := make(chan *mw.MessageMiddlewareError)
	go func() { <-doneChan }()
	s.InjectMessage(message, doneChan)
}

func (s *StubConsumer) StopConsuming() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Send(message []byte) (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Close() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Delete() (error *mw.MessageMiddlewareError) { return nil }

var endSignal, _ = ic.NewEndSignal().Marshal()

func TestSumAggregatorWorker(t *testing.T) {
	input := newStubConsumer()
	output := newStubProducer()
	config := &common.Config{
		GroupBy:      []string{"category"},
		Aggregations: []common.AggConfig{{Col: "value", Func: "sum"}},
		BatchSize:    10,
	}
	worker := common.NewAggregatorWorker(config, input, output)
	go worker.Start()

	input.waitForStart()
	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
	).Marshal()
	input.SimulateMessage(msg)
	input.SimulateMessage(msg)
	input.SimulateMessage(endSignal)
	output.waitForAMessage()

	if len(output.sentMessages) != 1 {
		t.Fatalf("Expected 1 message sent, got %d", len(output.sentMessages))
	}

	var outputBatch ic.RowsBatch
	err := json.Unmarshal(output.sentMessages[0], &outputBatch)
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %v", err)
	}

	expectedRows := map[string]float64{"A": 80.0, "B": 40.0}
	if len(outputBatch.Rows) != len(expectedRows) {
		t.Fatalf("Expected %d rows, got %d", len(expectedRows), len(outputBatch.Rows))
	}

	for _, row := range outputBatch.Rows {
		category := row[0].(string)
		sum := row[1].(float64)
		expectedSum, exists := expectedRows[category]
		if !exists {
			t.Fatalf("Unexpected category %s in output", category)
		}
		if sum != expectedSum {
			t.Fatalf("Expected sum for category %s to be %v, got %v", category, expectedSum, sum)
		}
		delete(expectedRows, category)
	}
}

func TestCountAggregatorWorker(t *testing.T) {
	input := newStubConsumer()
	output := newStubProducer()
	config := &common.Config{
		GroupBy:      []string{"category"},
		Aggregations: []common.AggConfig{{Col: "value", Func: "count"}},
		BatchSize:    10,
	}
	worker := common.NewAggregatorWorker(config, input, output)
	go worker.Start()
	input.waitForStart()
	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
	).Marshal()
	input.SimulateMessage(msg)
	input.SimulateMessage(msg)
	input.SimulateMessage(endSignal)
	output.waitForAMessage()
	if len(output.sentMessages) != 1 {
		t.Fatalf("Expected 1 message sent, got %d", len(output.sentMessages))
	}

	var outputBatch ic.RowsBatch
	err := json.Unmarshal(output.sentMessages[0], &outputBatch)
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %v", err)
	}

	expectedRows := map[string]int{"A": 4, "B": 2}
	if len(outputBatch.Rows) != len(expectedRows) {
		t.Fatalf("Expected %d rows, got %d", len(expectedRows), len(outputBatch.Rows))
	}

	for _, row := range outputBatch.Rows {
		category := row[0].(string)
		count := int(row[1].(float64)) // JSON numbers are always float64
		expectedCount, exists := expectedRows[category]
		if !exists {
			t.Fatalf("Unexpected category %s in output", category)
		}
		if count != expectedCount {
			t.Fatalf("Expected count for category %s to be %d, got %d", category, expectedCount, count)
		}
		delete(expectedRows, category)
	}
}
