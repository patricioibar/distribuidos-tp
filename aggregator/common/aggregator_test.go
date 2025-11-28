package common_test

import (
	"aggregator/common"
	agf "aggregator/common/aggFunctions"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
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
	onMessages     []mw.OnMessageCallback
	onMessagesLock []sync.Mutex
	lastCalled     int
	started        chan struct{}
	deletedChan    chan struct{}
	deleted        bool
}

func newStubConsumer() *StubConsumer {
	return &StubConsumer{
		started:        make(chan struct{}, 10),
		deletedChan:    make(chan struct{}),
		onMessages:     make([]mw.OnMessageCallback, 0),
		onMessagesLock: make([]sync.Mutex, 0),
		lastCalled:     0,
		deleted:        false,
	}
}

func (s *StubConsumer) waitForStart() {
	<-s.started
}

func (s *StubConsumer) StartConsuming(onMessageCallback mw.OnMessageCallback) (error *mw.MessageMiddlewareError) {
	println("StubConsumer started")
	s.onMessages = append(s.onMessages, onMessageCallback)
	s.onMessagesLock = append(s.onMessagesLock, sync.Mutex{})
	s.started <- struct{}{}
	<-s.deletedChan
	return nil
}

func (s *StubConsumer) InjectMessage(message []byte, doneChan chan *mw.MessageMiddlewareError) {
	if len(s.onMessages) == 0 {
		return
	}
	// Pick a random callback using math/rand
	// calling := rand.Intn(len(s.onMessages))
	calling := s.lastCalled
	s.lastCalled = (s.lastCalled + 1) % len(s.onMessages)
	s.onMessages[calling](mw.MiddlewareMessage{Body: message}, doneChan)
}

func (s *StubConsumer) SimulateMessage(message []byte) {
	if s.deleted {
		return
	}
	println("Simulating message: ", string(message))
	doneChan := make(chan *mw.MessageMiddlewareError)
	msgCopy := make([]byte, len(message))
	copy(msgCopy, message)
	go func() {
		if err := <-doneChan; err != nil {
			print("#")
			// If the consumer was deleted in the meantime, don't requeue.
			select {
			case <-s.deletedChan:
				// input deleted, drop the message
				return
			default:
				s.SimulateMessage(msgCopy)
			}
		}
	}()
	// Inject a copy so the handler doesn't share the same underlying buffer when requeued
	s.InjectMessage(msgCopy, doneChan)
}

func (s *StubConsumer) Send(message []byte) (error *mw.MessageMiddlewareError) {
	s.SimulateMessage(message)
	return nil
}

func (s *StubConsumer) StopConsuming() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Close() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Delete() (error *mw.MessageMiddlewareError) {
	close(s.deletedChan)
	s.deleted = true
	return nil
}

var endSignal, _ = ic.NewEndSignal(nil, 0).Marshal()

func TestSumAggregatorWorker(t *testing.T) {
	config := &common.Config{
		WorkerId:     "worker-1",
		GroupBy:      []string{"category"},
		Aggregations: []agf.AggConfig{{Col: "value", Func: "sum"}},
		BatchSize:    10,
		WorkersCount: 1,
	}
	input, output := newWorker(config)

	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		0,
	).Marshal()
	msg1, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		1,
	).Marshal()
	input.SimulateMessage(msg)
	input.SimulateMessage(msg1)
	input.SimulateMessage(endSignal)
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 2 {
		t.Fatalf("Expected 2 message sent, got %d", len(output.sentMessages))
	}

	println("Received messages:")
	for _, m := range output.sentMessages {
		fmt.Printf("%s\n", string(m))
	}

	var outputMsg ic.Message
	err := json.Unmarshal(output.sentMessages[0], &outputMsg)
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %v", err)
	}

	outputBatch, ok := outputMsg.Payload.(*ic.AggregatedDataPayload)
	if !ok || outputBatch == nil {
		t.Fatalf("Failed to cast payload to *ic.AggregatedDataPayload, got: %T", outputMsg.Payload)
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
	config := &common.Config{
		WorkerId:     "worker-1",
		GroupBy:      []string{"category"},
		Aggregations: []agf.AggConfig{{Col: "value", Func: "count"}},
		BatchSize:    10,
		WorkersCount: 1,
	}
	input, output := newWorker(config)
	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		0,
	).Marshal()
	msg2, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		1,
	).Marshal()
	input.SimulateMessage(msg)
	input.SimulateMessage(msg2)
	input.SimulateMessage(endSignal)
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 2 {
		t.Fatalf("Expected 2 message sent, got %d", len(output.sentMessages))
	}
	println("Received messages:")
	for _, m := range output.sentMessages {
		fmt.Printf("%s\n", string(m))
	}
	var outputMsg ic.Message
	err := json.Unmarshal(output.sentMessages[0], &outputMsg)
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %v", err)
	}

	outputBatch, ok := outputMsg.Payload.(*ic.AggregatedDataPayload)
	if !ok || outputBatch == nil {
		t.Fatalf("Failed to cast payload to *ic.AggregatedDataPayload, got: %T", outputMsg.Payload)
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

func newWorker(config *common.Config) (*StubConsumer, *StubProducer) {
	input := newStubConsumer()
	output := newStubProducer()
	worker := common.NewAggregatorWorker(config, input, output, "", make(chan string, 1), 0)
	go worker.Start()
	input.waitForStart()
	return input, output
}

func newWorkers(config *common.Config, workersCount int) (*StubConsumer, *StubProducer) {
	input := newStubConsumer()
	output := newStubProducer()
	config.WorkersCount = workersCount
	for i := 0; i < workersCount; i++ {
		configCopy := common.Config{
			GroupBy:      config.GroupBy,
			Aggregations: config.Aggregations,
			BatchSize:    config.BatchSize,
			WorkersCount: config.WorkersCount,
			WorkerId:     fmt.Sprintf("worker-%d", i+1),
			LogLevel:     "DEBUG",
		}
		worker := common.NewAggregatorWorker(&configCopy, input, output, "", make(chan string, 1), 0)
		go worker.Start()
		input.waitForStart()
	}
	return input, output
}

func TestSumDuplicatedMessageAggregatorWorker(t *testing.T) {
	config := &common.Config{
		WorkerId:     "worker-1",
		GroupBy:      []string{"category"},
		Aggregations: []agf.AggConfig{{Col: "value", Func: "sum"}},
		BatchSize:    10,
		WorkersCount: 1,
	}
	input, output := newWorker(config)

	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		0,
	).Marshal()
	input.SimulateMessage(msg)
	input.SimulateMessage(msg)
	input.SimulateMessage(endSignal)
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 2 {
		t.Fatalf("Expected 2 message sent, got %d", len(output.sentMessages))
	}

	println("Received messages:")
	for _, m := range output.sentMessages {
		fmt.Printf("%s\n", string(m))
	}

	var outputMsg ic.Message
	err := json.Unmarshal(output.sentMessages[0], &outputMsg)
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %v", err)
	}

	outputBatch, ok := outputMsg.Payload.(*ic.AggregatedDataPayload)
	if !ok || outputBatch == nil {
		t.Fatalf("Failed to cast payload to *ic.AggregatedDataPayload, got: %T", outputMsg.Payload)
	}

	expectedRows := map[string]float64{"A": 40.0, "B": 20.0}
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

func TestTwoAggregatorWorkers(t *testing.T) {
	config1 := &common.Config{
		GroupBy: []string{"category"},
		Aggregations: []agf.AggConfig{
			{Col: "category", Func: "count"},
			{Col: "value", Func: "sum"}},
		BatchSize: 10,
	}
	inputs, output := newWorkers(config1, 2)
	msg, _ := ic.NewRowsBatch(
		[]string{"category", "value"},
		[][]interface{}{
			{"A", 10},
			{"B", 20},
			{"A", 30},
		},
		0,
	).Marshal()
	inputs.SimulateMessage(msg)
	inputs.SimulateMessage(endSignal)
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 3 {
		t.Errorf("Expected 3 message sent, got %d:", len(output.sentMessages))
		for _, m := range output.sentMessages {
			t.Errorf("%s\n", string(m))
		}
		t.FailNow()
	}

	println("Received messages:")
	for _, m := range output.sentMessages {
		fmt.Printf("%s\n", string(m))
	}

	expectedRows := map[string][]interface{}{"A": {2, 40.0}, "B": {1, 20.0}}
	endSignalCount := 0
	ackedSeqs := roaring.NewBitmap()
	for _, msg := range output.sentMessages {
		var outputMsg ic.Message
		err := json.Unmarshal([]byte(msg), &outputMsg)
		if err != nil {
			t.Fatalf("Failed to unmarshal output message %s: %v", string(msg), err)
		}

		switch p := outputMsg.Payload.(type) {
		case *ic.EndSignalPayload:
			endSignalCount++
		case *ic.AggregatedDataPayload:
			if len(p.Rows) != len(expectedRows) {
				t.Errorf("Expected %d rows, got %d", len(expectedRows), len(p.Rows))
			}
			for _, row := range p.Rows {
				category := row[0].(string)
				count := int(row[1].(float64)) // JSON numbers are always float64
				sum := row[2].(float64)
				expected, exists := expectedRows[category]
				if !exists {
					t.Errorf("Unexpected category %s in output", category)
				}
				if count != expected[0] {
					t.Errorf("Expected count for category %s to be %d, got %d", category, expected[0], count)
				}
				if sum != expected[1] {
					t.Errorf("Expected sum for category %s to be %v, got %v", category, expected[1], sum)
				}
				delete(expectedRows, category)
			}
		case *ic.SequenceSetPayload:
			ackedSeqs.Or(p.Sequences.Bitmap)
		default:
			t.Errorf("Unexpected payload type: %T", outputMsg.Payload)
		}
	}

	if ackedSeqs.GetCardinality() != 1 {
		t.Errorf("Expected 1 acked sequence number, got %d", ackedSeqs.GetCardinality())
	}
	if endSignalCount != 0 {
		t.Errorf("Expected 0 end signal, got %d", endSignalCount)
	}
}
func TestTwoAggregatorsDuplicateEndSignal(t *testing.T) {
	numOfWorkers := 2
	config := &common.Config{
		GroupBy:      []string{"category"},
		Aggregations: []agf.AggConfig{{Col: "value", Func: "sum"}},
		BatchSize:    10,
		WorkersCount: numOfWorkers,
	}
	input := newStubConsumer()
	output := newStubProducer()
	waitToEnd := make([]chan struct{}, numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		configCopy := common.Config{
			GroupBy:      config.GroupBy,
			Aggregations: config.Aggregations,
			BatchSize:    config.BatchSize,
			WorkersCount: config.WorkersCount,
			WorkerId:     fmt.Sprintf("worker-%d", i+1),
			LogLevel:     "DEBUG",
		}
		waitToEnd[i] = make(chan struct{}, 1)
		worker := common.NewAggregatorWorker(&configCopy, input, output, "", make(chan string, 1), 0)
		go func(i int) {
			worker.Start()
			fmt.Printf("worker %d ended", i+1)
			waitToEnd[i] <- struct{}{}
		}(i)
		input.waitForStart()
	}

	// Send duplicate end signals to ensure both workers handle them and exit cleanly
	input.SimulateMessage(endSignal)
	input.SimulateMessage(endSignal)

	output.waitForAMessage()
	output.waitForAMessage()

	println("received two messages")

	// Wait for both workers to end
	for i := 0; i < numOfWorkers; i++ {
		<-waitToEnd[i]
	}
	println("both workers ended")
	if len(output.sentMessages) < numOfWorkers {
		t.Fatalf("Expected at least %d messages (one per worker), got %d", numOfWorkers, len(output.sentMessages))
	}

	workersEnded := make(map[string]bool)
	for _, msg := range output.sentMessages {
		var outputMsg ic.Message
		if err := json.Unmarshal(msg, &outputMsg); err != nil {
			t.Fatalf("Failed to unmarshal output message: %v", err)
		}
		if outputMsg.Type != ic.MsgSequenceSet {
			continue
		}
		payload, ok := outputMsg.Payload.(*ic.SequenceSetPayload)
		if !ok {
			t.Fatalf("Failed to cast payload to *ic.SequenceSetPayload, got: %T", outputMsg.Payload)
		}
		workersEnded[payload.WorkerID] = true
	}

	if len(workersEnded) != numOfWorkers {
		t.Fatalf("Expected messages from %d workers, got %d", numOfWorkers, len(workersEnded))
	}
}
