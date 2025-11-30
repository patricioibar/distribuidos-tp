package filter

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

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
	onMessages  []mw.OnMessageCallback
	lastCalled  int
	started     chan struct{}
	deletedChan chan struct{}
	deleted     bool
}

func newStubConsumer() *StubConsumer {
	return &StubConsumer{
		started:     make(chan struct{}, 10),
		deletedChan: make(chan struct{}),
		onMessages:  make([]mw.OnMessageCallback, 0),
		lastCalled:  0,
		deleted:     false,
	}
}

func (s *StubConsumer) waitForStart() {
	<-s.started
}

func (s *StubConsumer) StartConsuming(onMessageCallback mw.OnMessageCallback) (error *mw.MessageMiddlewareError) {
	println("StubConsumer started")
	s.onMessages = append(s.onMessages, onMessageCallback)
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

// Test for NewFilter with updated signature (workerID parameter)
func TestNewFilter(t *testing.T) {
	input := newStubConsumer()
	output := newStubProducer()

	filter := NewFilter("test-worker-1", input, output, "TbyYear", 1, nil, "job-123")

	if filter == nil {
		t.Errorf("expected filter instance but got nil")
		return
	}

	if filter.filterId != "test-worker-1" {
		t.Errorf("expected filterId to be 'test-worker-1', got '%s'", filter.filterId)
	}

	if filter.input != input {
		t.Errorf("expected input to be set correctly")
	}

	if filter.output != output {
		t.Errorf("expected output to be set correctly")
	}

	if filter.filterFunction == nil {
		t.Errorf("expected filterFunction to be set")
	}
}

// Test for getFilterFunction method (now a method on FilterWorker)
func TestGetFilterFunction(t *testing.T) {
	tests := []struct {
		name       string
		filterType string
		shouldErr  bool
	}{
		{"valid TbyYear filter", "TbyYear", false},
		{"valid TbyHour filter", "TbyHour", false},
		{"valid TbyAmount filter", "TbyAmount", false},
		{"valid TIbyYear filter", "TIbyYear", false},
		{"invalid filter type", "invalidFilter", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create a filter worker to test the method
			filterWorker := &FilterWorker{
				filterId:     "test-worker",
				workersCount: 1,
			}

			callback, err := filterWorker.getFilterFunction(tt.filterType)

			if tt.shouldErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if callback == nil {
					t.Errorf("expected callback function but got nil")
				}
			}
		})
	}
}

// Test callback function behavior with end signals
func TestFilterCallbackWithEndSignal(t *testing.T) {
	input := newStubConsumer()
	output := newStubProducer()

	// Create filter worker
	filterWorker := NewFilter("test-worker", input, output, "TbyYear", 1, nil, "job-123")

	callback, err := filterWorker.getFilterFunction("TbyYear")
	if err != nil {
		t.Fatalf("failed to get filter function: %v", err)
	}

	// Test with end signal
	endSignal := ic.NewEndSignal(nil, 0)
	jsonData, err := json.Marshal(endSignal)
	if err != nil {
		t.Fatalf("failed to marshal end signal: %v", err)
	}

	message := mw.MiddlewareMessage{
		Body: jsonData,
	}

	done := make(chan *mw.MessageMiddlewareError, 1)
	callback(message, done)

	result := <-done
	if result != nil {
		t.Errorf("unexpected error processing end signal: %v", result)
	}
}

// Test end signal handling
func TestEndSignalHandling(t *testing.T) {
	input := newStubConsumer()
	output := newStubProducer()

	// Create filter with workersCount = 1 (single worker scenario)
	filter := NewFilter("worker-1", input, output, "TbyYear", 1, nil, "job-123")
	filter.workersCount = 1

	// Start filter in background
	go func() {
		filter.Start()
	}()

	// Give time to start
	time.Sleep(10 * time.Millisecond)

	// Send end signal
	endSignal := ic.NewEndSignal(nil, 0)
	endData, err := json.Marshal(endSignal)
	if err != nil {
		t.Fatalf("failed to marshal end signal: %v", err)
	}

	input.SimulateMessage(endData)

	// Give time for processing
	time.Sleep(500 * time.Millisecond)

	// Should have one end signal message in output (since workersCount = 1)
	output.waitForAMessage()
	messageBytes := output.sentMessages[0]
	var msg ic.Message
	if err := msg.Unmarshal(messageBytes); err != nil {
		t.Fatalf("failed to unmarshal output message: %v", err)
	}

	if msg.Type != ic.MsgEndSignal {
		t.Fatalf("expected payload to be of type []*ic.EndSignal")
	}
}

func TestFiltersDuplicateEndSignal(t *testing.T) {
	numOfWorkers := 3
	input := newStubConsumer()
	output := newStubProducer()

	waitToEnd := make([]chan struct{}, numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		workerId := fmt.Sprintf("worker-%d", i+1)
		filter := NewFilter(workerId, input, output, "TbyYear", 1, make(chan string, 1), "job-123")
		waitToEnd[i] = make(chan struct{}, 1)
		go func(i int, f *FilterWorker) {
			f.Start()
			waitToEnd[i] <- struct{}{}
		}(i, filter)
		input.waitForStart()
	}

	// Send duplicate end signals
	endSignal, _ := ic.NewEndSignal(nil, 0).Marshal()
	input.SimulateMessage(endSignal)
	input.SimulateMessage(endSignal)

	// Wait for workers to finish
	for i := 0; i < numOfWorkers; i++ {
		<-waitToEnd[i]
	}

	endSignalCount := 0
	for _, msg := range output.sentMessages {
		var outputMsg ic.Message
		if err := outputMsg.Unmarshal(msg); err != nil {
			t.Fatalf("Failed to unmarshal output message: %v", err)
		}
		if outputMsg.Type != ic.MsgEndSignal {
			continue
		}
		endSignalCount++
	}

	if endSignalCount == 0 {
		t.Fatalf("No end signal messages were sent by the workers")
	}
}
