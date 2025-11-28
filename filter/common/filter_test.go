package filter

import (
	"encoding/json"
	"testing"
	"time"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

// Mock implementations for testing
type MockMiddleware struct {
	messages          chan []byte
	onMessage         mw.OnMessageCallback
	isConsuming       bool
	shouldFailSend    bool
	shouldFailConsume bool
	deletedChan       chan struct{}
}

func NewMockMiddleware() *MockMiddleware {
	return &MockMiddleware{
		messages:    make(chan []byte, 100),
		deletedChan: make(chan struct{}),
	}
}

func (m *MockMiddleware) StartConsuming(callback mw.OnMessageCallback) *mw.MessageMiddlewareError {
	if m.shouldFailConsume {
		return &mw.MessageMiddlewareError{
			Code: mw.MessageMiddlewareMessageError,
			Msg:  "mock consume error",
		}
	}

	m.onMessage = callback
	m.isConsuming = true
	<-m.deletedChan
	return nil
}

func (m *MockMiddleware) StopConsuming() *mw.MessageMiddlewareError {
	m.isConsuming = false
	return nil
}

func (m *MockMiddleware) Send(message []byte) *mw.MessageMiddlewareError {

	if m.shouldFailSend {
		return &mw.MessageMiddlewareError{
			Code: mw.MessageMiddlewareMessageError,
			Msg:  "mock send error",
		}
	}

	m.messages <- message
	return nil
}

func (m *MockMiddleware) Close() *mw.MessageMiddlewareError {
	return nil
}

func (m *MockMiddleware) Delete() *mw.MessageMiddlewareError {
	close(m.deletedChan)
	return nil
}

func (m *MockMiddleware) GetOneMessage() []byte {
	return <-m.messages
}

func (m *MockMiddleware) SimulateMessage(data []byte) {
	if m.onMessage != nil && m.isConsuming {
		message := mw.MiddlewareMessage{
			Body: data,
		}
		done := make(chan *mw.MessageMiddlewareError, 1)
		m.onMessage(message, done)
		<-done // wait for processing to complete
	}
}

// Test for NewFilter with updated signature (workerID parameter)
func TestNewFilter(t *testing.T) {
	input := NewMockMiddleware()
	output := NewMockMiddleware()

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
	input := NewMockMiddleware()
	output := NewMockMiddleware()

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
	input := NewMockMiddleware()
	output := NewMockMiddleware()

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
	messageBytes := output.GetOneMessage()
	var msg ic.Message
	if err := msg.Unmarshal(messageBytes); err != nil {
		t.Fatalf("failed to unmarshal output message: %v", err)
	}

	if msg.Type != ic.MsgEndSignal {
		t.Fatalf("expected payload to be of type []*ic.EndSignal")
	}
}
