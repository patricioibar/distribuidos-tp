package filter

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

// Mock implementations for testing
type MockMiddleware struct {
	mu                sync.Mutex
	messages          [][]byte
	onMessage         mw.OnMessageCallback
	isConsuming       bool
	shouldFailSend    bool
	shouldFailConsume bool
}

func (m *MockMiddleware) StartConsuming(callback mw.OnMessageCallback) *mw.MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFailConsume {
		return &mw.MessageMiddlewareError{
			Code: mw.MessageMiddlewareMessageError,
			Msg:  "mock consume error",
		}
	}

	m.onMessage = callback
	m.isConsuming = true
	return nil
}

func (m *MockMiddleware) StopConsuming() *mw.MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isConsuming = false
	return nil
}

func (m *MockMiddleware) Send(message []byte) *mw.MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFailSend {
		return &mw.MessageMiddlewareError{
			Code: mw.MessageMiddlewareMessageError,
			Msg:  "mock send error",
		}
	}

	m.messages = append(m.messages, message)
	return nil
}

func (m *MockMiddleware) Close() *mw.MessageMiddlewareError {
	return nil
}

func (m *MockMiddleware) Delete() *mw.MessageMiddlewareError {
	return nil
}

func (m *MockMiddleware) GetMessages() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages
}

func (m *MockMiddleware) SimulateMessage(data []byte) {
	if m.onMessage != nil && m.isConsuming {
		message := mw.MiddlewareMessage{
			Body: data,
		}
		done := make(chan *mw.MessageMiddlewareError, 1)
		go m.onMessage(message, done)
		<-done // wait for processing to complete
	}
}

// Test for NewFilter with updated signature (workerID parameter)
func TestNewFilter(t *testing.T) {
	input := &MockMiddleware{}
	output := &MockMiddleware{}

	filter := NewFilter("test-worker-1", input, output, "TbyYear")

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
			batchChan := make(chan ic.RowsBatch, 10)
			defer close(batchChan)

			// Create a filter worker to test the method
			filterWorker := &FilterWorker{
				filterId:     "test-worker",
				workersCount: 1,
				batchChan:    batchChan,
				closeChan:    make(chan struct{}),
			}

			callback, err := filterWorker.getFilterFunction(batchChan, tt.filterType)

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
	input := &MockMiddleware{}
	output := &MockMiddleware{}

	// Create filter worker
	filterWorker := &FilterWorker{
		filterId:     "test-worker",
		workersCount: 1,
		input:        input,
		output:       output,
		batchChan:    make(chan ic.RowsBatch, 10),
		closeChan:    make(chan struct{}),
	}

	batchChan := filterWorker.batchChan
	callback, err := filterWorker.getFilterFunction(batchChan, "TbyYear")
	if err != nil {
		t.Fatalf("failed to get filter function: %v", err)
	}

	// Test with end signal
	endSignal := ic.NewEndSignal()
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

// Integration test for complete workflow
func TestFilterWorkerIntegration(t *testing.T) {
	input := &MockMiddleware{}
	output := &MockMiddleware{}

	// Create filter worker with workerID
	filter := NewFilter("test-worker", input, output, "TbyYear")

	// Test data with mixed years
	testBatch := ic.RowsBatch{
		ColumnNames: []string{"id", "year", "data"},
		Rows: [][]interface{}{
			{1, "2024-03-15 10:30:45", "sample data"},
			{2, "2025-08-20 14:22:33", "more data"},
			{3, "2023-01-10 09:15:20", "old data"}, // should be filtered out
		},
		EndSignal: false,
	}

	testData, err := json.Marshal(testBatch)
	if err != nil {
		t.Fatalf("failed to marshal test data: %v", err)
	}

	// Start the filter worker in a goroutine
	go func() {
		filter.Start()
	}()

	// Give some time for setup
	time.Sleep(10 * time.Millisecond)

	// Send test message
	input.SimulateMessage(testData)

	// Give some time for processing
	time.Sleep(50 * time.Millisecond)

	// Check output messages
	messages := output.GetMessages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Parse the result
	var resultBatch ic.RowsBatch
	if err := json.Unmarshal(messages[0], &resultBatch); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	// Verify the result - Only 2024 and 2025 data should pass through the filter
	expectedRows := 2
	if len(resultBatch.Rows) != expectedRows {
		t.Errorf("expected %d rows, got %d", expectedRows, len(resultBatch.Rows))
	}

	// Clean up
	filter.Close()
}

// Test end signal handling
func TestEndSignalHandling(t *testing.T) {
	input := &MockMiddleware{}
	output := &MockMiddleware{}

	// Create filter with workersCount = 1 (single worker scenario)
	filter := NewFilter("worker-1", input, output, "TbyYear")
	filter.workersCount = 1

	// Start filter in background
	go func() {
		filter.Start()
	}()

	// Give time to start
	time.Sleep(10 * time.Millisecond)

	// Send end signal
	endSignal := ic.NewEndSignal()
	endData, err := json.Marshal(endSignal)
	if err != nil {
		t.Fatalf("failed to marshal end signal: %v", err)
	}

	input.SimulateMessage(endData)

	// Give time for processing
	time.Sleep(50 * time.Millisecond)

	// Should have one end signal message in output (since workersCount = 1)
	messages := output.GetMessages()
	if len(messages) != 1 {
		t.Errorf("expected 1 end signal message, got %d", len(messages))
	}

	filter.Close()
}