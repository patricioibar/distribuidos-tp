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

func (m *MockMiddleware) ClearMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = nil
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

// Test for parseTimestamp function
func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  time.Time
		shouldErr bool
	}{
		{
			name:      "valid timestamp",
			input:     "2024-03-15 10:30:45",
			expected:  time.Date(2024, time.March, 15, 10, 30, 45, 0, time.UTC),
			shouldErr: false,
		},
		{
			name:      "valid timestamp different date",
			input:     "2025-07-20 14:22:33",
			expected:  time.Date(2025, time.July, 20, 14, 22, 33, 0, time.UTC),
			shouldErr: false,
		},
		{
			name:      "invalid format - wrong separator",
			input:     "2024/03/15 10:30:45",
			shouldErr: true,
		},
		{
			name:      "invalid format - missing time",
			input:     "2024-03-15",
			shouldErr: true,
		},
		{
			name:      "invalid format - empty string",
			input:     "",
			shouldErr: true,
		},
		{
			name:      "invalid format - wrong time format",
			input:     "2024-03-15 10:30",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimestamp(tt.input)

			if tt.shouldErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if !result.Equal(tt.expected) {
					t.Errorf("expected %v, got %v", tt.expected, result)
				}
			}
		})
	}
}

// Test for filterRowsByYear function
func TestFilterRowsByYear(t *testing.T) {
	tests := []struct {
		name      string
		batch     ic.RowsBatch
		expected  ic.RowsBatch
		shouldErr bool
		errorMsg  string
	}{
		{
			name: "valid 2024 first semester data",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1, "2024-03-15 10:30:45", "sample data"},
					{2, "2024-05-20 14:22:33", "more data"},
				},
				EndSignal: false,
			},
			expected: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data", "semester"},
				Rows: [][]interface{}{
					{1, "2024-03-15 10:30:45", "sample data", "FirstSemester"},
					{2, "2024-05-20 14:22:33", "more data", "FirstSemester"},
				},
				EndSignal: false,
			},
			shouldErr: false,
		},
		{
			name: "valid 2025 second semester data",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "value"},
				Rows: [][]interface{}{
					{1, "2025-08-15 10:30:45", 100},
					{2, "2025-12-20 14:22:33", 200},
				},
				EndSignal: false,
			},
			expected: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "value", "semester"},
				Rows: [][]interface{}{
					{1, "2025-08-15 10:30:45", 100, "SecondSemester"},
					{2, "2025-12-20 14:22:33", 200, "SecondSemester"},
				},
				EndSignal: false,
			},
			shouldErr: false,
		},
		{
			name: "mixed years - only 2024/2025 should pass",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1, "2023-03-15 10:30:45", "old data"},
					{2, "2024-03-15 10:30:45", "good data"},
					{3, "2026-03-15 10:30:45", "future data"},
					{4, "2025-08-15 10:30:45", "good data 2"},
				},
				EndSignal: false,
			},
			expected: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data", "semester"},
				Rows: [][]interface{}{
					{2, "2024-03-15 10:30:45", "good data", "FirstSemester"},
					{4, "2025-08-15 10:30:45", "good data 2", "SecondSemester"},
				},
				EndSignal: false,
			},
			shouldErr: false,
		},
		{
			name: "no year column",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "timestamp", "data"},
				Rows: [][]interface{}{
					{1, "2024-03-15 10:30:45", "sample data"},
				},
				EndSignal: false,
			},
			shouldErr: true,
			errorMsg:  "year column not found",
		},
		{
			name: "row with insufficient columns",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1}, // missing columns
				},
				EndSignal: false,
			},
			shouldErr: true,
			errorMsg:  "row does not have enough columns",
		},
		{
			name: "invalid timestamp format",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1, "invalid-timestamp", "sample data"},
				},
				EndSignal: false,
			},
			shouldErr: true,
		},
		{
			name: "year column not string",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1, 20240315, "sample data"},
				},
				EndSignal: false,
			},
			shouldErr: true,
			errorMsg:  "year column is not a string",
		},
		{
			name: "empty batch",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows:        [][]interface{}{},
				EndSignal:   false,
			},
			expected: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data", "semester"},
				Rows:        [][]interface{}{},
				EndSignal:   false,
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filterRowsByYear(tt.batch)

			if tt.shouldErr {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}

				// Check column names
				if len(result.ColumnNames) != len(tt.expected.ColumnNames) {
					t.Errorf("expected %d columns, got %d", len(tt.expected.ColumnNames), len(result.ColumnNames))
					return
				}

				for i, col := range tt.expected.ColumnNames {
					if result.ColumnNames[i] != col {
						t.Errorf("expected column %d to be '%s', got '%s'", i, col, result.ColumnNames[i])
					}
				}

				// Check rows
				if len(result.Rows) != len(tt.expected.Rows) {
					t.Errorf("expected %d rows, got %d", len(tt.expected.Rows), len(result.Rows))
					return
				}

				for i, expectedRow := range tt.expected.Rows {
					if len(result.Rows[i]) != len(expectedRow) {
						t.Errorf("row %d: expected %d columns, got %d", i, len(expectedRow), len(result.Rows[i]))
						continue
					}

					for j, expectedVal := range expectedRow {
						if result.Rows[i][j] != expectedVal {
							t.Errorf("row %d, col %d: expected %v, got %v", i, j, expectedVal, result.Rows[i][j])
						}
					}
				}

				// Check JobDone
				if result.EndSignal != tt.expected.EndSignal {
					t.Errorf("expected JobDone %v, got %v", tt.expected.EndSignal, result.EndSignal)
				}
			}
		})
	}
}

// Test for getFilterFunction
func TestGetFilterFunction(t *testing.T) {
	tests := []struct {
		name       string
		filterType string
		shouldErr  bool
		errorMsg   string
	}{
		{
			name:       "valid byYear filter",
			filterType: "byYear",
			shouldErr:  false,
		},
		{
			name:       "invalid filter type",
			filterType: "invalidFilter",
			shouldErr:  true,
			errorMsg:   "unknown filter type",
		},
		{
			name:       "empty filter type",
			filterType: "",
			shouldErr:  true,
			errorMsg:   "unknown filter type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchChan := make(chan ic.RowsBatch, 10)
			callback, err := getFilterFunction(batchChan, tt.filterType)

			if tt.shouldErr {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if callback == nil {
					t.Errorf("expected callback function but got nil")
				}
			}

			close(batchChan)
		})
	}
}

// Test callback function behavior
func TestFilterCallbackFunction(t *testing.T) {
	batchChan := make(chan ic.RowsBatch, 10)
	callback, err := getFilterFunction(batchChan, "byYear")
	if err != nil {
		t.Fatalf("failed to get filter function: %v", err)
	}

	tests := []struct {
		name          string
		batch         ic.RowsBatch
		expectedBatch bool
		shouldError   bool
	}{
		{
			name: "valid batch with 2024 data",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows: [][]interface{}{
					{1, "2024-03-15 10:30:45", "sample data"},
				},
				EndSignal: false,
			},
			expectedBatch: true,
			shouldError:   false,
		},
		{
			name: "empty batch",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "data"},
				Rows:        [][]interface{}{},
				EndSignal:   false,
			},
			expectedBatch: false,
			shouldError:   false,
		},
		{
			name: "invalid JSON format",
			batch: ic.RowsBatch{
				ColumnNames: []string{"id", "timestamp", "data"}, // no year column
				Rows: [][]interface{}{
					{1, "2024-03-15 10:30:45", "sample data"},
				},
				EndSignal: false,
			},
			expectedBatch: false,
			shouldError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create message from batch
			jsonData, err := json.Marshal(tt.batch)
			if err != nil {
				t.Fatalf("failed to marshal batch: %v", err)
			}

			message := mw.MiddlewareMessage{
				Body: jsonData,
			}

			done := make(chan *mw.MessageMiddlewareError, 1)

			// Execute callback
			callback(message, done)

			// Check result
			result := <-done

			if tt.shouldError {
				if result == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if result != nil {
					t.Errorf("unexpected error: %v", result)
				}
			}

			// Check if batch was sent to channel
			select {
			case receivedBatch := <-batchChan:
				if !tt.expectedBatch {
					t.Errorf("expected no batch but got one: %+v", receivedBatch)
				}
			default:
				if tt.expectedBatch {
					t.Errorf("expected batch but got none")
				}
			}
		})
	}

	close(batchChan)
}

// Test for NewFilter
func TestNewFilter(t *testing.T) {
	t.Run("valid byYear filter", func(t *testing.T) {
		input := &MockMiddleware{}
		output := &MockMiddleware{}

		filter := NewFilter(input, output, "byYear")

		if filter == nil {
			t.Errorf("expected filter instance but got nil")
			return
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

		if filter.batchChan == nil {
			t.Errorf("expected batchChan to be initialized")
		}

		if filter.closeChan == nil {
			t.Errorf("expected closeChan to be initialized")
		}
	})

	// Note: We can't easily test invalid filter types because NewFilter
	// calls log.Fatalf which terminates the process. This is a design
	// choice in the original code that makes unit testing difficult.
	// In a production environment, this should be refactored to return
	// an error instead of calling log.Fatalf.
}

// Integration test for complete workflow
func TestFilterWorkerIntegration(t *testing.T) {
	input := &MockMiddleware{}
	output := &MockMiddleware{}

	// Create filter worker
	filter := NewFilter(input, output, "byYear")
	if filter == nil {
		t.Fatalf("failed to create filter worker")
	}

	// Test data
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

	// Verify the result
	expectedRows := 2 // Only 2024 and 2025 data should pass
	if len(resultBatch.Rows) != expectedRows {
		t.Errorf("expected %d rows, got %d", expectedRows, len(resultBatch.Rows))
	}

	// Check that semester column was added
	expectedColumns := 4 // original 3 + semester
	if len(resultBatch.ColumnNames) != expectedColumns {
		t.Errorf("expected %d columns, got %d", expectedColumns, len(resultBatch.ColumnNames))
	}

	if resultBatch.ColumnNames[3] != "semester" {
		t.Errorf("expected last column to be 'semester', got '%s'", resultBatch.ColumnNames[3])
	}

	// Verify semester assignments
	for i, row := range resultBatch.Rows {
		if len(row) != 4 {
			t.Errorf("row %d: expected 4 columns, got %d", i, len(row))
			continue
		}

		semester, ok := row[3].(string)
		if !ok {
			t.Errorf("row %d: semester should be string, got %T", i, row[3])
			continue
		}

		timestamp, ok := row[1].(string)
		if !ok {
			t.Errorf("row %d: timestamp should be string, got %T", i, row[1])
			continue
		}

		// Verify correct semester assignment
		if timestamp == "2024-03-15 10:30:45" && semester != "FirstSemester" {
			t.Errorf("March 2024 should be FirstSemester, got %s", semester)
		}
		if timestamp == "2025-08-20 14:22:33" && semester != "SecondSemester" {
			t.Errorf("August 2025 should be SecondSemester, got %s", semester)
		}
	}

	// Clean up
	filter.Close()
}

// Test for Start and Close methods
func TestFilterWorkerStartAndClose(t *testing.T) {
	t.Run("successful start and close", func(t *testing.T) {
		input := &MockMiddleware{}
		output := &MockMiddleware{}
		filter := NewFilter(input, output, "byYear")

		// Test that Start begins consuming
		done := make(chan bool, 1)
		go func() {
			filter.Start()
			done <- true
		}()

		// Give some time to start
		time.Sleep(10 * time.Millisecond)

		// Verify that input middleware started consuming
		if !input.isConsuming {
			t.Errorf("expected input middleware to be consuming")
		}

		// Close the filter
		filter.Close()

		// Wait for Start to finish
		<-done

		// Verify that input middleware stopped consuming
		if input.isConsuming {
			t.Errorf("expected input middleware to stop consuming after close")
		}
	})

	// Note: We can't easily test the consume error case because Start
	// calls log.Fatalf when StartConsuming fails, which terminates the process.
	// This is a design choice in the original code that makes unit testing difficult.
}

// Test error handling scenarios
func TestFilterWorkerErrorHandling(t *testing.T) {
	t.Run("output send error", func(t *testing.T) {
		input := &MockMiddleware{}
		output := &MockMiddleware{shouldFailSend: true}
		filter := NewFilter(input, output, "byYear")

		// Start filter in background
		go func() {
			filter.Start()
		}()

		// Give time to start
		time.Sleep(10 * time.Millisecond)

		// Send valid test data
		testBatch := ic.RowsBatch{
			ColumnNames: []string{"id", "year", "data"},
			Rows: [][]interface{}{
				{1, "2024-03-15 10:30:45", "sample data"},
			},
			EndSignal: false,
		}

		testData, err := json.Marshal(testBatch)
		if err != nil {
			t.Fatalf("failed to marshal test data: %v", err)
		}

		// Simulate message
		input.SimulateMessage(testData)

		// Give time for processing
		time.Sleep(50 * time.Millisecond)

		// Should not have any messages due to send error
		messages := output.GetMessages()
		if len(messages) != 0 {
			t.Errorf("expected no messages due to send error, got %d", len(messages))
		}

		filter.Close()
	})

	t.Run("invalid JSON in callback", func(t *testing.T) {
		input := &MockMiddleware{}
		output := &MockMiddleware{}
		filter := NewFilter(input, output, "byYear")

		// Start filter in background
		go func() {
			filter.Start()
		}()

		// Give time to start
		time.Sleep(10 * time.Millisecond)

		// Send invalid JSON data
		invalidData := []byte("{invalid json}")
		input.SimulateMessage(invalidData)

		// Give time for processing
		time.Sleep(50 * time.Millisecond)

		// Should not have any messages due to JSON error
		messages := output.GetMessages()
		if len(messages) != 0 {
			t.Errorf("expected no messages due to JSON error, got %d", len(messages))
		}

		filter.Close()
	})
}
