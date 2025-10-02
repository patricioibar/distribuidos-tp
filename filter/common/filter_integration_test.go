package filter

import (
	"encoding/json"
	"testing"
	"time"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func createTestTimestamp(year, month, day, hour, minute, second int) string {
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	return t.Format("2006-01-02 15:04:05")
}

func TestIntegrationParseTimestamp(t *testing.T) {
tests := []struct {
name      string
input     string
expectErr bool
checkYear int
}{
{
name:      "Valid 2024 timestamp",
input:     "2024-03-15 10:30:45",
expectErr: false,
checkYear: 2024,
},
{
name:      "Valid 2025 timestamp",
input:     "2025-12-31 23:59:59",
expectErr: false,
checkYear: 2025,
},
{
name:      "Invalid format with slashes",
input:     "2024/03/15 10:30:45",
expectErr: true,
},
{
name:      "Empty string",
input:     "",
expectErr: true,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
result, err := parseTimestamp(tt.input)
if (err != nil) != tt.expectErr {
t.Errorf("parseTimestamp() error = %v, expectErr %v", err, tt.expectErr)
return
}

if !tt.expectErr && tt.checkYear != 0 {
if result.Year() != tt.checkYear {
t.Errorf("Expected year %d, got %d", tt.checkYear, result.Year())
}
}
})
}
}

func TestIntegrationFilterRowsByYear(t *testing.T) {
batch := ic.RowsBatch{
ColumnNames: []string{"id", "year", "amount"},
Rows: [][]interface{}{
{1, createTestTimestamp(2024, 3, 15, 10, 0, 0), 100.0},
{2, createTestTimestamp(2023, 8, 20, 14, 30, 0), 150.0}, // Should be filtered out
{3, createTestTimestamp(2024, 8, 20, 14, 30, 0), 200.0},
},
}

result, err := filterRowsByYear(batch)
if err != nil {
t.Fatalf("filterRowsByYear() error = %v", err)
}

// Should have 2 rows (filtering out 2023 data)
if len(result.Rows) != 2 {
t.Errorf("Expected 2 rows, got %d", len(result.Rows))
}

// Should have added semester column
if len(result.ColumnNames) != 4 {
t.Errorf("Expected 4 columns (including semester), got %d", len(result.ColumnNames))
}

if result.ColumnNames[len(result.ColumnNames)-1] != "semester" {
t.Error("Expected last column to be 'semester'")
}
}

func TestIntegrationFilterRowsByHour(t *testing.T) {
batch := ic.RowsBatch{
ColumnNames: []string{"id", "timestamp", "amount"},
Rows: [][]interface{}{
{1, createTestTimestamp(2024, 3, 15, 8, 0, 0), 100.0},   // Should pass (8 AM)
{2, createTestTimestamp(2024, 3, 15, 2, 0, 0), 150.0},   // Should not pass (2 AM)
{3, createTestTimestamp(2024, 3, 15, 22, 30, 0), 200.0}, // Should pass (10:30 PM)
{4, createTestTimestamp(2024, 3, 15, 23, 30, 0), 250.0}, // Should not pass (11:30 PM)
},
}

result, err := filterRowsByHour(batch)
if err != nil {
t.Fatalf("filterRowsByHour() error = %v", err)
}

if len(result.Rows) != 2 {
t.Errorf("Expected 2 rows, got %d", len(result.Rows))
}
}

func TestIntegrationFilterRowsByTransactionAmount(t *testing.T) {
batch := ic.RowsBatch{
ColumnNames: []string{"id", "final_amount", "category"},
Rows: [][]interface{}{
{1, 50.0, "coffee"},  // Should not pass
{2, 75.0, "coffee"},  // Should pass
{3, 100.0, "coffee"}, // Should pass
{4, 74.99, "coffee"}, // Should not pass
},
}

result, err := filterRowsByTransactionAmount(batch)
if err != nil {
t.Fatalf("filterRowsByTransactionAmount() error = %v", err)
}

if len(result.Rows) != 2 {
t.Errorf("Expected 2 rows, got %d", len(result.Rows))
}
}

func TestIntegrationFilterTransactionItemsByYear(t *testing.T) {
	batch := ic.RowsBatch{
		ColumnNames: []string{"id", "year", "item"},
		Rows: [][]interface{}{
			{1, createTestTimestamp(2023, 3, 15, 10, 0, 0), "item1"}, // Should not pass
			{2, createTestTimestamp(2024, 3, 15, 10, 0, 0), "item2"}, // Should pass
			{3, createTestTimestamp(2025, 8, 20, 14, 0, 0), "item3"}, // Should pass
			{4, createTestTimestamp(2026, 1, 1, 12, 0, 0), "item4"},  // Should not pass
		},
	}

	result, err := filterTransactionItemsByYear(batch)
	if err != nil {
		t.Fatalf("filterTransactionItemsByYear() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
}

// Helper functions for middleware integration testing  
func addMessageToMock(m *MockMiddleware, batch ic.RowsBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	m.SimulateMessage(data)
	return nil
}

// Integration tests with middleware mocks
func TestMiddlewareIntegrationNewFilter(t *testing.T) {
	inputMock := &MockMiddleware{}
	outputMock := &MockMiddleware{}

	tests := []struct {
		name       string
		workerID   string
		filterType string
		expectNil  bool
	}{
		{
			name:       "Valid TbyYear filter",
			workerID:   "worker1",
			filterType: "TbyYear",
			expectNil:  false,
		},
		{
			name:       "Valid TbyHour filter",
			workerID:   "worker2", 
			filterType: "TbyHour",
			expectNil:  false,
		},
		{
			name:       "Valid TbyAmount filter",
			workerID:   "worker3",
			filterType: "TbyAmount",
			expectNil:  false,
		},
		{
			name:       "Valid TIbyYear filter",
			workerID:   "worker4",
			filterType: "TIbyYear",
			expectNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := NewFilter(tt.workerID, inputMock, outputMock, tt.filterType)
			if worker == nil {
				t.Error("NewFilter() returned nil for valid filter type")
			}

			if worker.filterId != tt.workerID {
				t.Errorf("Expected filterId %s, got %s", tt.workerID, worker.filterId)
			}
		})
	}
}

func TestMiddlewareIntegrationFilterWorkflow(t *testing.T) {
	tests := []struct {
		name            string
		filterType      string
		inputBatch      ic.RowsBatch
		expectedRows    int
		expectedColumns int
		checkLastColumn string
	}{
		{
			name:       "TbyYear filter workflow",
			filterType: "TbyYear",
			inputBatch: ic.RowsBatch{
				ColumnNames: []string{"id", "year", "amount"},
				Rows: [][]interface{}{
					{1, createTestTimestamp(2024, 3, 15, 10, 0, 0), 100.0},
					{2, createTestTimestamp(2023, 8, 20, 14, 30, 0), 150.0}, // Should be filtered out
					{3, createTestTimestamp(2024, 8, 20, 14, 30, 0), 200.0},
				},
			},
			expectedRows:    2,
			expectedColumns: 4,
			checkLastColumn: "semester",
		},
		{
			name:       "TbyHour filter workflow",
			filterType: "TbyHour",
			inputBatch: ic.RowsBatch{
				ColumnNames: []string{"id", "timestamp", "amount"},
				Rows: [][]interface{}{
					{1, createTestTimestamp(2024, 3, 15, 8, 0, 0), 100.0},   // Should pass
					{2, createTestTimestamp(2024, 3, 15, 2, 0, 0), 150.0},   // Should not pass
					{3, createTestTimestamp(2024, 3, 15, 22, 30, 0), 200.0}, // Should pass
				},
			},
			expectedRows:    2,
			expectedColumns: 3,
		},
		{
			name:       "TbyAmount filter workflow",
			filterType: "TbyAmount",
			inputBatch: ic.RowsBatch{
				ColumnNames: []string{"id", "final_amount", "category"},
				Rows: [][]interface{}{
					{1, 50.0, "coffee"},  // Should not pass
					{2, 75.0, "coffee"},  // Should pass
					{3, 100.0, "coffee"}, // Should pass
				},
			},
			expectedRows:    2,
			expectedColumns: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputMock := &MockMiddleware{}
			outputMock := &MockMiddleware{}

			// Create filter worker
			worker := NewFilter("worker1", inputMock, outputMock, tt.filterType)
			if worker == nil {
				t.Fatal("Failed to create FilterWorker")
			}

			// Start the worker in a goroutine
			go worker.Start()

			// Give time for setup
			time.Sleep(10 * time.Millisecond)

			// Now send the test message
			err := addMessageToMock(inputMock, tt.inputBatch)
			if err != nil {
				t.Fatalf("Failed to simulate message: %v", err)
			}

			// Wait for processing
			time.Sleep(50 * time.Millisecond)

			// Stop the worker
			worker.Close()

			// Check output messages
			sentMessages := outputMock.GetMessages()
			if len(sentMessages) == 0 {
				t.Error("No messages were sent to output")
				return
			}

			// Unmarshal and verify the output
			var outputBatch ic.RowsBatch
			err = json.Unmarshal(sentMessages[0], &outputBatch)
			if err != nil {
				t.Fatalf("Failed to unmarshal output batch: %v", err)
			}

			if len(outputBatch.Rows) != tt.expectedRows {
				t.Errorf("Expected %d filtered rows, got %d", tt.expectedRows, len(outputBatch.Rows))
			}

			if len(outputBatch.ColumnNames) != tt.expectedColumns {
				t.Errorf("Expected %d columns, got %d", tt.expectedColumns, len(outputBatch.ColumnNames))
			}

			if tt.checkLastColumn != "" {
				lastCol := outputBatch.ColumnNames[len(outputBatch.ColumnNames)-1]
				if lastCol != tt.checkLastColumn {
					t.Errorf("Expected last column to be '%s', got '%s'", tt.checkLastColumn, lastCol)
				}
			}
		})
	}
}

func TestMiddlewareIntegrationEndSignalHandling(t *testing.T) {
	inputMock := &MockMiddleware{}
	outputMock := &MockMiddleware{}

	// Create filter worker (worker1)
	worker := NewFilter("worker1", inputMock, outputMock, "TbyYear")
	if worker == nil {
		t.Fatal("Failed to create FilterWorker")
	}

	// Start the worker
	go worker.Start()

	// Give time for setup
	time.Sleep(10 * time.Millisecond)

	// Create end signal batch
	endSignalBatch := ic.RowsBatch{
		EndSignal:   true,
		WorkersDone: []string{"worker2", "worker3"}, // Worker1 not done yet
		Rows:        nil,
		ColumnNames: nil,
	}

	err := addMessageToMock(inputMock, endSignalBatch)
	if err != nil {
		t.Fatalf("Failed to add end signal batch: %v", err)
	}

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// Stop the worker
	worker.Close()

	// Check that messages were sent (could be to input for coordination or output for next stage)
	inputSentMessages := inputMock.GetMessages()
	outputSentMessages := outputMock.GetMessages()

	if len(inputSentMessages) == 0 && len(outputSentMessages) == 0 {
		t.Error("No messages were sent for end signal handling")
		return
	}

	// There should be some end signal coordination happening
	t.Logf("Input messages sent: %d, Output messages sent: %d", len(inputSentMessages), len(outputSentMessages))
}

func TestMiddlewareIntegrationErrorHandling(t *testing.T) {
	t.Run("Send Error", func(t *testing.T) {
		inputMock := &MockMiddleware{}
		outputMock := &MockMiddleware{shouldFailSend: true}

		worker := NewFilter("worker1", inputMock, outputMock, "TbyYear")
		if worker == nil {
			t.Fatal("Failed to create FilterWorker")
		}

		// Start and let it process
		go worker.Start()
		time.Sleep(10 * time.Millisecond) // Setup time

		testBatch := ic.RowsBatch{
			ColumnNames: []string{"id", "year", "amount"},
			Rows: [][]interface{}{
				{1, createTestTimestamp(2024, 3, 15, 10, 0, 0), 100.0},
			},
		}

		err := addMessageToMock(inputMock, testBatch)
		if err != nil {
			t.Fatalf("Failed to add test batch: %v", err)
		}

		time.Sleep(50 * time.Millisecond) // Processing time
		worker.Close()

		// The worker should handle the send error gracefully
		// This test verifies the worker doesn't crash on send errors
	})

	t.Run("Consume Error", func(t *testing.T) {
		inputMock := &MockMiddleware{shouldFailConsume: true}
		outputMock := &MockMiddleware{}

		worker := NewFilter("worker1", inputMock, outputMock, "TbyYear")
		if worker == nil {
			t.Fatal("Failed to create FilterWorker")
		}

		// This should handle the consume error gracefully
		// In the real implementation, this might call log.Fatalf
		// For testing purposes, we're just verifying the worker can be created
		if worker.filterId != "worker1" {
			t.Errorf("Worker not created properly despite consume setup failure")
		}
	})
}

func TestMiddlewareIntegrationMultipleMessages(t *testing.T) {
	inputMock := &MockMiddleware{}
	outputMock := &MockMiddleware{}

	worker := NewFilter("worker1", inputMock, outputMock, "TbyYear")
	if worker == nil {
		t.Fatal("Failed to create FilterWorker")
	}

	go worker.Start()
	time.Sleep(10 * time.Millisecond) // Setup time

	// Add multiple batches after worker is ready
	batch1 := ic.RowsBatch{
		ColumnNames: []string{"id", "year", "amount"},
		Rows: [][]interface{}{
			{1, createTestTimestamp(2024, 3, 15, 10, 0, 0), 100.0},
		},
	}

	batch2 := ic.RowsBatch{
		ColumnNames: []string{"id", "year", "amount"},
		Rows: [][]interface{}{
			{2, createTestTimestamp(2024, 8, 20, 14, 0, 0), 200.0},
		},
	}

	err := addMessageToMock(inputMock, batch1)
	if err != nil {
		t.Fatalf("Failed to add batch1: %v", err)
	}

	err = addMessageToMock(inputMock, batch2)
	if err != nil {
		t.Fatalf("Failed to add batch2: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // Processing time for multiple messages
	worker.Close()

	sentMessages := outputMock.GetMessages()
	if len(sentMessages) != 2 {
		t.Errorf("Expected 2 output messages, got %d", len(sentMessages))
	}

	// Verify both messages were processed correctly
	for i, msgData := range sentMessages {
		var outputBatch ic.RowsBatch
		err := json.Unmarshal(msgData, &outputBatch)
		if err != nil {
			t.Fatalf("Failed to unmarshal message %d: %v", i, err)
		}

		if len(outputBatch.Rows) != 1 {
			t.Errorf("Message %d: Expected 1 row, got %d", i, len(outputBatch.Rows))
		}

		if len(outputBatch.ColumnNames) != 4 {
			t.Errorf("Message %d: Expected 4 columns (including semester), got %d", i, len(outputBatch.ColumnNames))
		}
	}
}