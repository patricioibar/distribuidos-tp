package common_test

import (
	"joiner/common"
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
	onMessages []mw.OnMessageCallback
	lastCalled int
	started    chan struct{}
}

func newStubConsumer() *StubConsumer {
	return &StubConsumer{
		started:    make(chan struct{}, 10),
		lastCalled: 0,
	}
}

func (s *StubConsumer) waitForStart() {
	<-s.started
}

func (s *StubConsumer) StartConsuming(
	onMessageCallback mw.OnMessageCallback,
) (error *mw.MessageMiddlewareError) {
	println("StubConsumer started")
	s.onMessages = append(s.onMessages, onMessageCallback)
	s.started <- struct{}{}
	return nil
}

func (s *StubConsumer) InjectMessage(
	message []byte,
	doneChan chan *mw.MessageMiddlewareError,
) {
	calling := s.lastCalled
	s.lastCalled = (s.lastCalled + 1) % len(s.onMessages)
	s.onMessages[calling](mw.MiddlewareMessage{Body: message}, doneChan)
}

func (s *StubConsumer) SimulateMessage(message []byte) {
	println("Simulating message %v", string(message))
	doneChan := make(chan *mw.MessageMiddlewareError)
	go func() { <-doneChan }()
	s.InjectMessage(message, doneChan)
}

func (s *StubConsumer) Send(message []byte) (error *mw.MessageMiddlewareError) {
	s.SimulateMessage(message)
	return nil
}

func (s *StubConsumer) StopConsuming() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Close() (error *mw.MessageMiddlewareError) { return nil }

func (s *StubConsumer) Delete() (error *mw.MessageMiddlewareError) { return nil }

var endSignal, _ = ic.NewEndSignal().Marshal()

func TestJoinMultipleRows(t *testing.T) {
	leftColumns := []string{"id", "name"}
	leftRows := [][]any{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "Diana"},
		{5, "Eve"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]any{
		{1, "HR"},
		{2, "Engineering"},
		{7, "Marketing"},
		{5, "Finance"},
		{8, "Sales"},
	}

	config := &common.Config{
		WorkerId:      "worker-1",
		WorkersCount:  1,
		JoinKey:       "id",
		OutputColumns: []string{"id", "name", "department"},
		BatchSize:     10,
	}

	expectedJoinedRows := [][]any{
		{1, "Alice", "HR"},
		{2, "Bob", "Engineering"},
		{5, "Eve", "Finance"},
	}

	leftBatch := ic.NewRowsBatch(leftColumns, leftRows)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows)

	leftBatchBytes, _ := leftBatch.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output)

	go func() { joiner.Start() }()

	rightInput.waitForStart()
	rightInput.SimulateMessage(rightBatchBytes)
	rightInput.SimulateMessage(endSignal)

	leftInput.waitForStart()
	leftInput.SimulateMessage(leftBatchBytes)
	leftInput.SimulateMessage(endSignal)

	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 2 {
		t.Fatalf("Expected 2 messages sent, got %d", len(output.sentMessages))
	}

	joinedBatch, err := ic.RowsBatchFromString(string(output.sentMessages[0]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}

	if len(joinedBatch.Rows) != len(expectedJoinedRows) {
		t.Fatalf("Expected %d joined rows, got %d", len(expectedJoinedRows), len(joinedBatch.Rows))
	}

	for i, row := range joinedBatch.Rows {
		id := int(row[0].(float64))
		name := row[1].(string)
		department := row[2].(string)

		expectedRow := expectedJoinedRows[i]
		expectedId := expectedRow[0].(int)
		expectedName := expectedRow[1].(string)
		expectedDepartment := expectedRow[2].(string)

		if id != expectedId || name != expectedName || department != expectedDepartment {
			t.Errorf("Row %d mismatch: expected %v, got %v", i, expectedRow, row)
		}
	}

	lastBatch, err := ic.RowsBatchFromString(string(output.sentMessages[1]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if !lastBatch.IsEndSignal() {
		t.Fatalf("Expected end signal in last message")
	}
}

func TestJoinNoMatches(t *testing.T) {
	leftColumns := []string{"id", "name"}
	leftRows := [][]any{
		{1, "Alice"},
		{2, "Bob"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]any{
		{3, "HR"},
		{4, "Engineering"},
	}

	config := &common.Config{
		WorkerId:      "worker-1",
		WorkersCount:  1,
		JoinKey:       "id",
		OutputColumns: []string{"id", "name", "department"},
		BatchSize:     10,
	}

	leftBatch := ic.NewRowsBatch(leftColumns, leftRows)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows)

	leftBatchBytes, _ := leftBatch.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output)

	go func() { joiner.Start() }()

	rightInput.waitForStart()
	rightInput.SimulateMessage(rightBatchBytes)
	rightInput.SimulateMessage(endSignal)

	leftInput.waitForStart()
	leftInput.SimulateMessage(leftBatchBytes)
	leftInput.SimulateMessage(endSignal)

	output.waitForAMessage()

	if len(output.sentMessages) != 1 {
		t.Fatalf("Expected 1 message sent, got %d", len(output.sentMessages))
	}

	batch, err := ic.RowsBatchFromString(string(output.sentMessages[0]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}

	if len(batch.Rows) != 0 {
		t.Fatalf("Expected 0 joined rows, got %d", len(batch.Rows))
	}

	if !batch.IsEndSignal() {
		t.Fatalf("Expected end signal in last message")
	}
}

func TestJoinMultipleBatches(t *testing.T) {
	leftColumns := []string{"id", "name"}
	leftRows1 := [][]any{
		{1, "Alice"},
		{2, "Bob"},
	}
	leftRows2 := [][]any{
		{3, "Charlie"},
		{4, "Diana"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]any{
		{1, "HR"},
		{2, "Engineering"},
		{3, "Marketing"},
		{4, "Finance"},
	}

	config := &common.Config{
		WorkerId:      "worker-1",
		WorkersCount:  1,
		JoinKey:       "id",
		OutputColumns: []string{"name", "department"},
		BatchSize:     2,
	}

	expectedJoinedRows := map[string]string{
		"Alice":   "HR",
		"Bob":     "Engineering",
		"Charlie": "Marketing",
		"Diana":   "Finance",
	}

	leftBatch1 := ic.NewRowsBatch(leftColumns, leftRows1)
	leftBatch2 := ic.NewRowsBatch(leftColumns, leftRows2)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows)

	leftBatchBytes1, _ := leftBatch1.Marshal()
	leftBatchBytes2, _ := leftBatch2.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output)

	go func() { joiner.Start() }()

	rightInput.waitForStart()
	rightInput.SimulateMessage(rightBatchBytes)
	rightInput.SimulateMessage(endSignal)

	leftInput.waitForStart()
	leftInput.SimulateMessage(leftBatchBytes1)
	leftInput.SimulateMessage(leftBatchBytes2)
	leftInput.SimulateMessage(endSignal)

	output.waitForAMessage()
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 3 {
		t.Fatalf("Expected 3 messages sent, got %d", len(output.sentMessages))
	}

	for i := 0; i < 2; i++ {
		joinedBatch, err := ic.RowsBatchFromString(string(output.sentMessages[i]))
		if err != nil {
			t.Fatalf("Failed to unmarshal output message: %s", err)
		}

		if len(joinedBatch.Rows) != 2 {
			t.Fatalf("Expected 2 joined rows in batch %d, got %d", i, len(joinedBatch.Rows))
		}

		for _, row := range joinedBatch.Rows {
			name := row[0].(string)
			department := row[1].(string)

			expectedDepartment, exists := expectedJoinedRows[name]
			if !exists {
				t.Errorf("Unexpected name %s in output", name)
			} else if department != expectedDepartment {
				t.Errorf("For name %s, expected department %s, got %s", name, expectedDepartment, department)
			}
			delete(expectedJoinedRows, name)
		}
	}

	lastBatch, err := ic.RowsBatchFromString(string(output.sentMessages[2]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if !lastBatch.IsEndSignal() {
		t.Fatalf("Expected end signal in last message")
	}
}
