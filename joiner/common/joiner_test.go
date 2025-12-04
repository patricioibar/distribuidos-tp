package common_test

import (
	"fmt"
	"joiner/common"
	"os"
	"testing"

	"github.com/patricioibar/distribuidos-tp/bitmap"
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

func removePersistenceFolder(folderName string) {
	stateDir := common.StateRoot + "/" + folderName
	os.RemoveAll(stateDir)
}

var endSignal, _ = ic.NewEndSignal(nil, 0).Marshal()

func TestJoinMultipleRows(t *testing.T) {
	removePersistenceFolder("test1")
	leftColumns := []string{"id", "name"}
	leftRows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
		{4, "Diana"},
		{5, "Eve"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]interface{}{
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

	expectedJoinedRows := [][]interface{}{
		{1, "Alice", "HR"},
		{2, "Bob", "Engineering"},
		{5, "Eve", "Finance"},
	}

	leftBatch := ic.NewRowsBatch(leftColumns, leftRows, 0)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows, 0)

	leftBatchBytes, _ := leftBatch.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output, "test1", nil)

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

	joinedBatch, err := rowsBatchFromString(string(output.sentMessages[0]))
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

	var lastMsg ic.Message
	err = lastMsg.Unmarshal(output.sentMessages[1])
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if !isEndSignal(&lastMsg) {
		t.Fatalf("Expected end signal in last message")
	}
}

func isEndSignal(msg *ic.Message) bool {
	return msg.Type == ic.MsgEndSignal
}

func sequenceSetFromString(s string) (*ic.SequenceSetPayload, error) {
	var msg ic.Message
	if err := msg.Unmarshal([]byte(s)); err != nil {
		return nil, err
	}
	payload, ok := msg.Payload.(*ic.SequenceSetPayload)
	if !ok {
		return nil, fmt.Errorf("expected SequenceSetPayload, got %T", msg.Payload)
	}
	return payload, nil
}

func rowsBatchFromString(s string) (*ic.RowsBatchPayload, error) {
	var msg ic.Message
	if err := msg.Unmarshal([]byte(s)); err != nil {
		return nil, err
	}
	payload, ok := msg.Payload.(*ic.RowsBatchPayload)
	if !ok {
		return nil, fmt.Errorf("expected RowsBatchPayload, got %T", msg.Payload)
	}
	return payload, nil
}

func TestJoinNoMatches(t *testing.T) {
	removePersistenceFolder("test2")
	leftColumns := []string{"id", "name"}
	leftRows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]interface{}{
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

	leftBatch := ic.NewRowsBatch(leftColumns, leftRows, 0)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows, 0)

	leftBatchBytes, _ := leftBatch.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output, "test2", nil)

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
		t.Fatalf("Expected 2 message sent, got %d", len(output.sentMessages))
	}

	msg, err := sequenceSetFromString(string(output.sentMessages[0]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if msg.Sequences.GetCardinality() != 1 {
		t.Fatalf("Expected cardinality 1 in sequence set, got %d", msg.Sequences.GetCardinality())
	}

	var lastMsg ic.Message
	err = lastMsg.Unmarshal(output.sentMessages[1])
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if !isEndSignal(&lastMsg) {
		t.Fatalf("Expected end signal in last message")
	}
}

func TestJoinMultipleBatches(t *testing.T) {
	removePersistenceFolder("test3")
	leftColumns := []string{"name", "id"}
	leftRows1 := [][]interface{}{
		{"Alice", 1},
		{"Bob", 2},
	}
	leftRows2 := [][]interface{}{
		{"Charlie", 3},
		{"Diana", 4},
	}
	leftRows3 := [][]interface{}{
		{"Rudolph", 33},
	}
	rightColumns := []string{"id", "department", "trassh"}
	rightRows := [][]interface{}{
		{1, "HR", "Trash"},
		{2, "Engineering", "Trash"},
		{3, "Marketing", "Trash"},
		{4, "Finance", "Trash"},
		{5, "Sales", "Trash"},
	}

	config := &common.Config{
		WorkerId:      "worker-1",
		WorkersCount:  1,
		JoinKey:       "id",
		OutputColumns: []string{"name", "department"},
	}

	expectedJoinedRows := map[string]string{
		"Alice":   "HR",
		"Bob":     "Engineering",
		"Charlie": "Marketing",
		"Diana":   "Finance",
	}

	leftBatch1 := ic.NewRowsBatch(leftColumns, leftRows1, 0)
	leftBatch2 := ic.NewRowsBatch(leftColumns, leftRows2, 1)
	leftBatch3 := ic.NewRowsBatch(leftColumns, leftRows3, 2)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows, 0)

	leftBatchBytes1, _ := leftBatch1.Marshal()
	leftBatchBytes2, _ := leftBatch2.Marshal()
	leftBatchBytes3, _ := leftBatch3.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	leftInput := newStubConsumer()
	rightInput := newStubConsumer()
	output := newStubProducer()

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output, "test3", nil)

	go func() { joiner.Start() }()

	rightInput.waitForStart()
	rightInput.SimulateMessage(rightBatchBytes)
	rightInput.SimulateMessage(endSignal)

	leftInput.waitForStart()
	leftInput.SimulateMessage(leftBatchBytes1)
	leftInput.SimulateMessage(leftBatchBytes2)
	leftInput.SimulateMessage(leftBatchBytes3)
	leftInput.SimulateMessage(endSignal)

	output.waitForAMessage()
	output.waitForAMessage()
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 4 {
		t.Fatalf("Expected 4 messages sent, got %d", len(output.sentMessages))
	}

	for i := 0; i < 2; i++ {
		joinedBatch, err := rowsBatchFromString(string(output.sentMessages[i]))
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

	msg, err := sequenceSetFromString(string(output.sentMessages[2]))
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if msg.Sequences.GetCardinality() != 1 {
		t.Fatalf("Expected cardinality 1 in sequence set, got %d", msg.Sequences.GetCardinality())
	}
	var lastMsg ic.Message
	err = lastMsg.Unmarshal(output.sentMessages[3])
	if err != nil {
		t.Fatalf("Failed to unmarshal output message: %s", err)
	}
	if !isEndSignal(&lastMsg) {
		t.Fatalf("Expected end signal in last message")
	}
}

func TestMultipleJoiners(t *testing.T) {
	removePersistenceFolder("test4-w1")
	removePersistenceFolder("test4-w2")
	leftColumns := []string{"id", "name"}
	leftRows1 := [][]interface{}{
		{1, "Alice"},
		{6, "Frank"},
	}
	leftRows2 := [][]interface{}{
		{5, "Eve"},
		{2, "Bob"},
	}
	leftRows3 := [][]interface{}{
		{3, "Charlie"},
		{4, "Diana"},
	}
	rightColumns := []string{"id", "department"}
	rightRows := [][]interface{}{
		{1, "HR"},
		{2, "Engineering"},
		{3, "Marketing"},
		{4, "Finance"},
	}

	config1 := &common.Config{
		WorkerId:      "worker-1",
		WorkersCount:  2,
		JoinKey:       "id",
		OutputColumns: []string{"name", "department"},
		BatchSize:     3,
	}
	config2 := &common.Config{
		WorkerId:      "worker-2",
		WorkersCount:  2,
		JoinKey:       "id",
		OutputColumns: []string{"name", "department"},
		BatchSize:     3,
	}

	expectedJoinedRows := map[string]string{
		"Alice":   "HR",
		"Bob":     "Engineering",
		"Charlie": "Marketing",
		"Diana":   "Finance",
	}

	leftBatch1 := ic.NewRowsBatch(leftColumns, leftRows1, 0)
	leftBatch2 := ic.NewRowsBatch(leftColumns, leftRows2, 1)
	leftBatch3 := ic.NewRowsBatch(leftColumns, leftRows3, 2)
	rightBatch := ic.NewRowsBatch(rightColumns, rightRows, 0)

	leftBatchBytes1, _ := leftBatch1.Marshal()
	leftBatchBytes2, _ := leftBatch2.Marshal()
	leftBatchBytes3, _ := leftBatch3.Marshal()
	rightBatchBytes, _ := rightBatch.Marshal()

	// round robin on left input, replicated data in right input
	leftInput := newStubConsumer()
	right1Input := newStubConsumer()
	right2Input := newStubConsumer()
	output := newStubProducer()

	joiner1 := common.NewJoinerWorker(config1, leftInput, right1Input, output, "test4-w1", nil)
	joiner2 := common.NewJoinerWorker(config2, leftInput, right2Input, output, "test4-w2", nil)

	go func() { joiner1.Start() }()
	go func() { joiner2.Start() }()

	right1Input.waitForStart()
	right2Input.waitForStart()
	right1Input.SimulateMessage(rightBatchBytes)
	right1Input.SimulateMessage(endSignal)
	right2Input.SimulateMessage(rightBatchBytes)
	right2Input.SimulateMessage(endSignal)

	leftInput.waitForStart()
	leftInput.SimulateMessage(leftBatchBytes1)
	leftInput.SimulateMessage(leftBatchBytes2)
	leftInput.SimulateMessage(leftBatchBytes3)
	leftInput.SimulateMessage(endSignal)

	output.waitForAMessage()
	output.waitForAMessage()
	output.waitForAMessage()
	output.waitForAMessage()

	if len(output.sentMessages) != 4 {
		t.Fatalf("Expected 4 messages sent, got %d", len(output.sentMessages))
	}

	endSignalCount := 0
	ackedSequences := bitmap.New()
	for i := 0; i < len(output.sentMessages); i++ {
		var msg ic.Message
		err := msg.Unmarshal(output.sentMessages[i])
		if err != nil {
			t.Fatalf("Failed to unmarshal output message: %s", err)
		}
		switch p := msg.Payload.(type) {
		case *ic.RowsBatchPayload:
			for _, row := range p.Rows {
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
		case *ic.SequenceSetPayload:
			ackedSequences.Or(p.Sequences.Bitmap)
		case *ic.EndSignalPayload:
			endSignalCount++
		default:
			t.Errorf("Unexpected payload type %T in output", msg.Payload)
		}
	}

	if len(expectedJoinedRows) != 0 {
		t.Errorf("Some expected rows were not found in output: %v", expectedJoinedRows)
	}

	if ackedSequences.GetCardinality() != 0 {
		t.Errorf("Expected cardinality 0 in sequence set, got %d", ackedSequences.GetCardinality())
	}

	if endSignalCount != 1 {
		t.Errorf("Expected 1 end signals, got %d", endSignalCount)
	}
}

func TestJoinersDuplicateEndSignal(t *testing.T) {
	numOfWorkers := 3
	for i := 1; i <= numOfWorkers; i++ {
		removePersistenceFolder(fmt.Sprintf("test5-w%d", i))
	}
	leftInput := newStubConsumer()
	output := newStubProducer()

	waitToEnd := make([]chan struct{}, numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		rightInput := newStubConsumer()
		config := &common.Config{
			WorkerId:      fmt.Sprintf("worker-%d", i+1),
			WorkersCount:  numOfWorkers,
			JoinKey:       "id",
			OutputColumns: []string{"id", "name"},
		}

		waitToEnd[i] = make(chan struct{}, 1)
		joiner := common.NewJoinerWorker(config, leftInput, rightInput, output, fmt.Sprintf("test5-w%d", i+1), make(chan string, 1))
		go func(i int, j *common.JoinerWorker) {
			j.Start()
			waitToEnd[i] <- struct{}{}
		}(i, joiner)

		// ensure right input started for this joiner
		rightInput.waitForStart()

		// close right input so joiner proceeds to left input
		rightInput.SimulateMessage(endSignal)
	}

	// Wait for both joiners to register their left input consumers
	leftInput.waitForStart()
	leftInput.waitForStart()

	// send duplicate end signals on left input
	leftInput.SimulateMessage(endSignal)
	leftInput.SimulateMessage(endSignal)

	// wait for workers to finish
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
		_, ok := outputMsg.Payload.(*ic.EndSignalPayload)
		if !ok {
			t.Fatalf("Failed to cast payload to *ic.EndSignalPayload, got: %T", outputMsg.Payload)
		}
		endSignalCount++
	}

	if endSignalCount == 0 {
		t.Fatalf("No end signal messages were sent by the workers")
	}
}
