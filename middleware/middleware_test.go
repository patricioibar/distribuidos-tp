package middleware_test

import (
	"testing"
	"time"

	"github.com/patricioibar/distribuidos-tp/middleware"

	"github.com/stretchr/testify/assert"
)

func TestProducerConsumerCommunication(t *testing.T) {
	producerName := "producerTest1"
	consumerName := "consumerTest1"

	// Create Producer
	producer, err := middleware.NewProducer(producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer producer.Close()

	// Create Consumer
	consumer, err := middleware.NewConsumer(consumerName, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer.Close()

	// Channel to capture received message
	received := make(chan string, 1)

	// Define the callback function for the consumer
	callback := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received <- string(msg.Body)
		done <- nil
	}

	// Start consuming messages
	errConsume := consumer.StartConsuming(callback)
	assert.Nil(t, errConsume)

	// Send a test message
	testMessage := "hello from producer"
	errSend := producer.Send([]byte(testMessage))
	assert.Nil(t, errSend)

	// Wait for the message or timeout after 3 seconds
	select {
	case msg := <-received:
		assert.Equal(t, testMessage, msg)
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for message")
	}

	// Stop consuming
	errStop := consumer.StopConsuming()
	assert.Nil(t, errStop)
}

func TestOneProducerTwoConsumersRoundRobin(t *testing.T) {
	producerName := "producerTest2"
	consumerName := "consumerTest2"

	producer, err := middleware.NewProducer(producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer producer.Close()

	// to have round robin on two consumers, they must have the same name
	consumer1, err := middleware.NewConsumer(consumerName, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer1.Close()
	consumer2, err := middleware.NewConsumer(consumerName, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer2.Close()

	received1 := make(chan string, 1)
	received2 := make(chan string, 1)
	callback1 := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received1 <- string(msg.Body)
		done <- nil
	}
	callback2 := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received2 <- string(msg.Body)
		done <- nil
	}
	errConsume1 := consumer1.StartConsuming(callback1)
	assert.Nil(t, errConsume1)
	errConsume2 := consumer2.StartConsuming(callback2)
	assert.Nil(t, errConsume2)

	testMessage1 := "message 1"
	errSend1 := producer.Send([]byte(testMessage1))
	assert.Nil(t, errSend1)
	testMessage2 := "message 2"
	errSend2 := producer.Send([]byte(testMessage2))
	assert.Nil(t, errSend2)

	// Wait for the messages or timeout after 3 seconds
	var receivedMsgs []string
	for len(receivedMsgs) < 2 {
		select {
		case msg1 := <-received1:
			receivedMsgs = append(receivedMsgs, msg1)
		case msg2 := <-received2:
			receivedMsgs = append(receivedMsgs, msg2)
		case <-time.After(3 * time.Second):
			t.Fatal("Timed out waiting for messages")
		}
	}

	assert.Contains(t, receivedMsgs, testMessage1)
	assert.Contains(t, receivedMsgs, testMessage2)
}

func TestOneProducerTwoConsumersClonedMessages(t *testing.T) {
	producerName := "produceTest3"
	consumerName1 := "consumer1Test3"
	consumerName2 := "consumer2Test3"

	producer, err := middleware.NewProducer(producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer producer.Close()

	// if two consumers share the same source (producer) but have different names,
	// they will each receive a copy of each message (fanout)
	consumer1, err := middleware.NewConsumer(consumerName1, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer1.Close()
	consumer2, err := middleware.NewConsumer(consumerName2, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer2.Close()

	received1 := make(chan string, 1)
	received2 := make(chan string, 1)
	callback1 := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received1 <- string(msg.Body)
		done <- nil
	}
	callback2 := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received2 <- string(msg.Body)
		done <- nil
	}
	errConsume1 := consumer1.StartConsuming(callback1)
	assert.Nil(t, errConsume1)
	errConsume2 := consumer2.StartConsuming(callback2)
	assert.Nil(t, errConsume2)

	testMessage := "hello to both consumers"
	errSend := producer.Send([]byte(testMessage))
	assert.Nil(t, errSend)

	receivedMsgs := 0
	for receivedMsgs < 2 {
		select {
		case msg1 := <-received1:
			assert.Equal(t, testMessage, msg1)
			receivedMsgs++
		case msg2 := <-received2:
			assert.Equal(t, testMessage, msg2)
			receivedMsgs++
		case <-time.After(3 * time.Second):
			t.Fatal("Timed out waiting for messages")
		}
	}
}

func TestTwoProducersOneConsumer(t *testing.T) {
	producerName := "producerTest4"
	consumerName := "consumerTest4"

	// to have two producers and one consumer, both producers must have the same name
	producer1, err := middleware.NewProducer(producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer producer1.Close()
	producer2, err := middleware.NewProducer(producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer producer2.Close()

	consumer, err := middleware.NewConsumer(consumerName, producerName, "amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer consumer.Close()
	received := make(chan string, 2)
	callback := func(msg middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		received <- string(msg.Body)
		done <- nil
	}
	errConsume := consumer.StartConsuming(callback)
	assert.Nil(t, errConsume)

	testMessage1 := "message from producer 1"
	errSend1 := producer1.Send([]byte(testMessage1))
	assert.Nil(t, errSend1)
	testMessage2 := "message from producer 2"
	errSend2 := producer2.Send([]byte(testMessage2))
	assert.Nil(t, errSend2)

	var receivedMsgs []string
	for len(receivedMsgs) < 2 {
		select {
		case msg := <-received:
			receivedMsgs = append(receivedMsgs, msg)
		case <-time.After(3 * time.Second):
			t.Fatal("Timed out waiting for messages")
		}
	}

	assert.Contains(t, receivedMsgs, testMessage1)
	assert.Contains(t, receivedMsgs, testMessage2)
}
