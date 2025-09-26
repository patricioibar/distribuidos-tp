package middleware_test

import (
	"testing"
	"time"

	"github.com/patricioibar/distribuidos-tp/middleware"

	"github.com/stretchr/testify/assert"
)

func TestProducerConsumerCommunication(t *testing.T) {
	exchangeName := "testExchange"
	//queueName := "testQueue"

	// Create Producer
	producer, err := middleware.NewProducer(exchangeName)
	assert.NoError(t, err)
	defer producer.Close()

	// Create Consumer
	consumer, err := middleware.NewConsumer(exchangeName)
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
