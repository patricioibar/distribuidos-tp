package middleware

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	name       string
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery

	quit chan struct{} // para señalar al goroutine que pare
	done chan struct{} // para esperar a que termine el goroutine

	startOnce sync.Once
}

func NewConsumer(name, exchangeName string) (*Consumer, error) {
	ch, err := GetConnection("amqp://guest:guest@localhost:5672/").Channel()
	if err != nil {
		return nil, err
	}

	// Declarar la queue con el nombre provisto
	_, err = ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	err = ch.QueueBind(
		name,         // queue name
		"",           // routing key — ignored for fanout
		exchangeName, // exchange name
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	return &Consumer{
		name:    name,
		channel: ch,
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}, nil
}

func (c *Consumer) StartConsuming(onMessageCallback onMessageCallback) *MessageMiddlewareError {
	var startErr error

	c.startOnce.Do(func() {
		deliveries, err := c.channel.Consume(
			c.name, // queue
			"",     // consumer
			false,  // autoAck
			false,  // exclusive
			false,  // noLocal
			false,  // noWait
			nil,    // args
		)
		if err != nil {
			startErr = err
			return
		}

		c.deliveries = deliveries

		go func() {
			defer close(c.done)
			for {
				select {
				case d, ok := <-c.deliveries:
					if !ok {
						// canal cerrado por el server o por Cancel
						return
					}

					ret := make(chan *MessageMiddlewareError)
					onMessageCallback(MiddlewareMessage{Body: d.Body, Headers: d.Headers}, ret)
					err := <-ret
					if err != nil {
						_ = d.Nack(false, true) // requeue
					} else {
						_ = d.Ack(false)
					}
				case <-c.quit:
					// señal de cierre desde StopConsuming
					return
				}
			}
		}()
	})

	if startErr != nil {
		return &MessageMiddlewareError{Code: MessageMiddlewareMessageError, Msg: "Failed to start consuming: " + startErr.Error()}
	}

	return nil
}

func (c *Consumer) StopConsuming() *MessageMiddlewareError {

	close(c.quit) // señal al goroutine que pare
	<-c.done      // esperar a que termine

	return nil
}

func (c *Consumer) Send(message []byte) (error *MessageMiddlewareError) {
	return &MessageMiddlewareError{Code: MessageMiddlewareConsumerCannotSendError, Msg: "Consumer cannot send messages"}
}

func (c *Consumer) Close() (error *MessageMiddlewareError) {
	_ = c.StopConsuming()

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			return &MessageMiddlewareError{Code: MessageMiddlewareCloseError, Msg: "Failed to close channel: " + err.Error()}
		}
	}
	return nil
}

func (c *Consumer) Delete() (error *MessageMiddlewareError) {
	_ = c.StopConsuming()

	if c.channel != nil {
		_, err := c.channel.QueueDelete(c.name, false, false, false)
		if err != nil {
			return &MessageMiddlewareError{Code: MessageMiddlewareDeleteError, Msg: "Failed to delete queue: " + err.Error()}
		}
	}
	return nil
}
