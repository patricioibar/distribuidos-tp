package middleware

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	name       string
	sourceName string
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery

	quit chan struct{} // para señalar al goroutine que pare
	done chan struct{} // para esperar a que termine el goroutine

	startOnce sync.Once
	closeOnce sync.Once
}

func NewConsumer(consumerName string, sourceName string, connectionAddr string, keys ...string) (*Consumer, error) {
	ch, err := GetConnection(connectionAddr).Channel()
	if err != nil {
		return nil, err
	}

	key := ""
	if len(keys) > 0 {
		key = keys[0]
	}

	q, err := ch.QueueDeclare(
		consumerName+key, // name
		false,            // durable
		true,             // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		sourceName,
		"direct", // type
		false,    // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,     // queue name
		key,        // routing key
		sourceName, // exchange name
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	return &Consumer{
		name:       q.Name,
		sourceName: sourceName,
		channel:    ch,
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
	}, nil
}

func (c *Consumer) StartConsuming(onMessageCallback OnMessageCallback) *MessageMiddlewareError {
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

					ret := make(chan *MessageMiddlewareError, 1)
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

	c.closeOnce.Do(func() {
		close(c.quit) // señal al goroutine que pare
		<-c.done      // esperar a que termine
	})

	return nil
}

func (c *Consumer) Send(message []byte) (error *MessageMiddlewareError) {
	err := c.channel.Publish(
		"",
		c.name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		return &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Failed to send message"}
	}

	return nil
}

func (c *Consumer) Close() (error *MessageMiddlewareError) {
	c.StopConsuming()

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			return &MessageMiddlewareError{Code: MessageMiddlewareCloseError, Msg: "Failed to close channel: " + err.Error()}
		}
	}
	return nil
}

func (c *Consumer) Delete() (error *MessageMiddlewareError) {
	c.StopConsuming()

	if c.channel != nil {
		_, err := c.channel.QueueDelete(c.name, false, false, false)
		if err != nil {
			return &MessageMiddlewareError{Code: MessageMiddlewareDeleteError, Msg: "Failed to delete queue: " + err.Error()}
		}
	}
	return nil
}
