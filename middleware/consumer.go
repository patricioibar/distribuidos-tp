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

	quit    chan struct{} // para señalar al goroutine que pare
	deleted chan struct{} // para señalar que se eliminó la cola
	done    chan struct{} // para esperar a que termine el goroutine

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
		key+"###"+consumerName, // name
		false,                  // durable
		false,                  // delete when unused (no auto-delete)
		false,                  // exclusive
		false,                  // no-wait
		nil,                    // arguments
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
		deleted:    make(chan struct{}),
		done:       make(chan struct{}),
	}, nil
}

// ResumeConsumer attempts to create a Consumer structure for an existing queue
// but will NOT create the queue. If the named queue does not exist, it returns
// (nil, nil).
func ResumeConsumer(consumerName string, sourceName string, connectionAddr string, keys ...string) (*Consumer, error) {
	ch, err := GetConnection(connectionAddr).Channel()
	if err != nil {
		return nil, err
	}

	key := ""
	if len(keys) > 0 {
		key = keys[0]
	}

	qName := key + "###" + consumerName

	// Inspect the queue without creating it. If it does not exist, return (nil, nil).
	// QueueInspect is deprecated; use QueueDeclarePassive (equivalent to passive QueueDeclare).
	_, err = ch.QueueDeclarePassive(qName, false, false, false, false, nil)
	if err != nil {
		// If the error is a NOT_FOUND from the server, return (nil, nil).
		amqp.Logger.Printf("Error inspecting queue %s: %v", qName, err)
		if amqpErr, ok := err.(*amqp.Error); ok && amqpErr.Code == 404 {
			_ = ch.Close()
			return nil, nil
		}

		// Other errors (e.g., connection issues) should be returned.
		_ = ch.Close()
		return nil, err
	}

	// The queue exists. Create the Consumer structure but do not re-declare the queue
	// or exchange; bindings are assumed to be already present.
	return &Consumer{
		name:       qName,
		sourceName: sourceName,
		channel:    ch,
		quit:       make(chan struct{}),
		deleted:    make(chan struct{}),
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

	})

	if startErr != nil {
		return &MessageMiddlewareError{Code: MessageMiddlewareMessageError, Msg: "Failed consuming: " + startErr.Error()}
	}

	defer close(c.done)
	for {
		select {
		case <-c.quit:
			// señal de cierre desde StopConsuming
			return nil
		case d, ok := <-c.deliveries:
			if !ok {
				// canal cerrado por el server o por Cancel
				return nil
			}

			ret := make(chan *MessageMiddlewareError, 1)
			onMessageCallback(MiddlewareMessage{Body: d.Body, Headers: d.Headers}, ret)

			select {
			case <-c.deleted:
				return nil
			case err := <-ret:
				if err != nil {
					_ = d.Nack(false, true) // requeue
				} else {
					_ = d.Ack(false)
				}
			}
		}
	}
}

func (c *Consumer) StopConsuming() *MessageMiddlewareError {

	c.closeOnce.Do(func() {
		if c.quit != nil {
			close(c.quit) // señal al goroutine que pare
		}

		// If StartConsuming was never called, the goroutine that closes c.done
		// was never started and <-c.done would block forever. In that case we
		// detect that no deliveries channel was set and skip waiting. If the
		// deliveries channel is set we wait for the goroutine to finish.
		if c.deliveries != nil {
			<-c.done // esperar a que termine
		}
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
	var firstErr *MessageMiddlewareError

	if c.channel != nil {
		close(c.deleted)
		_, err := c.channel.QueueDelete(c.name, false, false, false)
		if err != nil {
			firstErr = &MessageMiddlewareError{Code: MessageMiddlewareDeleteError, Msg: "Failed to delete queue: " + err.Error()}
		}
		if err := c.channel.Close(); err != nil {
			if firstErr == nil {
				firstErr = &MessageMiddlewareError{Code: MessageMiddlewareCloseError, Msg: "Failed to close channel: " + err.Error()}
			} else {
				// append info about close failure
				firstErr.Msg = firstErr.Msg + "; failed to close channel: " + err.Error()
			}
		}
	}

	return firstErr
}
