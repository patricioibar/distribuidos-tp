package middleware

import (
	"fmt"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var logg = logging.MustGetLogger("log")

type Producer struct {
	name     string
	channel  *amqp.Channel
	key      string
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
	closed   chan *amqp.Error
}

func NewProducer(name string, connectionAddr string, keys ...string) (*Producer, error) {
	conn := GetConnection(connectionAddr)
	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		name,
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return nil, err
	}

	key := ""
	if len(keys) > 0 {
		key = keys[0]
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("confirm mode: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1000))
	returns := ch.NotifyReturn(make(chan amqp.Return, 100))
	closed := make(chan *amqp.Error, 1)
	conn.NotifyClose(closed)

	p := &Producer{
		name:     name,
		channel:  ch,
		key:      key,
		confirms: confirms,
		returns:  returns,
		closed:   closed,
	}

	// log if anything goes wrong
	go func() {
		for {
			select {
			case ret, ok := <-p.returns:
				if !ok {
					return
				}
				logg.Errorf("[publisher] returned msg: replyCode=%d replyText=%q exchange=%s routingKey=%s bodyLen=%d",
					ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey, len(ret.Body))
			case err := <-p.closed:
				if err != nil {
					logg.Errorf("[publisher] connection closed: %v", err)
				} else {
					logg.Errorf("[publisher] connection closed (nil error)")
				}
				return
			}
		}
	}()

	return p, nil
}

func (p *Producer) StartConsuming(onMessageCallback OnMessageCallback) (error *MessageMiddlewareError) {
	return &MessageMiddlewareError{Code: MessageMiddlewareProducerCannotConsumeError, Msg: "Producer cannot consume messages"}
}

func (p *Producer) StopConsuming() (error *MessageMiddlewareError) {
	return &MessageMiddlewareError{Code: MessageMiddlewareProducerCannotConsumeError, Msg: "Producer cannot consume messages"}
}

func (p *Producer) Send(message []byte) *MessageMiddlewareError {
	type publishResult struct {
		err error
	}
	pr := make(chan publishResult, 1)

	go func() {
		err := p.channel.Publish(
			p.name,
			p.key,
			true,  // mandatory
			false, // immediate deprecated
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         message,
				Timestamp:    time.Now(),
			},
		)
		pr <- publishResult{err: err}
	}()

	select {
	case res := <-pr:
		if res.err != nil {
			_ = p.channel.Close()
			return &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Failed to send message"}
		}
	case <-time.After(5 * time.Second):
		_ = p.channel.Close()
		return &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Timeout during Publish call"}
	}

	select {
	case conf, ok := <-p.confirms:
		if !ok {
			_ = p.channel.Close()
			return &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Confirms channel closed"}
		}
		if conf.Ack {
			return nil
		}
		return &MessageMiddlewareError{Code: MessageMiddlewareSendError, Msg: "Message NACKed by broker"}
	case <-time.After(5 * time.Second):
		_ = p.channel.Close()
		return &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Timeout waiting for publisher confirm"}
	}
}

func (p *Producer) Close() (error *MessageMiddlewareError) {
	err := p.channel.Close()
	if err != nil {
		return &MessageMiddlewareError{Code: MessageMiddlewareCloseError, Msg: "Failed to close channel"}
	}
	return nil
}

func (p *Producer) Delete() (error *MessageMiddlewareError) {
	err := p.channel.ExchangeDelete(p.name, false, false)
	if err != nil {
		return &MessageMiddlewareError{Code: MessageMiddlewareDeleteError, Msg: "Failed to delete exchange"}
	}
	return nil
}
