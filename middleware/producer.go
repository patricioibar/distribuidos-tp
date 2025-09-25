package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	name    string
	channel *amqp.Channel
}

func NewProducer(name string) (*Producer, error) {
	ch, err := GetConnection("amqp://guest:guest@localhost:5672/").Channel()

	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		name,
		"fanout", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return nil, err
	}

	return &Producer{name: name, channel: ch}, nil
}

func (p *Producer) StartConsuming(onMessageCallback OnMessageCallback) (error *MessageMiddlewareError) {
	return &MessageMiddlewareError{Code: MessageMiddlewareProducerCannotConsumeError, Msg: "Producer cannot consume messages"}
}

func (p *Producer) StopConsuming() (error *MessageMiddlewareError) {
	return &MessageMiddlewareError{Code: MessageMiddlewareProducerCannotConsumeError, Msg: "Producer cannot consume messages"}
}

func (p *Producer) Send(message []byte) (error *MessageMiddlewareError) {
	err := p.channel.Publish(
		p.name,
		"",
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
