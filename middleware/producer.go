package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	name    string
	channel *amqp.Channel
	key     string
}

func NewProducer(name string, connectionAddr string, keys ...string) (*Producer, error) {
	ch, err := GetConnection(connectionAddr).Channel()

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

	return &Producer{name: name, channel: ch, key: key}, nil
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
		p.key,
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
