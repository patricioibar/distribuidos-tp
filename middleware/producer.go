package middleware

type Producer struct {
	exchange MessageMiddlewareExchange
}

func NewProducer(name string) (*Producer, error) {
	ch, err := GetConnection("amqp://guest:guest@localhost:5672/").Channel()

	if err != nil {
		return nil, err
	}

	exchange := MessageMiddlewareExchange{
		exchangeName:   name,
		routeKeys:      []string{},
		channel:        ch,
		consumeChannel: nil,
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

	return &Producer{exchange: exchange}, nil
}

func (p *Producer) StartConsuming(onMessageCallback onMessageCallback) (error MessageMiddlewareError) {
	return MessageMiddlewareProducerCannotConsumeError
}

func (p *Producer) StopConsuming() (error MessageMiddlewareError) {
	return MessageMiddlewareProducerCannotConsumeError
}

func (p *Producer) Send(message []byte) (error MessageMiddlewareError) {

}

func (p *Producer) Close() (error MessageMiddlewareError) {

}

func (p *Producer) Delete() (error MessageMiddlewareError) {

}
