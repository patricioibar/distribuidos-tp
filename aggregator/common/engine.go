package common

import (
	"github.com/patricioibar/distribuidos-tp/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type AggregatorWorker struct {
	Config    *Config
	input     middleware.MessageMiddleware
	output    middleware.MessageMiddleware
	onMessage middleware.OnMessageCallback
	batchChan chan string
	closeChan chan struct{}
}

func NewAggregator(config *Config) *AggregatorWorker {
	var input middleware.MessageMiddleware
	var output middleware.MessageMiddleware

	input, err := middleware.NewConsumer(config.InputName)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = middleware.NewProducer(config.OutputName)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	onMessage := func(consumeChannel middleware.MiddlewareMessage, done chan *middleware.MessageMiddlewareError) {
		// TODO: Implement aggregation logic
	}

	return &AggregatorWorker{
		Config:    config,
		input:     input,
		output:    output,
		onMessage: onMessage,
	}
}

func (a *AggregatorWorker) Start() {
	if err := a.input.StartConsuming(a.onMessage); err != nil {
		a.Close()
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	go func() {
		select {
		case <-a.closeChan:
			return
		case batch := <-a.batchChan:
			if err := a.output.Send([]byte(batch)); err != nil {
				log.Errorf("Failed to send message: %v", err)
			}
		}
	}()
}

func (a *AggregatorWorker) Close() {
	if err := a.input.StopConsuming(); err != nil {
		log.Errorf("Failed to stop consuming messages: %v", err)
	}
	if err := a.output.StopConsuming(); err != nil {
		log.Errorf("Failed to stop producing messages: %v", err)
	}
}
