package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareChannel *amqp.Channel
type MiddlewareMessage amqp.Delivery

type MessageMiddlewareError struct {
	Code int
	Msg  string
}

func (e *MessageMiddlewareError) Error() string {
	return fmt.Sprintf("middleware error (%d): %s", e.Code, e.Msg)
}

const (
	MessageMiddlewareMessageError int = iota + 1
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError

	// hablar con el corrector
	MessageMiddlewareProducerCannotConsumeError
	MessageMiddlewareConsumerCannotSendError
)

// cambié el done chan a un puntero para poder devolver nil
type onMessageCallback func(consumeChannel MiddlewareMessage, done chan *MessageMiddlewareError)

// Puede especificarse un tipo más específico para T si se desea
type MessageMiddleware interface {
	/*
	   Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
	   cada mensaje de datos o de control.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	StartConsuming(onMessageCallback onMessageCallback) (error *MessageMiddlewareError)

	/*
	   Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	   no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	*/
	StopConsuming() (error *MessageMiddlewareError)

	/*
	   Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	Send(message []byte) (error *MessageMiddlewareError)

	/*
	   Se desconecta de la cola o exchange al que estaba conectado.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	*/
	Close() (error *MessageMiddlewareError)

	/*
	   Se fuerza la eliminación remota de la cola o exchange.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	*/
	Delete() (error *MessageMiddlewareError)
}
