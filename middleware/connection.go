package middleware

import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConn struct {
	conn *amqp.Connection
}

var (
	instance *RabbitConn
	once     sync.Once
)

func GetConnection(url string) *RabbitConn {
	once.Do(func() {
		c, err := amqp.Dial(url)
		if err != nil {
			log.Fatalf("No se pudo conectar a RabbitMQ: %v", err)
		}
		instance = &RabbitConn{conn: c}
	})
	return instance
}

func (r *RabbitConn) Channel() (*amqp.Channel, error) {
	if r.conn.IsClosed() {
		return nil, &MessageMiddlewareError{Code: MessageMiddlewareDisconnectedError, Msg: "Connection is closed"}
	}
	return r.conn.Channel()
}

func (r *RabbitConn) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
