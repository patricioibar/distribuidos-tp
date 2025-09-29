package middleware

import (
	"log"
	"sync"
	"time"

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
		connectExponentialRetry(url)
	})
	return instance
}

func connectExponentialRetry(url string) {
	var err error
	var c *amqp.Connection
	retryTime := 1
	for {
		c, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("No se pudo conectar a RabbitMQ: %v", err)
		retryTime *= 2
		time.Sleep(time.Duration(retryTime) * time.Second)
	}
	instance = &RabbitConn{conn: c}
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
