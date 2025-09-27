module filter

go 1.25.1

require github.com/rabbitmq/amqp091-go v1.10.0

require (
	github.com/patricioibar/distribuidos-tp/innercommunication v0.0.0-00010101000000-000000000000
	github.com/patricioibar/distribuidos-tp/middleware v0.0.0-00010101000000-000000000000
	github.com/spf13/viper v1.21.0
)

replace github.com/patricioibar/distribuidos-tp/innercommunication => ../innercommunication

replace github.com/patricioibar/distribuidos-tp/middleware => ../middleware