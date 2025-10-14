module filter

go 1.25

require github.com/rabbitmq/amqp091-go v1.10.0 // indirect

require (
	github.com/google/uuid v1.6.0
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/patricioibar/distribuidos-tp/innercommunication v0.0.0-00010101000000-000000000000
	github.com/patricioibar/distribuidos-tp/middleware v0.0.0-00010101000000-000000000000
)

replace github.com/patricioibar/distribuidos-tp/innercommunication => ../innercommunication

replace github.com/patricioibar/distribuidos-tp/middleware => ../middleware
