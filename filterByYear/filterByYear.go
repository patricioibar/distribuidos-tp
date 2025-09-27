package filterbyyear

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)


func main() {
	msgs, ch, conn, _, err := initializeRabbitMQ()
	failOnError(err, "Failed to initialize RabbitMQ")
	defer ch.Close()
	defer conn.Close()


	termChan := make(chan bool, 1)

	setSignalHandler(termChan)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			batch := NewBatchFromJSON(string(d.Body))
			filteredRows, err := processBatch(batch)
			if err != nil {
				log.Printf("Error processing batch: %v", err)
				continue
			}
			log.Printf("Filtered rows: %v", filteredRows)
			// enviar filteredRows a la siguiente etapa
		}
	}()

	if <-termChan; true {
		log.Println("Shutting down...")
		conn.Close()
		ch.Close()
		failOnError(err, "Failed to delete a queue")
	}


}




func setSignalHandler(termChan chan bool) {
	// Aquí puedes agregar la lógica para manejar señales del sistema
	// Por ejemplo, para manejar la señal de interrupción (Ctrl+C)
	// y cerrar la conexión de RabbitMQ de manera ordenada
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Received interrupt signal, shutting down...")
		termChan <- true
	}()

}


func initializeRabbitMQ() (<-chan amqp.Delivery, *amqp.Channel, *amqp.Connection, amqp.Queue, error) {


	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"transactions",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		"transactions",
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")
	



	return msgs, ch, conn, q, err
}

func failOnError(err error, msg string) {
        if err != nil {
                log.Panicf("%s: %s", msg, err)
        }
}