package filterbyyear

import (
	"github.com/op/go-logging"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)


var log = logging.MustGetLogger("log")

func main() {


	var input mw.MessageMiddleware
	var output mw.MessageMiddleware



	consumerName := "transactionToFilterByYear"

	input, err := mw.NewConsumer(consumerName, "transactions", "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = mw.NewProducer("filteredTransactionsByYear", "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	filter := NewFilterByYear(input, output)
	filter.Start()
}




