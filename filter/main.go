package filter

import (
	filter "filter/common"

	"github.com/op/go-logging"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)


var log = logging.MustGetLogger("log")

func main() {
	var input mw.MessageMiddleware
	var output mw.MessageMiddleware




	filterType := "byYear"
	mwAddress := "amqp://guest:guest@localhost:5672/"

	consumerName := "transactionToFilterByYear"

	input, err := mw.NewConsumer(consumerName, "transactions", mwAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = mw.NewProducer("filteredTransactionsByYear", mwAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	filterByYear := filter.NewFilter(input, output, filterType)
	filterByYear.Start()
}




