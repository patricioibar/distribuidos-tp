package main

import (
	filter "filter/common"
	"os"

	"github.com/op/go-logging"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)


var log = logging.MustGetLogger("log")

func main() {
	var input mw.MessageMiddleware
	var output mw.MessageMiddleware

	filterId, filterType, consumerName, mwAddress, sourceQueue, outputExchange := getConfig()


	// filterType := "byYear"
	// mwAddress := "amqp://guest:guest@localhost:5672/"

	// consumerName := "transactionToFilterByYear"
	// sourceQueue := "transactions"
	// outputExchange := "filteredTransactionsByYear"

	input, err := mw.NewConsumer(consumerName, sourceQueue, mwAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = mw.NewProducer(outputExchange, mwAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	filterByYear := filter.NewFilter(filterId, input, output, filterType)
	filterByYear.Start()
}


func getConfig() (string, string, string, string, string, string) {
	// get enviroment variables for filterType, consumerName, mwAddress, sourceQueue, outputExchange
	filterId := os.Getenv("FILTER_ID")
	filterType := os.Getenv("FILTER_TYPE")
	consumerName := os.Getenv("CONSUMER_NAME")
	mwAddress := os.Getenv("MW_ADDRESS")
	sourceQueue := os.Getenv("SOURCE_QUEUE")
	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if filterType == "" || consumerName == "" || mwAddress == "" || sourceQueue == "" || outputExchange == "" {
		log.Critical("One or more required environment variables are not set: FILTER_TYPE, CONSUMER_NAME, MW_ADDRESS, SOURCE_QUEUE, OUTPUT_EXCHANGE")
		os.Exit(1)
	}

	return filterId, filterType, consumerName, mwAddress, sourceQueue, outputExchange	
}
