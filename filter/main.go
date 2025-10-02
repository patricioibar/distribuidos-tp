package main

import (
	filter "filter/common"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/op/go-logging"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)


var log = logging.MustGetLogger("log")

func main() {
	var input mw.MessageMiddleware
	var output mw.MessageMiddleware

	filterId, workersCountStr, filterType, consumerName, mwAddress, sourceQueue, outputExchange := getConfig()

	// Create signal channel to handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	input, err := mw.NewConsumer(consumerName, sourceQueue, mwAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = mw.NewProducer(outputExchange, mwAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	workersCount, err := strconv.Atoi(workersCountStr)
	if err != nil {
		log.Fatalf("Invalid WORKERS_COUNT value: %v", err)
		return
	}

	filterWorker := filter.NewFilter(filterId, input, output, filterType, workersCount)
	
	// Start the filter worker in a goroutine
	go func() {
		log.Infof("Starting filter worker (ID: %s, Type: %s)", filterId, filterType)
		filterWorker.Start()
	}()

	// Wait for termination signal
	sig := <-sigChan
	log.Infof("Received signal: %v. Initiating graceful shutdown...", sig)

	// Graceful shutdown sequence
	shutdownGracefully(filterWorker, input, output)
	
	log.Info("Filter service shutdown completed.")
}

func shutdownGracefully(filterWorker *filter.FilterWorker, input, output mw.MessageMiddleware) {
	log.Info("Starting graceful shutdown sequence...")
	
	// Set a timeout for graceful shutdown
	shutdownTimeout := 30 * time.Second
	shutdownComplete := make(chan bool, 1)
	
	go func() {
		// Step 1: Stop the filter worker
		log.Info("Stopping filter worker...")
		filterWorker.Close()
		log.Info("Filter worker stopped.")
		
		// Step 2: Close input consumer (stop receiving new messages)
		log.Info("Closing input consumer...")
		if err := input.StopConsuming(); err != nil {
			log.Errorf("Error stopping input consumer: %v", err)
		} else {
			log.Info("Input consumer stopped.")
		}
		
		// Step 3: Close output producer (finish sending pending messages)
		log.Info("Closing output producer...")
		if err := output.StopConsuming(); err != nil {
			log.Errorf("Error stopping output producer: %v", err)
		} else {
			log.Info("Output producer stopped.")
		}
		
		shutdownComplete <- true
	}()
	
	// Wait for graceful shutdown or timeout
	select {
	case <-shutdownComplete:
		log.Info("Graceful shutdown completed successfully.")
	case <-time.After(shutdownTimeout):
		log.Warningf("Graceful shutdown timed out after %v. Forcing exit.", shutdownTimeout)
	}
}


func getConfig() (string, string, string, string, string, string, string) {
	// get enviroment variables for filterType, consumerName, mwAddress, sourceQueue, outputExchange
	filterId := os.Getenv("FILTER_ID")
	workersCount := os.Getenv("WORKERS_COUNT")
	filterType := os.Getenv("FILTER_TYPE")
	consumerName := os.Getenv("CONSUMER_NAME")
	mwAddress := os.Getenv("MW_ADDRESS")
	sourceQueue := os.Getenv("SOURCE_QUEUE")
	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if filterType == "" || consumerName == "" || mwAddress == "" || sourceQueue == "" || outputExchange == "" {
		log.Critical("One or more required environment variables are not set: FILTER_TYPE, CONSUMER_NAME, MW_ADDRESS, SOURCE_QUEUE, OUTPUT_EXCHANGE")
		os.Exit(1)
	}

	return filterId, workersCount, filterType, consumerName, mwAddress, sourceQueue, outputExchange	
}
