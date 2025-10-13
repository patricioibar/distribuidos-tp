package main

import (
	filter "filter/common"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

func main() {
	var input mw.MessageMiddleware
	var output mw.MessageMiddleware

	filterId, workersCountStr, filterType, consumerName, mwAddress, sourceQueue, outputExchange, logLevel := getConfig()

	level := "INFO"
	if logLevel != "" {
		level = logLevel
	}
	if err := InitLogger(level); err != nil {
		log.Fatalf("%s", err)
	}

	// Create signal channel to handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Quiero leer de la cola de jobAnnouncements
	// para eso tengo que bindear la cola para poder consumir de ella
	// Una vez que lea de ella tengo que crear las nuevas colas para el job ese
	// Y cuando ya tengo estas colas creadas, paso a avisarle al coffee analyzer que ya las cree
	// Despues inicializo el worker en una go rutine
	filterName := "FILTER-" + filterType + "-" + filterId
	jobSource := "JOB_SOURCE"

	jobAnnouncements, err := mw.NewConsumer(filterName, jobSource, mwAddress)

	jobAnnouncements.StartConsuming(handleNewIncommingJob)





	// input, err = mw.NewConsumer(consumerName, sourceQueue, mwAddress)
	// if err != nil {
	// 	log.Fatalf("Failed to create input consumer: %v", err)
	// }
	// output, err = mw.NewProducer(outputExchange, mwAddress)
	// if err != nil {
	// 	log.Fatalf("Failed to create output producer: %v", err)
	// }

	// workersCount, err := strconv.Atoi(workersCountStr)
	// if err != nil {
	// 	log.Fatalf("Invalid WORKERS_COUNT value: %v", err)
	// }

	// filterWorker := filter.NewFilter(filterId, input, output, filterType, workersCount)

	// // Start the filter worker in a goroutine
	// go func() {
	// 	log.Infof("Starting filter worker (ID: %s, Type: %s)", filterId, filterType)
	// 	filterWorker.Start()
	// }()

	// Wait for termination signal
	sig := <-sigChan
	log.Infof("Received signal: %v. Initiating graceful shutdown...", sig)

	// Graceful shutdown sequence
	shutdownGracefully(filterWorker, input, output)

	log.Info("Filter service shutdown completed.")
}


func getHandleIncommingJob(ConsumerName: string, sourceQueue: string, mwAddress: string, filterId: string, input: string, output: string, filterType:string, workersCount: string, runningFilters: map[string]*filter.FilterWorker) (mw.OnMessageCallback) {

	return func(consumeChannel MiddlewareMessage, done chan *MessageMiddlewareError) {
		var id uuid.UUID
		id, err := uuid.FromBytes(consumeChannel.Body)

		// cola del que va a consumir el worker
		inputQueue, err := mw.NewConsumer(ConsumerName, sourceQueue, mwAddress, id.String())
		if err != nil {
			log.Fatalf("Failed to create input consumer for job %s: %v", id.String(), err)
		}

		// Exchange al que avisar que ya se bindio la cola
		ExchangeNotify, err := mw.NewProducer(id.String(), mwAddress)

		ExchangeNotify.Send("PERON PERON") //?

		// creo mi filter y me guardo la referencia

		outputExchange, err = mw.NewProducer(outputExchange, mwAddress, id.String())
		if err != nil {
			log.Fatalf("Failed to create output producer: %v", err)
		}

		filterWorker := filter.NewFilter(filterId, inputQueue, outputExchange, filterType, workersCount)

		runningFilters.add()

		go func() {
			log.Infof("Starting filter rutine (ID: %s, Type: %s, JobID: %s)", filterId, filterType, id.String())
			filterWorker.Start()
		}()

	}

}

func handleNewIncommingJob(consumeChannel MiddlewareMessage, done chan *MessageMiddlewareError) {
	var id uuid.UUID
	id, err := uuid.FromBytes(consumeChannel.Body)

	// cola del que va a consumir el worker
	inputQueue, err := mw.NewConsumer(ConsumerName, sourceQueue, mwAddress, id.String())
	if err != nil {
		log.Fatalf("Failed to create input consumer for job %s: %v", id.String(), err)
	}

	// Exchange al que avisar que ya se bindio la cola
	ExchangeNotify, err := mw.NewProducer(id.String(), mwAddress)

	ExchangeNotify.Send("PERON PERON") //?

	// creo mi filter y me guardo la referencia

	output, err = mw.NewProducer(outputExchange, mwAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	filterWorker := filter.NewFilter(filterId, input, output, filterType, workersCount)


	go func() {
		log.Infof("Starting filter rutine (ID: %s, Type: %s, JobID: %s)", filterId, filterType, id.String())
		filterWorker.Start()
	}()

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

func getConfig() (string, string, string, string, string, string, string, string) {
	// get enviroment variables for filterType, consumerName, mwAddress, sourceQueue, outputExchange
	filterId := os.Getenv("FILTER_ID")
	workersCount := os.Getenv("WORKERS_COUNT")
	filterType := os.Getenv("FILTER_TYPE")
	consumerName := os.Getenv("CONSUMER_NAME")
	mwAddress := os.Getenv("MW_ADDRESS")
	sourceQueue := os.Getenv("SOURCE_QUEUE")
	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	logLevel := os.Getenv("LOG_LEVEL")
	if filterType == "" || consumerName == "" || mwAddress == "" || sourceQueue == "" || outputExchange == "" {
		log.Critical("One or more required environment variables are not set: FILTER_TYPE, CONSUMER_NAME, MW_ADDRESS, SOURCE_QUEUE, OUTPUT_EXCHANGE")
		os.Exit(1)
	}

	return filterId, workersCount, filterType, consumerName, mwAddress, sourceQueue, outputExchange, logLevel
}

func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}
