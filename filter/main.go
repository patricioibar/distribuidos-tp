package main

import (
	filter "filter/common"
	"fmt"
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
	config := getConfig()

	level := "INFO"
	if config.LogLevel != "" {
		level = config.LogLevel
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
	filterName := "filter-" + config.FilterType + "-" + config.FilterId
	jobSource := "JOB_SOURCE"

	jobAnnouncements, err := mw.NewConsumer(jobSource+"_"+filterName, jobSource, config.MwAddress)
	if err != nil {
		log.Fatalf("Failed to create job announcements consumer: %v", err)
	}

	runningFilters := make(map[string]*filter.FilterWorker)
	handleNewIncommingJob := getHandleIncommingJob(config, runningFilters)

	jobAnnouncements.StartConsuming(handleNewIncommingJob)

	// Wait for termination signal
	sig := <-sigChan
	log.Infof("Received signal: %v. Initiating graceful shutdown...", sig)

	// Graceful shutdown sequence
	shutdownGracefully(runningFilters, jobAnnouncements)

	log.Info("Filter service shutdown completed.")
}

func getHandleIncommingJob(config Config, runningFilters map[string]*filter.FilterWorker) mw.OnMessageCallback {

	return func(consumeChannel mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		var id uuid.UUID
		id, err := uuid.FromBytes(consumeChannel.Body)
		if err != nil {
			log.Errorf("Failed to parse job ID from message: %v", err)
			done <- nil
			return
		}

		// cola del que va a consumir el worker
		input, err := mw.NewConsumer(config.ConsumerName, config.SourceQueue, config.MwAddress, id.String())
		if err != nil {
			log.Errorf("Failed to create input consumer for job %s: %v", id.String(), err)
			done <- nil
			return
		}

		// Exchange al que avisar que ya se bindio la cola
		ExchangeNotify, err := mw.NewProducer(id.String(), config.MwAddress)
		if err != nil {
			log.Errorf("Failed to create exchange notify producer for job %s: %v", id.String(), err)
			done <- nil
			return
		}

		ExchangeNotify.Send([]byte(fmt.Sprintf("Job %s is ready", id.String())))
		ExchangeNotify.Close()
		// creo mi filter y me guardo la referencia

		output, err := mw.NewProducer(config.OutputExchange, config.MwAddress, id.String())
		if err != nil {
			log.Errorf("Failed to create output producer: %v", err)
			done <- nil
			return
		}

		filterWorker := filter.NewFilter(config.FilterId, input, output, config.FilterType, config.WorkersCount)

		runningFilters[id.String()] = filterWorker

		go func() {
			log.Infof("Starting filter rutine (ID: %s, Type: %s, JobID: %s)", config.FilterId, config.FilterType, id.String())
			filterWorker.Start()
		}()

		done <- nil
	}

}

func shutdownGracefully(runningFilters map[string]*filter.FilterWorker, input mw.MessageMiddleware) {
	log.Info("Starting graceful shutdown sequence...")

	// Set a timeout for graceful shutdown
	shutdownTimeout := 30 * time.Second
	shutdownComplete := make(chan bool, 1)

	go func() {
		// Step 1: Stop the filter workers
		log.Info("Stopping filter workers...")
		for _, filterWorker := range runningFilters {
			filterWorker.Close()
		}
		log.Info("Filter workers stopped.")

		// Step 2: Close input consumer (stop receiving new messages)
		log.Info("Closing input consumer...")
		if err := input.Close(); err != nil {
			log.Errorf("Error stopping input consumer: %v", err)
		} else {
			log.Info("Input consumer stopped.")
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

func getConfig() Config {
	// get enviroment variables for filterType, consumerName, mwAddress, sourceQueue, outputExchange
	filterId := os.Getenv("FILTER_ID")
	workersCountStr := os.Getenv("WORKERS_COUNT")
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
	workersCount, err := strconv.Atoi(workersCountStr)
	if err != nil {
		log.Critical("Invalid WORKERS_COUNT value")
		os.Exit(1)
	}

	return Config{
		FilterId:       filterId,
		WorkersCount:   workersCount,
		FilterType:     filterType,
		ConsumerName:   consumerName,
		MwAddress:      mwAddress,
		SourceQueue:    sourceQueue,
		OutputExchange: outputExchange,
		LogLevel:       logLevel,
	}
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

type Config struct {
	FilterId       string
	WorkersCount   int
	FilterType     string
	ConsumerName   string
	MwAddress      string
	SourceQueue    string
	OutputExchange string
	LogLevel       string
}
