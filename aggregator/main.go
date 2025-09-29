package main

import (
	"os"
	"os/signal"
	"syscall"

	"aggregator/common"

	"github.com/op/go-logging"

	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
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

func main() {
	config, err := common.InitConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	if err := InitLogger(config.LogLevel); err != nil {
		log.Fatalf("%s", err)
	}

	log.Debugf("Config: %+v", config)

	var input mw.MessageMiddleware
	var output mw.MessageMiddleware

	consumerName := config.InputName + config.QueryName
	input, err = mw.NewConsumer(consumerName, config.InputName, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}
	output, err = mw.NewProducer(config.OutputName, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	aggregator := common.NewAggregatorWorker(config, input, output)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Received signal %s, shutting down aggregator...", sig)
		aggregator.Close()
	}()

	log.Infof("Starting aggregator %s...", config.WorkerId)
	aggregator.Start()
}
