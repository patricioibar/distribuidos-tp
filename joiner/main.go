package main

import (
	"os"
	"os/signal"
	"syscall"

	"joiner/common"

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

	var leftInput mw.MessageMiddleware
	var rightInput mw.MessageMiddleware
	var output mw.MessageMiddleware

	// Share same queue for left input (round-robin among workers)
	leftInput, err = mw.NewConsumer(config.LeftInputName, config.LeftInputName, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}

	// All workers receive copies of the messages from the right input
	uniqueName := config.RightInputName + "-" + config.WorkerId
	rightInput, err = mw.NewConsumer(uniqueName, config.RightInputName, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create input consumer: %v", err)
	}

	output, err = mw.NewProducer(config.OutputName, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create output producer: %v", err)
	}

	joiner := common.NewJoinerWorker(config, leftInput, rightInput, output)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Received signal %s, shutting down joiner...", sig)
		joiner.Close()
	}()

	log.Infof("Starting joiner %s...", config.WorkerId)
	joiner.Start()
}
