package main

import (
	"os"
	"os/signal"
	"syscall"

	"joiner/common"

	"github.com/google/uuid"
	"github.com/op/go-logging"

	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

const incomingJobsSource = "JOB_SOURCE"

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

	jobsMap := make(map[string]*common.JoinerWorker)

	incomingJobs, err := mw.NewConsumer("", incomingJobsSource, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create incoming jobs consumer: %v", err)
	}
	callback := initializeJoinerJob(config, jobsMap)
	if err := incomingJobs.StartConsuming(callback); err != nil {
		log.Fatalf("Failed to start consuming messages: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Infof("Received signal %s, shutting down joiner...", sig)
	for _, job := range jobsMap {
		job.Close()
	}
}

func initializeJoinerJob(config *common.Config, jobsMap map[string]*common.JoinerWorker) func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		jobId, err := uuid.FromBytes(msg.Body[0:16])
		if err != nil {
			done <- nil
			log.Errorf("Failed to parse job ID: %v", err)
			return
		}
		jobStr := jobId.String()
		log.Infof("Received job %s", jobStr)

		var leftInput mw.MessageMiddleware
		var rightInput mw.MessageMiddleware
		var output mw.MessageMiddleware

		// Share same queue for left input (round-robin among workers)
		leftName := config.LeftInputName + "-" + config.QueryName
		leftInput, err = mw.NewConsumer(leftName, config.LeftInputName, config.MiddlewareAddress, jobStr)
		if err != nil {
			log.Fatalf("Failed to create input consumer: %v", err)
		}

		// All workers receive copies of the messages from the right input
		uniqueName := config.RightInputName + "-" + config.WorkerId
		rightInput, err = mw.NewConsumer(uniqueName, config.RightInputName, config.MiddlewareAddress, jobStr)
		if err != nil {
			log.Fatalf("Failed to create input consumer: %v", err)
		}

		output, err = mw.NewProducer(config.OutputName, config.MiddlewareAddress, jobStr)
		if err != nil {
			log.Fatalf("Failed to create output producer: %v", err)
		}

		joiner := common.NewJoinerWorker(config, leftInput, rightInput, output)
		jobsMap[jobStr] = joiner

		ready, err := mw.NewProducer(jobStr, config.MiddlewareAddress)
		if err != nil {
			done <- nil
			log.Errorf("Failed to create worker ready notifier for job %s: %v", jobStr, err)
			return
		}
		ready.Send([]byte(config.WorkerId))
		ready.Close()

		log.Infof("Starting joiner %s...", config.WorkerId)
		joiner.Start()
		done <- nil
	}
}
