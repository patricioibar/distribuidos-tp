package main

import (
	"communication"
	"os"
	"os/signal"
	"sync"
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
	jobsMapLock := sync.Mutex{}
	removeFromMap := make(chan string, 10)
	incomingJobs, err := mw.NewConsumer(incomingJobsSource+"_"+config.WorkerId, incomingJobsSource, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create incoming jobs consumer: %v", err)
	}
	callback := initializeJoinerJob(config, jobsMap, &jobsMapLock, removeFromMap)

	go communication.SendHeartBeat(config.WorkerId)

	go func() {
		if err := incomingJobs.StartConsuming(callback); err != nil {
			log.Fatalf("Failed to start consuming messages: %v", err)
		}
		log.Warning("Incoming jobs queue closed! Stopped receiving new jobs")
	}()

	go removeDoneJobs(jobsMap, &jobsMapLock, removeFromMap)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Infof("Received signal %s, shutting down joiner...", sig)
	incomingJobs.Close()
	jobsMapLock.Lock()
	for _, job := range jobsMap {
		job.Close()
	}
	jobsMapLock.Unlock()
	close(removeFromMap)
}

func removeDoneJobs(jobsMap map[string]*common.JoinerWorker, mutex *sync.Mutex, removeFromMap chan string) {
	for jobID := range removeFromMap {
		mutex.Lock()
		delete(jobsMap, jobID)
		mutex.Unlock()
	}
}

func initializeJoinerJob(config *common.Config, jobsMap map[string]*common.JoinerWorker, jobsMapLock *sync.Mutex, removeFromMap chan string) func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
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

		joiner := common.NewJoinerWorker(config, leftInput, rightInput, output, jobStr, removeFromMap)

		jobsMapLock.Lock()
		jobsMap[jobStr] = joiner
		jobsMapLock.Unlock()

		log.Infof("Starting joiner %s...", config.WorkerId)
		go joiner.Start()

		ready, err := mw.NewProducer(jobStr, config.MiddlewareAddress)
		if err != nil {
			done <- nil
			log.Errorf("Failed to create worker ready notifier for job %s: %v", jobStr, err)
			return
		}
		ready.Send([]byte(config.WorkerId))
		ready.Close()

		done <- nil
	}
}
