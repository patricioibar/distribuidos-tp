package main

import (
	"communication"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"aggregator/common"

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

	jobsMap := make(map[string]*common.AggregatorWorker)
	jobsMapLock := sync.Mutex{}

	incomingJobs, err := mw.NewConsumer(incomingJobsSource+"_"+config.WorkerId, incomingJobsSource, config.MiddlewareAddress)
	if err != nil {
		log.Fatalf("Failed to create incoming jobs consumer: %v", err)
	}
	removeFromMap := make(chan string, 10)
	callback := initializeAggregatorJob(config, jobsMap, &jobsMapLock, removeFromMap)

	monitorsCount, _ := strconv.Atoi(config.MonitorsCount)
	go communication.SendHeartbeatToMonitors("WORKER", config.WorkerId, monitorsCount)

	go func() {
		if err := incomingJobs.StartConsuming(callback); err != nil {
			log.Fatalf("Failed to start consuming messages: %v", err)
		}
		log.Warningf("Incoming jobs queue closed! Stopped receiving new jobs")
	}()

	go removeDoneJobs(jobsMap, &jobsMapLock, removeFromMap)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Infof("Received signal %s, shutting down aggregator...", sig)
	incomingJobs.Close()
	jobsMapLock.Lock()
	for _, job := range jobsMap {
		job.Close()
	}
	jobsMapLock.Unlock()
	close(removeFromMap)
}

func removeDoneJobs(jobsMap map[string]*common.AggregatorWorker, mutex *sync.Mutex, removeFromMap chan string) {
	for jobID := range removeFromMap {
		mutex.Lock()
		delete(jobsMap, jobID)
		mutex.Unlock()
	}
}

func initializeAggregatorJob(config *common.Config, jobsMap map[string]*common.AggregatorWorker, jobsMapLock *sync.Mutex, removeFromMap chan string) func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		jobId, err := uuid.FromBytes(msg.Body[0:16])
		if err != nil {
			done <- nil
			log.Errorf("Failed to parse job ID: %v", err)
			return
		}
		jobStr := jobId.String()
		log.Infof("Received job %s", jobStr)

		var input mw.MessageMiddleware
		var output mw.MessageMiddleware

		consumerName := config.InputName + config.QueryName
		input, err = mw.NewConsumer(consumerName, config.InputName, config.MiddlewareAddress, jobStr)
		if err != nil {
			done <- nil
			log.Errorf("Failed to create input consumer: %v", err)
			return
		}
		output, err = mw.NewProducer(config.OutputName, config.MiddlewareAddress, jobStr)
		if err != nil {
			done <- nil
			log.Errorf("Failed to create output producer: %v", err)
			return
		}

		aggregator := common.NewAggregatorWorker(config, input, output, jobStr, removeFromMap, 0)
		jobsMapLock.Lock()
		jobsMap[jobStr] = aggregator
		jobsMapLock.Unlock()

		log.Infof("Starting aggregator %s for job %s", config.WorkerId, jobStr)
		go aggregator.Start()

		ready, err := mw.NewProducer(jobStr, config.MiddlewareAddress)
		if err != nil {
			done <- nil
			log.Errorf("Failed to create ready producer for job %s: %v", jobStr, err)
			return
		}
		ready.Send([]byte(config.WorkerId))
		ready.Close()

		done <- nil
	}

}
