package main

import (
	"os"

	"github.com/op/go-logging"
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
	config, err := InitConfig()
	log.Infof("Loaded config: %+v", config)
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(config.LogLevel); err != nil {
		log.Criticalf("%s", err)
	}

	serverConn := ServerConnection{
		BatchSize:             config.BatchSize,
		CoffeeAnalyzerAddress: config.CoffeeAnalyzerAddress,
	}

	for _, table := range config.Tables {
		log.Infof("Sending dataset: %s with columns: %v", table.Name, table.Columns)
		serverConn.sendDataset(table, config.DataDir)
	}
}
