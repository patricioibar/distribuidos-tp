package persistence

import (
	"os"

	"github.com/op/go-logging"
	"github.com/patricioibar/distribuidos-tp/persistance"
)

const StateRoot = ".state"

var log = logging.MustGetLogger("log")

func InitStateManager(jobId string) *persistance.StateManager {
	stateDir := StateRoot + "/" + jobId

	recover := false
	if _, err := os.Stat(stateDir); err == nil {
		recover = true
	} else if !os.IsNotExist(err) {
		log.Errorf("Failed to stat state dir %s: %v", stateDir, err)
	}

	stateLog, err := persistance.NewStateLog(stateDir)
	if err != nil {
		log.Errorf("Failed to create state log for job %s: %v", jobId, err)
		return nil
	}

	fs := NewPersistentState()
	sm, err := persistance.NewStateManager(fs, stateLog, 100)
	if err != nil {
		log.Errorf("Failed to create state manager for job %s: %v", jobId, err)
		_ = stateLog.Close()
		return nil
	}

	if !recover {
		return sm
	}

	if err := sm.Restore(); err != nil {
		log.Errorf("Failed to restore state for job %s: %v", jobId, err)
		_ = sm.Close()
		return nil
	}
	return sm
}
