package persistance

import (
	"bytes"
	"encoding/gob"

	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

type monitoreState struct {
	monitorsSeen []string
	workersSeen  []string
}

type stateSnapshot struct {
	MonitorsSeen []string
	WorkersSeen  []string
}

var _ pers.State = (*monitoreState)(nil)

func NewMonitorState() pers.State {
	return &monitoreState{
		monitorsSeen: []string{},
		workersSeen:  []string{},
	}
}

func (ms *monitoreState) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	snapshot := stateSnapshot{
		MonitorsSeen: ms.monitorsSeen,
		WorkersSeen:  ms.workersSeen,
	}
	if err := enc.Encode(snapshot); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ms *monitoreState) Deserialize(data []byte) error {
	if len(data) == 0 {
		ms.monitorsSeen = nil
		ms.workersSeen = nil
		return nil
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var snapshot stateSnapshot
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}
	ms.monitorsSeen = append([]string(nil), snapshot.MonitorsSeen...)
	ms.workersSeen = append([]string(nil), snapshot.WorkersSeen...)
	return nil
}

func (ms *monitoreState) GetSeenMonitors() []string {
	return ms.monitorsSeen
}

func (ms *monitoreState) GetLastSeenWorkers() []string {
	return ms.workersSeen
}

func (ms *monitoreState) Apply(op pers.Operation) error {
	return op.ApplyTo(ms)
}
