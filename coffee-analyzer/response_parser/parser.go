package responseparser

import (
	jobsessions "cofee-analyzer/jobsessions"
	c "communication"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/op/go-logging"

	mw "github.com/patricioibar/distribuidos-tp/middleware"
	"github.com/patricioibar/distribuidos-tp/persistance"
)

var log = logging.MustGetLogger("log")

const (
	socketHeartbeatInterval = 5 * time.Second
	socketProbeTimeout      = time.Duration(0)
)

type QueryOutput struct {
	QueryId  int    `json:"query-id" mapstructure:"query-id"`
	SinkName string `json:"sink-name" mapstructure:"sink-name"`
}

type QuerySink struct {
	cfg      QueryOutput
	consumer *mw.Consumer
	callback mw.OnMessageCallback
}

type ResponseParser struct {
	jobID      uuid.UUID
	socket     *c.Socket
	mwAddr     string
	querySinks []QuerySink
	queryDone  []chan struct{}
	queries    []QueryOutput
	jobsState  *persistance.StateManager
	stateMutex *sync.Mutex
}

func NewResponseParser(id uuid.UUID, queries []QueryOutput, mwAddr string, jobsState *persistance.StateManager, stateMutex *sync.Mutex) *ResponseParser {
	if len(queries) < 4 {
		log.Fatalf("Expected at least 4 queries, got %d", len(queries))
	}
	rp := ResponseParser{
		jobID:      id,
		mwAddr:     mwAddr,
		queryDone:  make([]chan struct{}, len(queries)),
		queries:    queries,
		jobsState:  jobsState,
		stateMutex: stateMutex,
	}
	var querySinks []QuerySink
	for i, query := range queries {
		name := fmt.Sprintf("%s_queue", query.SinkName)
		consumer, err := mw.ResumeConsumer(name, query.SinkName, mwAddr, id.String())
		if err != nil {
			log.Fatalf("Failed to create consumer for sink %s: %v", query.SinkName, err)
		}
		rp.queryDone[i] = make(chan struct{}, 1)
		callback := rp.callbackForQuery(i + 1)
		querySinks = append(querySinks, QuerySink{
			cfg:      query,
			consumer: consumer, //this will be nil if all data was already sent
			callback: callback,
		})
	}
	rp.querySinks = querySinks
	return &rp
}

func (rp *ResponseParser) callbackForQuery(i int) mw.OnMessageCallback {
	var callback mw.OnMessageCallback
	switch i {
	case 1:
		callback = rp.parseQuery1Response()
	case 2:
		callback = rp.parseQuery2Response()
	case 3:
		callback = rp.parseQuery3Response()
	case 4:
		callback = rp.parseQuery4Response()
	default:
		log.Fatalf("No callback defined for query %d", i)
	}
	return callback
}

func (rp *ResponseParser) Start(s *c.Socket) {
	rp.socket = s
	stopMonitor := make(chan struct{})
	go rp.monitorConnection(stopMonitor)
	for _, sink := range rp.querySinks {
		go func(sink QuerySink) {
			if sink.consumer == nil {
				// all results already sent
				return
			}
			if err := sink.consumer.StartConsuming(sink.callback); err != nil {
				log.Errorf("Failed to start consuming messages for sink %s: %v", sink.cfg.SinkName, err)
			}
		}(sink)
	}
	for i, done := range rp.queryDone {
		<-done
		if rp.querySinks[i].consumer == nil {
			continue
		}
		rp.querySinks[i].consumer.Delete()
		rp.querySinks[i].consumer.Close()
	}
	close(stopMonitor)
	state := rp.jobsState.GetState().(*jobsessions.JobSessionsState)
	state.RemoveSession(rp.jobID)
	log.Infof("[%s] All queries done, closing response parser", rp.jobID)
	s.Close()
}

func (rp *ResponseParser) CreateSinkQueues() {
	for i, query := range rp.queries {
		name := fmt.Sprintf("%s_queue", query.SinkName)
		consumer, err := mw.NewConsumer(name, query.SinkName, rp.mwAddr, rp.jobID.String())
		if err != nil {
			log.Fatalf("Failed to create consumer for sink %s: %v", query.SinkName, err)
		}
		rp.querySinks[i].consumer = consumer
	}
}

func (rp *ResponseParser) monitorConnection(stop <-chan struct{}) {
	if rp.socket == nil || rp.jobsState == nil || rp.stateMutex == nil {
		return
	}
	ticker := time.NewTicker(socketHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !rp.socket.IsAlive(socketProbeTimeout) {
				log.Infof("[%s] response connection closed, skipping activity refresh", rp.jobID)
				return
			}
			rp.refreshSessionActivity()
		}
	}
}

func (rp *ResponseParser) refreshSessionActivity() {
	rp.stateMutex.Lock()
	defer rp.stateMutex.Unlock()
	state := rp.jobsState.GetState()
	jobState, ok := state.(*jobsessions.JobSessionsState)
	if !ok {
		return
	}
	jobState.UpdateSessionLastActivity(rp.jobID, time.Now().Unix())
}

func genericRowsToStringRows(rows [][]interface{}) [][]string {
	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		stringRows[i] = make([]string, len(row))
		for j, col := range row {
			switch v := col.(type) {
			case float32, float64:
				stringRows[i][j] = fmt.Sprintf("%.2f", v)
			default:
				stringRows[i][j] = fmt.Sprintf("%v", col)
			}
		}
	}
	return stringRows
}

func (rp *ResponseParser) queryResultReceived(queryId int, queryIndex int) {
	log.Infof("[%s] Query %d result received, sending EOF batch", rp.jobID, queryId)
	eofBatch := c.QueryResponseBatch{
		QueryId: queryId,
		EOF:     true,
	}
	data, err := json.Marshal(eofBatch)
	if err != nil {
		log.Errorf("Failed to marshal EOF response: %v", err)
		return
	}
	if err := rp.socket.SendBatch(data); err != nil {
		log.Errorf("Failed to send EOF batch: %v", err)
	}
	rp.queryDone[queryIndex] <- struct{}{}
}
