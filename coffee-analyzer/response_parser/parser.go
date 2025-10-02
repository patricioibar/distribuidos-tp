package responseparser

import (
	"communication"
	"fmt"

	"github.com/op/go-logging"

	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var log = logging.MustGetLogger("log")

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
	socket     *communication.Socket
	querySinks []QuerySink
	queryDone  []chan struct{}
}

func NewResponseParser(queries []QueryOutput, mwAddr string) *ResponseParser {
	if len(queries) < 4 {
		log.Fatalf("Expected at least 4 queries, got %d", len(queries))
	}
	rp := ResponseParser{
		queryDone: make([]chan struct{}, len(queries)),
	}
	var querySinks []QuerySink
	for i, query := range queries {
		name := fmt.Sprintf("%s_queue", query.SinkName)
		consumer, err := mw.NewConsumer(name, query.SinkName, mwAddr)
		if err != nil {
			log.Fatalf("Failed to create consumer for sink %s: %v", query.SinkName, err)
		}
		rp.queryDone[i] = make(chan struct{})
		callback := rp.callbackForQuery(i + 1)
		querySinks = append(querySinks, QuerySink{
			cfg:      query,
			consumer: consumer,
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

func (rp *ResponseParser) Start(s *communication.Socket) {
	rp.socket = s
	for _, sink := range rp.querySinks {
		sink.consumer.StartConsuming(sink.callback)
	}
	for _, done := range rp.queryDone {
		<-done
	}
	log.Infof("All queries done, shutting down response parser")
	s.Close()
	for _, sink := range rp.querySinks {
		sink.consumer.Close()
	}
}

func anyRowsToStringRows(rows [][]any) [][]string {
	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		stringRows[i] = make([]string, len(row))
		for j, col := range row {
			stringRows[i][j] = fmt.Sprintf("%v", col)
		}
	}
	return stringRows
}

func genericRowsToStringRows(rows [][]interface{}) [][]string {
	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		stringRows[i] = make([]string, len(row))
		for j, col := range row {
			stringRows[i][j] = fmt.Sprintf("%v", col)
		}
	}
	return stringRows
}
