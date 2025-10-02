package responseparser

import (
	c "communication"
	"encoding/json"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (rp *ResponseParser) parseQuery4Response() mw.OnMessageCallback {
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {

		jsonStr := string(msg.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if batch.IsEndSignal() {
			log.Infof("Received end signal for query 4")
			done <- nil
			rp.queryDone[3] <- struct{}{}
			return
		}

		parsedBatch := c.QueryResponseBatch{
			QueryId: 4,
			Columns: batch.ColumnNames,
			Rows:    anyRowsToStringRows(batch.Rows),
		}

		data, err := json.Marshal(parsedBatch)
		if err != nil {
			log.Errorf("Failed to marshal response: %v", err)
			done <- nil
			return
		}
		if err := rp.socket.SendBatch(data); err != nil {
			log.Errorf("Failed to send batch: %v", err)
		}
		done <- nil
	}
}
