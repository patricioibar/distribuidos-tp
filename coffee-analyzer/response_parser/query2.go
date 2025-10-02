package responseparser

import (
	c "communication"
	"encoding/json"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (rp *ResponseParser) parseQuery2Response() mw.OnMessageCallback {
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {

		jsonStr := string(msg.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if batch.IsEndSignal() {
			log.Infof("Received end signal for query 2")
			done <- nil
			return
		}

		parsedBatch := c.QueryResponseBatch{
			QueryId: 2,
			Columns: batch.ColumnNames,
			Rows:    batch.Rows,
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
