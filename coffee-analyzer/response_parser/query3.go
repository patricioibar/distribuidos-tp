package responseparser

import (
	c "communication"
	"encoding/json"
	"strings"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (rp *ResponseParser) parseQuery3Response() mw.OnMessageCallback {
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		jsonStr := string(msg.Body)
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if batch.IsEndSignal() {
			log.Infof("Received end signal for query 3")
			done <- nil
			rp.queryDone[2] <- struct{}{}
			return
		}

		for i, col := range batch.ColumnNames {
			if strings.Contains(col, "sum") {
				batch.ColumnNames[i] = "tpv"
				break
			}
		}

		parsedBatch := c.QueryResponseBatch{
			QueryId: 3,
			Columns: batch.ColumnNames,
			Rows:    genericRowsToStringRows(batch.Rows),
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
