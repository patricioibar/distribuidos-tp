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
		var receivedMsg ic.Message
		err := receivedMsg.Unmarshal([]byte(jsonStr))
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		switch p := receivedMsg.Payload.(type) {

		case *ic.RowsBatchPayload:

			parsedBatch := c.QueryResponseBatch{
				QueryId: 2,
				Columns: p.ColumnNames,
				Rows:    genericRowsToStringRows(p.Rows),
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

		case *ic.EndSignalPayload:
			rp.queryResultReceived(2, 1)
			done <- nil

		// case *ic.SequenceSetPayload:

		default:
			log.Errorf("Unknown payload type")
			done <- nil

		}
	}
}
