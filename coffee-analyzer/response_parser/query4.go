package responseparser

import (
	c "communication"
	"encoding/json"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (rp *ResponseParser) parseQuery4Response() mw.OnMessageCallback {
	sentBatches := bitmap.New()
	return func(msg mw.MiddlewareMessage, done chan *mw.MessageMiddlewareError) {
		defer func() { done <- nil }()
		jsonStr := string(msg.Body)
		var receivedMsg ic.Message
		err := receivedMsg.Unmarshal([]byte(jsonStr))
		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			return
		}

		switch p := receivedMsg.Payload.(type) {

		case *ic.RowsBatchPayload:
			if sentBatches.Contains(p.SeqNum) {
				return
			}
			parsedBatch := c.QueryResponseBatch{
				QueryId: 4,
				Columns: p.ColumnNames,
				Rows:    genericRowsToStringRows(p.Rows),
			}

			data, err := json.Marshal(parsedBatch)
			if err != nil {
				log.Errorf("Failed to marshal response: %v", err)
				return
			}
			if err := rp.socket.SendBatch(data); err != nil {
				log.Errorf("Failed to send batch: %v", err)
				return
			}
			sentBatches.Add(p.SeqNum)

		case *ic.EndSignalPayload:
			if sentBatches.GetCardinality() != p.SeqNum {
				log.Errorf(
					"Received end signal but not all batches were sent!\tTotal batches: %d\tSent Batches: %d",
					p.SeqNum, sentBatches.GetCardinality(),
				)
			}
			rp.queryResultReceived(4, 3)

		case *ic.SequenceSetPayload:
			// batches with these sequence numbers had no information for this query
			sentBatches.Or(p.Sequences.Bitmap)

		default:
			log.Errorf("Unknown payload type")

		}

	}
}
