package responseparser

import (
	c "communication"
	"encoding/json"
	"strings"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

func (rp *ResponseParser) parseQuery3Response() mw.OnMessageCallback {
	seenBatches := bitmap.New()
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
			if seenBatches.Contains(p.SeqNum) {
				return
			}

			for i, col := range p.ColumnNames {
				if strings.Contains(col, "sum") {
					p.ColumnNames[i] = "tpv"
					break
				}
			}

			parsedBatch := c.QueryResponseBatch{
				QueryId: 3,
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
			seenBatches.Add(p.SeqNum)

		case *ic.EndSignalPayload:
			log.Infof("EOF SeqNum for query 3: %d, seen batches: %d", p.SeqNum, seenBatches.GetCardinality())
			rp.queryResultReceived(3, 2)

		case *ic.SequenceSetPayload:
			// batches with these sequence numbers had no information for this query
			seenBatches.Or(p.Sequences.Bitmap)

		default:
			log.Errorf("Unknown payload type")

		}
	}
}
