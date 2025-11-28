package responseparser

import (
	c "communication"
	"encoding/json"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var query1OutputColumns = []string{
	"transaction_id",
	"final_amount",
}

func (rp *ResponseParser) parseQuery1Response() mw.OnMessageCallback {
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

			rows := retainColumns(p, query1OutputColumns)

			parsedBatch := c.QueryResponseBatch{
				QueryId: 1,
				Columns: query1OutputColumns,
				Rows:    genericRowsToStringRows(rows),
			}

			data, err := json.Marshal(parsedBatch)
			if err != nil {
				log.Errorf("Failed to marshal response: %v", err)
				return
			}
			if err := rp.socket.SendBatch(data); err != nil {
				log.Errorf("Failed to send batch: %v", err)
			}
			sentBatches.Add(p.SeqNum)

		case *ic.EndSignalPayload:
			if sentBatches.GetCardinality() != p.SeqNum {
				log.Errorf(
					"Received end signal but not all batches were sent!\tTotal batches: %d\tSent Batches: %d",
					p.SeqNum, sentBatches.GetCardinality(),
				)
			}
			rp.queryResultReceived(1, 0)

		case *ic.SequenceSetPayload:
			// batches with these sequence numbers had no information for this query
			sentBatches.Or(p.Sequences.Bitmap)

		default:
			log.Errorf("Unexpected payload type")

		}
	}
}

func retainColumns(batch *ic.RowsBatchPayload, query1OutputColumns []string) [][]interface{} {
	colIndices := getColIndices(query1OutputColumns, batch)
	retainedRows := make([][]interface{}, len(batch.Rows))
	for i, row := range batch.Rows {
		newRow := make([]interface{}, len(colIndices))
		for j, colIdx := range colIndices {
			newRow[j] = row[colIdx]
		}
		retainedRows[i] = newRow
	}
	return retainedRows
}

func getColIndices(query1OutputColumns []string, batch *ic.RowsBatchPayload) []int {
	colIndices := make([]int, 0, len(query1OutputColumns))
	for _, colName := range query1OutputColumns {
		for i, batchColName := range batch.ColumnNames {
			if colName == batchColName {
				colIndices = append(colIndices, i)
				break
			}
		}
	}
	return colIndices
}
