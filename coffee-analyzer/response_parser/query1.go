package responseparser

import (
	c "communication"
	"encoding/json"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
	mw "github.com/patricioibar/distribuidos-tp/middleware"
)

var query1OutputColumns = []string{
	"transaction_id",
	"final_amount",
}

func (rp *ResponseParser) parseQuery1Response() mw.OnMessageCallback {
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

			rows := retainColumns(p, query1OutputColumns)

			parsedBatch := c.QueryResponseBatch{
				QueryId: 1,
				Columns: query1OutputColumns,
				Rows:    genericRowsToStringRows(rows),
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
			rp.queryResultReceived(1, 0)
			done <- nil

		// case *ic.SequenceSetPayload:

		default:
			log.Errorf("Unknown payload type")
			done <- nil

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
