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
		batch, err := ic.RowsBatchFromString(jsonStr)

		if err != nil {
			log.Errorf("Failed to unmarshal message: %v", err)
			done <- nil
			return
		}

		if batch.IsEndSignal() {
			log.Infof("Received end signal for query 1")
			done <- nil
			rp.queryDone[0] <- struct{}{}
			return
		}

		rows := retainColumns(batch, query1OutputColumns)

		parsedBatch := c.QueryResponseBatch{
			QueryId: 1,
			Columns: query1OutputColumns,
			Rows:    anyRowsToStringRows(rows),
		}

		data, err := json.Marshal(parsedBatch)
		if err != nil {
			log.Errorf("Failed to marshabatch.ColumnNamesl response: %v", err)
			done <- nil
			return
		}
		if err := rp.socket.SendBatch(data); err != nil {
			log.Errorf("Failed to send batch: %v", err)
		}
		done <- nil
	}
}

func retainColumns(batch *ic.RowsBatch, query1OutputColumns []string) [][]any {
	colIndices := getColIndices(query1OutputColumns, batch)
	retainedRows := make([][]any, len(batch.Rows))
	for i, row := range batch.Rows {
		newRow := make([]any, len(colIndices))
		for j, colIdx := range colIndices {
			newRow[j] = row[colIdx]
		}
		retainedRows[i] = newRow
	}
	return retainedRows
}

func getColIndices(query1OutputColumns []string, batch *ic.RowsBatch) []int {
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
