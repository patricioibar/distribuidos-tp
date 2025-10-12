package communication

type QueryResponseBatch struct {
	QueryId int        `json:"query-id"`
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
	EOF     bool       `json:"eof"`
}
