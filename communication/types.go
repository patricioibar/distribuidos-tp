package communication

type QueryResponseBatch struct {
	QueryId        int        `json:"query-id"`
	SequenceNumber uint64     `json:"sequence-number"`
	Columns        []string   `json:"columns"`
	Rows           [][]string `json:"rows"`
	EOF            bool       `json:"eof"`
}
