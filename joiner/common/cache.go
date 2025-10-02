package common

import "fmt"

type TableCache struct {
	Columns []string
	Rows    [][]interface{}
}

func NewTableCache(columns []string) (*TableCache, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns cannot be empty")
	}
	return &TableCache{
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}, nil
}

func (tc *TableCache) AddRow(row []interface{}) error {
	if len(row) != len(tc.Columns) {
		return fmt.Errorf("row length %d does not match columns length %d", len(row), len(tc.Columns))
	}

	tc.Rows = append(tc.Rows, row)
	return nil
}
