package common

import (
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func findColumnIndex(columnName string, columns []string) int {
	for i, col := range columns {
		if col == columnName {
			return i
		}
	}
	return -1
}
