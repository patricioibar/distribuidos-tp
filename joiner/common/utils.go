package common

import (
	"fmt"

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

// Helper functions
func toInt(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		i := int64(v)
		if float32(i) == v {
			return i, true
		}
	case float64:
		i := int64(v)
		if float64(i) == v {
			return i, true
		}
	case string:
		var i int64
		_, err := fmt.Sscanf(v, "%d", &i)
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

func toFloat(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		var f float64
		_, err := fmt.Sscanf(v, "%f", &f)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func toString(val interface{}) string {
	return fmt.Sprintf("%v", val)
}
