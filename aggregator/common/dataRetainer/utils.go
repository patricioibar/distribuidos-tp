package dataretainer

import (
	a "aggregator/common/aggFunctions"
	"strings"
)

const KeyPartsSeparator = "|"

func getRowFromKeyAndAggregations(key string, aggs []a.Aggregation) []interface{} {
	row := []interface{}{}
	keyParts := getPartsFromKey(key, KeyPartsSeparator)
	for _, part := range keyParts {
		row = append(row, part)
	}
	for _, agg := range aggs {
		row = append(row, agg.Result())
	}
	return row
}

func getPartsFromKey(key string, separator string) []string {
	return strings.Split(key, separator)
}

func getAggIndex(colName string, aggs []a.AggConfig) int {
	for i, agg := range aggs {
		if agg.Col == colName {
			return i
		}
	}
	return -1
}

func getGroupByIndex(colName string, groupByCols []string) int {
	for i, col := range groupByCols {
		if col == colName {
			return i
		}
	}
	return -1
}
