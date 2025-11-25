package common

import (
	a "aggregator/common/aggFunctions"
	"fmt"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

const KeyPartsSeparator = "|"
const AggFuncColSeparator = "_"

func getAggColIndexes(config *Config, batch *ic.RowsBatchPayload) map[string]int {
	var aggColIndexes map[string]int = make(map[string]int)
	for _, agg := range config.Aggregations {
		for i, colName := range batch.ColumnNames {
			if colName == agg.Col {
				aggColIndexes[agg.Col] = i
				break
			}
		}
	}
	return aggColIndexes
}

func getGroupByColIndexes(config *Config, batch *ic.RowsBatchPayload) []int {
	return getIndexes(config.GroupBy, batch.ColumnNames)
}

func aggsAsColNames(aggs []a.AggConfig) []string {
	var colNames []string
	for _, agg := range aggs {
		colNames = append(colNames, agg.Col)
	}
	return colNames
}

func getIndexes(colNames []string, allColNames []string) []int {
	var indexes []int
	for _, colName := range colNames {
		indexes = append(indexes, getIndex(colName, allColNames))
	}
	return indexes
}

func getIndex(colName string, colNames []string) int {
	for i, name := range colNames {
		if name == colName {
			return i
		}
	}
	return -1
}

func getGroupByKey(groupByIndexes []int, row []interface{}) string {
	var keyParts []string
	for _, idx := range groupByIndexes {
		stringKey := fmt.Sprintf("%v", row[idx])
		keyParts = append(keyParts, stringKey)
	}
	return joinParts(keyParts, KeyPartsSeparator)
}

func joinParts(keyParts []string, separator string) string {
	key := ""
	if len(keyParts) > 0 {
		key = keyParts[0]
		for i := 1; i < len(keyParts); i++ {
			key += separator + keyParts[i]
		}
	}
	return key
}

func getBatchFromAggregatedRows(
	groupByColNames []string,
	aggregations []a.AggConfig,
	isReducer bool,
	aggregatedRows *[][]interface{},
) *ic.RowsBatchPayload {
	var aggregatedColumnNames []string

	aggregatedColumnNames = append(aggregatedColumnNames, groupByColNames...)

	for _, agg := range aggregations {
		var aggColName string
		if isReducer {
			aggColName = agg.Col
		} else {
			aggColName = joinParts([]string{agg.Func, agg.Col}, AggFuncColSeparator)
		}
		aggregatedColumnNames = append(aggregatedColumnNames, aggColName)
	}

	return &ic.RowsBatchPayload{
		ColumnNames: aggregatedColumnNames,
		Rows:        *aggregatedRows,
	}
}
