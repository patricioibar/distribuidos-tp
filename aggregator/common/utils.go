package common

import (
	a "aggregator/common/aggFunctions"
	"fmt"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func getAggregatedRowsFromGroupedData(groupedData *map[string][]a.Aggregation) *[][]interface{} {
	var result [][]interface{}
	for key, aggs := range *groupedData {
		row := []interface{}{key}
		for _, agg := range aggs {
			row = append(row, agg.Result())
		}
		result = append(result, row)
	}
	return &result
}

func getAggColIndexes(config *Config, batch *ic.RowsBatch) map[string]int {
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

func getGroupByColIndexes(config *Config, batch *ic.RowsBatch) []int {
	var groupByIndexes []int
	for _, groupByCol := range config.GroupBy {
		for i, colName := range batch.ColumnNames {
			if colName == groupByCol {
				groupByIndexes = append(groupByIndexes, i)
				break
			}
		}
	}
	return groupByIndexes
}

func getGroupByKey(groupByIndexes []int, row []interface{}) string {
	var keyParts []string
	for _, idx := range groupByIndexes {
		stringKey := fmt.Sprintf("%v", row[idx])
		keyParts = append(keyParts, stringKey)
	}
	return joinParts(keyParts, "-")
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

func getBatchFromAggregatedRows(config *Config, aggregatedRows *[][]interface{}) *ic.RowsBatch {
	var aggregatedColumnNames []string

	groupedColName := joinParts(config.GroupBy, "-")
	aggregatedColumnNames = append(aggregatedColumnNames, groupedColName)

	for _, agg := range config.Aggregations {
		aggColName := joinParts([]string{agg.Func, agg.Col}, "_")
		aggregatedColumnNames = append(aggregatedColumnNames, aggColName)
	}

	return &ic.RowsBatch{
		ColumnNames: aggregatedColumnNames,
		Rows:        *aggregatedRows,
	}
}
