package dataretainer

import (
	a "aggregator/common/aggFunctions"
)

type AllRetainer struct{}

func (ar *AllRetainer) RetainData(
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
) []RetainedData {
	data := getRowsFromGroupedData(&groupedData)
	return []RetainedData{
		{
			KeyColumns:   groupByColumns,
			Aggregations: aggregations,
			Data:         data,
		},
	}
}

func getRowsFromGroupedData(groupedData *map[string][]a.Aggregation) [][]interface{} {
	var result [][]interface{}
	for key, aggs := range *groupedData {
		row := getRowFromKeyAndAggregations(key, aggs)
		result = append(result, row)
	}
	return result
}
