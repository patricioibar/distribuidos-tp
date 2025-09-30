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
	return []RetainedData{
		{
			KeyColumns:   groupByColumns,
			Aggregations: aggregations,
			Data:         groupedData,
		},
	}
}
