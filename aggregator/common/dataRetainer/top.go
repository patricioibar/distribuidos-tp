package dataretainer

import (
	a "aggregator/common/aggFunctions"
)

type TopRetainer struct {
	retainings []Retaining
}

func (tr *TopRetainer) RetainData(
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
) []RetainedData {
	panic("not implemented")
}
