package dataretainer

import (
	a "aggregator/common/aggFunctions"
)

type Retaining struct {
	AmountRetained int    `json:"amount-retained" mapstructure:"amount-retained"`
	GroupBy        string `json:"group-by" mapstructure:"group-by"`
	Value          string `json:"value" mapstructure:"value"`
	Largest        bool   `json:"largest" mapstructure:"largest"`
}

type RetainedData struct {
	KeyColumns   []string
	Aggregations []a.AggConfig
	Data         [][]interface{}
}

type DataRetainer interface {
	RetainData(
		groupByColumns []string,
		aggregations []a.AggConfig,
		groupedData map[string][]a.Aggregation,
	) []RetainedData
}
