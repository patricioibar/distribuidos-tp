package dataretainer

import (
	a "aggregator/common/aggFunctions"
	cmp "cmp"
)

type TopRetainer struct {
	retainings []Retaining
}

func (tr *TopRetainer) RetainData(
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
) []RetainedData {
	var results []RetainedData

	for _, retaining := range tr.retainings {
		valueIdx := getAggIndex(retaining.Value, aggregations)

		var result RetainedData
		switch a.GetTypeOfAgg(aggregations[valueIdx].Func) {
		case "int":
			result = retain[int](
				groupByColumns,
				aggregations,
				groupedData,
				valueIdx,
				retaining.GroupBy,
				retaining.AmountRetained,
				retaining.Largest,
			)
		case "float64":
			result = retain[float64](
				groupByColumns,
				aggregations,
				groupedData,
				valueIdx,
				retaining.GroupBy,
				retaining.AmountRetained,
				retaining.Largest,
			)
		}

		results = append(results, result)
	}

	return results
}

func retain[V cmp.Ordered](
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
	valueIdx int,
	groupBy string,
	amountRetained int,
	largest bool,
) RetainedData {
	switch groupBy {
	case "*", "":
		return retainWithoutGrouping[V](
			groupByColumns,
			aggregations,
			groupedData,
			valueIdx,
			amountRetained,
			largest,
		)
	default:
		return retainGrouping[V](
			groupByColumns,
			aggregations,
			groupedData,
			valueIdx,
			groupBy,
			amountRetained,
			largest,
		)
	}

}

func retainWithoutGrouping[V cmp.Ordered](
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
	valueIdx int,
	amountRetained int,
	largest bool,
) RetainedData {
	topValues := NewTopN[V](amountRetained, largest)

	for key, aggs := range groupedData {
		topValues.Insert(
			Entry[V]{
				Key:   key,
				Value: aggs[valueIdx].Result().(V),
				Aggs:  aggs,
			},
		)
	}

	result := RetainedData{
		KeyColumns:   groupByColumns,
		Aggregations: []a.AggConfig{aggregations[valueIdx]},
		Data:         make([][]interface{}, 0),
	}

	for _, entry := range topValues.Values() {
		row := getRowFromKeyAndAggregations(entry.Key.(string), entry.Aggs)
		result.Data = append(result.Data, row)
	}

	return result
}

func retainGrouping[V cmp.Ordered](
	groupByColumns []string,
	aggregations []a.AggConfig,
	groupedData map[string][]a.Aggregation,
	valueIdx int,
	groupBy string,
	amountRetained int,
	largest bool,
) RetainedData {
	groupedTops := make(map[string]*TopN[V])
	keyIdx := getGroupByIndex(groupBy, groupByColumns)

	for key, aggs := range groupedData {
		keyParts := getPartsFromKey(key, KeyPartsSeparator)
		groupKey := keyParts[keyIdx]

		if _, exists := groupedTops[groupKey]; !exists {
			groupedTops[groupKey] = NewTopN[V](amountRetained, largest)
		}

		groupedTops[groupKey].Insert(
			Entry[V]{
				Key:   key,
				Value: aggs[valueIdx].Result().(V),
				Aggs:  aggs,
			},
		)
	}

	result := RetainedData{
		KeyColumns:   groupByColumns,
		Aggregations: []a.AggConfig{aggregations[valueIdx]},
		Data:         make([][]interface{}, 0),
	}

	for _, topValues := range groupedTops {
		for _, entry := range topValues.Values() {
			row := getRowFromKeyAndAggregations(entry.Key.(string), entry.Aggs)
			result.Data = append(result.Data, row)
		}
	}

	return result
}
