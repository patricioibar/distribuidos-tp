package aggfunctions

func NewAggregation(funcName string) Aggregation {
	switch funcName {
	case "sum":
		return NewSumAggregation()
	case "count":
		return NewCountAggregation()
	}
	return nil
}
