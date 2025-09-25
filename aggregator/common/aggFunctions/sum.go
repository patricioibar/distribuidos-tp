package aggfunctions

import "fmt"

func NewSumAggregation() *SumAggregation {
	return &SumAggregation{sum: 0}
}

type SumAggregation struct {
	sum float64
}

func (s *SumAggregation) Add(value interface{}) Aggregation {
	switch v := value.(type) {
	case float64:
		s.sum += v
	case int:
		s.sum += float64(v)
	default:
		// try parse to float64
		var parsed float64
		_, err := fmt.Sscanf(fmt.Sprintf("%v", value), "%f", &parsed)
		if err == nil {
			s.sum += parsed
		}
	}

	return s
}
func (s *SumAggregation) Result() interface{} {
	return s.sum
}
