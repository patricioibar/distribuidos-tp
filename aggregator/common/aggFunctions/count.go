package aggfunctions

import "fmt"

func NewCountAggregation() *CountAggregation {
	return &CountAggregation{count: 0}
}

type CountAggregation struct {
	count int
}

func (c *CountAggregation) Add(value interface{}) Aggregation {
	// suma uno sin importar si el valor es nulo o no
	c.count++
	return c
}

func (c *CountAggregation) Result() interface{} {
	return c.count
}

func (c *CountAggregation) Set(value interface{}) {
	switch v := value.(type) {
	case int:
		c.count = v
	default:
		// try parse to int
		var parsed int
		_, err := fmt.Sscanf(fmt.Sprintf("%v", value), "%d", &parsed)
		if err == nil {
			c.count = parsed
		}
	}
}
