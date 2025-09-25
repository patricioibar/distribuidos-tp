package aggfunctions

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
