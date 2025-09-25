package aggfunctions

type Aggregation interface {
	Add(value interface{}) Aggregation
	Result() interface{}
}
