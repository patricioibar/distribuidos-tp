package aggfunctions

type AggConfig struct {
	Col  string `json:"col" mapstructure:"col"`
	Func string `json:"func" mapstructure:"func"`
}

type Aggregation interface {
	Add(value interface{}) Aggregation
	Result() interface{}
}
