package aggfunctions

type AggConfig struct {
	Col  string `json:"col" mapstructure:"col"`
	Func string `json:"func" mapstructure:"func"`
}

type Aggregation interface {
	Add(value interface{}) Aggregation
	Result() interface{}
}

func GetTypeOfAgg(funcName string) string {
	switch funcName {
	case "sum":
		return "float64"
	case "count":
		return "int"
	default:
		return "unknown"
	}
}
