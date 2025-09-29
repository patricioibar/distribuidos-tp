package integration_test

import (
	utils "aggregator/integration"
	"reflect"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func Test1OneAggregator(t *testing.T) {

	output := utils.SimulateProcessing(
		"test1",
		"amqp://guest:guest@localhost:5672/",
		[]ic.RowsBatch{
			*ic.NewRowsBatch(
				[]string{"id", "value"},
				[][]interface{}{
					{"A", 10},
					{"B", 20},
					{"A", 30},
				},
			),
			*ic.NewRowsBatch(
				[]string{"id", "value"},
				[][]interface{}{
					{"A", 5},
					{"B", 15},
					{"C", 25},
				},
			),
			*ic.NewEndSignal(),
		},
		2,
	)

	expected := []string{
		`{"column_names":["id","sum_value","count_value"],"rows":[["A",45,3],["B",35,2],["C",25,1]]}`,
		`{"end_signal":true}`,
	}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Unexpected output for test1: got %v, want %v", output, expected)
	}
}
