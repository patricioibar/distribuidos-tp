package integration_test

import (
	utils "aggregator/integration"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func TestInputOutput(t *testing.T) {

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
					{"A", "5"},
					{"B", 15},
					{"C", 25},
				},
			),
			*ic.NewRowsBatch(
				[]string{"id", "value"},
				[][]interface{}{
					{"C", 2},
					{"B", 5},
					{"A", 15},
				},
			),
			*ic.NewEndSignal(),
		},
		2,
	)

	utils.AssertBatchesMatch(
		t,
		`{"column_names":["id","sum_value","count_value"],"rows":[["A",60,4],["B",40,3],["C",27,2]]}`,
		output[0],
	)

	utils.AssertIsEndSignal(t, output[1])
}
