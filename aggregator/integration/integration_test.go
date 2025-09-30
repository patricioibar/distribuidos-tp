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
				[]string{"id", "value", "category"},
				[][]interface{}{
					{"A", 10, "cat1"},
					{"B", 20, "cat2"},
					{"A", 30, "cat1"},
				},
			),
			*ic.NewRowsBatch(
				[]string{"id", "value", "category"},
				[][]interface{}{
					{"A", "5", "cat1"},
					{"B", 15, "cat2"},
					{"C", 25, "cat1"},
				},
			),
			*ic.NewRowsBatch(
				[]string{"id", "value", "category"},
				[][]interface{}{
					{"C", 2, "cat1"},
					{"B", 5, "cat2"},
					{"A", 15, "cat1"},
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
