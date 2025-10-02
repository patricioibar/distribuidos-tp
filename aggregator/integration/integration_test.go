package integration_test

import (
	utils "aggregator/integration"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func TestInputOutput(t *testing.T) {

	output := utils.SimulateProcessing(
		"test1",
		"amqp://guest:guest@rabbitmq:5672/",
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

func TestBestSellingProducts(t *testing.T) {

	output := utils.SimulateProcessing(
		"test3",
		"amqp://guest:guest@rabbitmq:5672/",
		[]ic.RowsBatch{
			*ic.NewRowsBatch(
				[]string{"year_month", "item_name", "profit"},
				[][]interface{}{
					{"2025-01", "americano", 10.0},
					{"2025-01", "boba", 16.0},
					{"2025-01", "lagrima", 20.1},
					{"2025-02", "cortado", 1.0},
				},
			),
			*ic.NewRowsBatch(
				[]string{"year_month", "item_name", "profit"},
				[][]interface{}{
					{"2025-02", "cortado", 1.0},
					{"2025-02", "cortado", 1.0},
					{"2025-02", "cortado", 1.0},
					{"2025-02", "boba", 160.0},
					{"2025-02", "lagrima", 20.0},
					{"2025-02", "lagrima", 20.0},
				},
			),
			*ic.NewRowsBatch(
				[]string{"year_month", "item_name", "profit"},
				[][]interface{}{
					{"2025-01", "americano", 10.0},
				},
			),
			*ic.NewEndSignal(),
		},
		3,
	)

	utils.AssertBatchesMatch(
		t,
		`{"column_names":["year_month","item_name","count_profit"],"rows":[["2025-01","americano",2],["2025-02","cortado",4]]}`,
		output[0],
	)
	utils.AssertBatchesMatch(
		t,
		`{"column_names":["year_month","item_name","sum_profit"],"rows":[["2025-01","lagrima",20.1],["2025-02","boba",160]]}`,
		output[1],
	)

	utils.AssertIsEndSignal(t, output[2])
}
