package integration_test

import (
	utils "aggregator/integration"
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

	assert_batches_match(
		t,
		`{"column_names":["id","sum_value","count_value"],"rows":[["A",45,3],["B",35,2],["C",25,1]]}`,
		output[0],
	)

	assert_is_end_signal(t, output[1])
}

func assert_is_end_signal(t *testing.T, end_signal string) {
	got, err := ic.RowsBatchFromString(string(end_signal))
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if !got.IsEndSignal() {
		t.Fatalf("Expected end signal, got %v", got)
	}
}

func assert_batches_match(t *testing.T, expectedString string, gotString string) (*ic.RowsBatch, error) {
	expectedBatch, _ := ic.RowsBatchFromString(expectedString)
	got, err := ic.RowsBatchFromString(gotString)
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if len(got.Rows) != len(expectedBatch.Rows) {
		t.Fatalf("Expected %d rows, got %d", len(expectedBatch.Rows), len(got.Rows))
	}
	if len(got.ColumnNames) != len(expectedBatch.ColumnNames) {
		t.Fatalf("Expected %d column names, got %d", len(expectedBatch.ColumnNames), len(got.ColumnNames))
	}

	for _, col := range expectedBatch.ColumnNames {
		found := false
		for _, gotCol := range got.ColumnNames {
			if col == gotCol {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected column name %s not found in output", col)
		}
	}

	for _, expectedRow := range expectedBatch.Rows {
		found := false
		for _, gotRow := range got.Rows {
			match := true
			for i := range expectedRow {
				if expectedRow[i] != gotRow[i] {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected row %v not found in output", expectedRow)
		}
	}
	return got, err
}
