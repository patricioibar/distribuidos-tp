package integration_test

import (
	utils "joiner/integration"
	"testing"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)

func TestInputOutput(t *testing.T) {
	rightInput := []ic.RowsBatch{
		{
			ColumnNames: []string{"store_id", "user_id", "purchases_qty"},
			Rows: [][]any{
				{"store-1", "john", 10},
				{"store-2", "mary", 20},
				{"store-3", "david", 30},
				{"store-4", "sam", 30},
			},
		},
		*ic.NewEndSignal(),
	}

	leftInput := []ic.RowsBatch{
		{
			ColumnNames: []string{"birth_date", "account_type", "user_id"},
			Rows: [][]any{
				{"1990-01-01", "premium", "john"},
				{"1985-07-15", "basic", "alice"},
				{"1988-11-23", "premium", "bob"},
				{"1993-05-30", "basic", "carol"},
				{"1982-09-12", "premium", "eve"},
				{"1995-12-01", "basic", "frank"},
				{"1987-03-18", "premium", "grace"},
				{"1991-08-25", "basic", "heidi"},
			},
		},
		{
			ColumnNames: []string{"birth_date", "account_type", "user_id"},
			Rows: [][]any{
				{"1986-04-10", "premium", "ivan"},
				{"1994-06-22", "basic", "judy"},
				{"1991-02-02", "basic", "mary"},
				{"1992-03-03", "premium", "david"},
				{"1989-10-05", "premium", "mallory"},
				{"1992-01-14", "basic", "oscar"},
				{"1983-02-28", "premium", "peggy"},
				{"1996-07-19", "basic", "trent"},
				{"1984-12-09", "premium", "victor"},
				{"1990-03-03", "basic", "walter"},
			},
		},
		{
			ColumnNames: []string{"birth_date", "account_type", "user_id"},
			Rows: [][]any{
				{"1981-05-21", "premium", "sybil"},
				{"1993-09-17", "basic", "zara"},
				{"1987-11-11", "premium", "nina"},
				{"1992-04-04", "basic", "quinn"},
				{"1985-08-08", "premium", "ruth"},
				{"1988-08-23", "basic", "sam"},
			},
		},
		*ic.NewEndSignal(),
	}

	output := utils.SimulateProcessing(
		"test1",
		"amqp://guest:guest@rabbitmq:5672/",
		rightInput,
		leftInput,
		3,
	)

	utils.AssertOutputMatches(
		t,
		[]string{"store_id", "birth_date", "purchases_qty"},
		[][]any{
			{"store-1", "1990-01-01", 10},
			{"store-2", "1991-02-02", 20},
			{"store-3", "1992-03-03", 30},
			{"store-4", "1988-08-23", 30},
		},
		output[0], output[1],
	)

	utils.AssertIsEndSignal(t, output[3])
}
