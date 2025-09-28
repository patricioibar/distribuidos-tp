package filter

import (
	"errors"
	"time"

	ic "github.com/patricioibar/distribuidos-tp/innercommunication"
)



func filterRowsByYear(batch ic.RowsBatch) (ic.RowsBatch, error) {
	var filteredRows [][]interface{}

	indexYear := -1 
	for i, header := range batch.ColumnNames {
		if header == "year" {
			indexYear = i
			break
		}
	}

	if indexYear == -1 {
		return ic.RowsBatch{}, errors.New("year column not found")
	}

	monthsOfFirstSemester := map[time.Month]bool{
		time.January:   true,
		time.February:  true,
		time.March:     true,
		time.April:     true,
		time.May:       true,
		time.June:      true,
	}

	for _, row := range batch.Rows {
		if len(row) <= indexYear {
			return ic.RowsBatch{}, errors.New("row does not have enough columns")
		}

		tsVal, ok := row[indexYear].(string)
		if !ok {
			return ic.RowsBatch{}, errors.New("year column is not a string")
		}
		timestamp, err := parseTimestamp(tsVal)
		if err != nil {
			return ic.RowsBatch{}, err
		}
		
		if timestamp.Year() == 2024 || timestamp.Year() == 2025 {
			if monthsOfFirstSemester[timestamp.Month()] {
				row = append(row, "FirstSemester")
			} else {
				row = append(row, "SecondSemester")
			}
			filteredRows = append(filteredRows, row)
		}
	}
	batch.ColumnNames = append(batch.ColumnNames, "semester")
	filteredBatch := ic.RowsBatch{
		ColumnNames: batch.ColumnNames,
		JobDone:     batch.JobDone,
		Rows:        filteredRows,
	}
	return filteredBatch, nil
}

func filterRowsByHour(batch ic.RowsBatch) (ic.RowsBatch, error) {
	var filteredRows [][]interface{}

	indexTimestamp := -1 
	for i, header := range batch.ColumnNames {
		if header == "timestamp" {
			indexTimestamp = i
			break
		}
	}

	if indexTimestamp == -1 {
		return ic.RowsBatch{}, errors.New("timestamp column not found")
	}

	for _, row := range batch.Rows {
		if len(row) <= indexTimestamp {
			return ic.RowsBatch{}, errors.New("row does not have enough columns")
		}

		tsVal, ok := row[indexTimestamp].(string)
		if !ok {
			return ic.RowsBatch{}, errors.New("timestamp column is not a string")
		}
		timestamp, err := parseTimestamp(tsVal)
		if err != nil {
			return ic.RowsBatch{}, err
		}

		if timestamp.Hour() >= 6 && timestamp.Hour() < 23 {
			filteredRows = append(filteredRows, row)
		}
	}

	filteredBatch := ic.RowsBatch{
		ColumnNames: batch.ColumnNames,
		JobDone:     batch.JobDone,
		Rows:        filteredRows,
	}
	return filteredBatch, nil
}

func filterRowsByTransactionAmount(batch ic.RowsBatch) (ic.RowsBatch, error) {
	var filteredRows [][]interface{}

	indexAmount := -1 
	for i, header := range batch.ColumnNames {
		if header == "final_amount" {
			indexAmount = i
			break
		}
	}

	if indexAmount == -1 {
		return ic.RowsBatch{}, errors.New("final_amount column not found")
	}

	for _, row := range batch.Rows {
		if len(row) <= indexAmount {
			return ic.RowsBatch{}, errors.New("row does not have enough columns")
		}

		amountVal, ok := row[indexAmount].(float64)
		if !ok {
			return ic.RowsBatch{}, errors.New("final_amount column is not a float64")
		}

		if amountVal >= 75.0 {
			filteredRows = append(filteredRows, row)
		}
	}

	filteredBatch := ic.RowsBatch{
		ColumnNames: batch.ColumnNames,
		JobDone:     batch.JobDone,
		Rows:        filteredRows,
	}
	return filteredBatch, nil


}

func filterTransactionItemsByYear(batch ic.RowsBatch) (ic.RowsBatch, error) {
	var filteredRows [][]interface{}

	indexYear := -1
	for i, header := range batch.ColumnNames {
		if header == "year" {
			indexYear = i
			break
		}
	}

	if indexYear == -1 {
		return ic.RowsBatch{}, errors.New("year column not found")
	}

	for _, row := range batch.Rows {
		if len(row) <= indexYear {
			return ic.RowsBatch{}, errors.New("row does not have enough columns")
		}

		tsVal, ok := row[indexYear].(string)
		if !ok {
			return ic.RowsBatch{}, errors.New("year column is not a string")
		}
		timestamp, err := parseTimestamp(tsVal)
		if err != nil {
			return ic.RowsBatch{}, err
		}

		if timestamp.Year() == 2024 || timestamp.Year() == 2025 {
			filteredRows = append(filteredRows, row)
		}
	}

	filteredBatch := ic.RowsBatch{
		ColumnNames: batch.ColumnNames,
		JobDone:     batch.JobDone,
		Rows:        filteredRows,
	}
	return filteredBatch, nil
}




func parseTimestamp (timestampStr string) (time.Time, error) {
	layout := "2006-01-02 15:04:05"
	return time.Parse(layout, timestampStr)
}