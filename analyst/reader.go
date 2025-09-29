package analyst

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type Reader struct {
	FilePath  string
	BatchSize int
}

func (r *Reader) getBatch(batchCount int, v interface{}) error {
	file, err := os.Open(r.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows := [][]string{}
	start := batchCount * r.BatchSize
	end := start + r.BatchSize

	// Skip rows before the chunk
	for i := 0; i < start; i++ {
		_, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}

	// Read up to BatchSize rows
	for i := start; i < end; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows = append(rows, record)
	}

	for _, row := range rows {
		//maybe skip header, idk

		store_id, _ := strconv.Atoi(row[1])
		payment_method_id, _ := strconv.Atoi(row[2])
		voucher_id, _ := strconv.ParseFloat(row[3], 64)
		user_id, _ := strconv.ParseFloat(row[4], 64)
		original_amount, _ := strconv.ParseFloat(row[5], 64)
		discount_applied, _ := strconv.ParseFloat(row[6], 64)
		final_amount, _ := strconv.ParseFloat(row[7], 64)
		//created_at, _ := time.Parse(time.RFC3339, row[8])

		*v.(*[]Transaction) = append(*v.(*[]Transaction), Transaction{
			transaction_id:    row[0],
			store_id:          store_id,
			payment_method_id: payment_method_id,
			voucher_id:        voucher_id,
			user_id:           user_id,
			original_amount:   original_amount,
			discount_applied:  discount_applied,
			final_amount:      final_amount,
			//created_at:        created_at,
		})
	}

	return nil
}
