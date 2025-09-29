package analyst

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"
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
	end := batchCount + r.BatchSize

	_, _ = reader.Read() // Skip header

	// Skip rows before the batch
	for range batchCount {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil && err != io.EOF {
			return err
		}
	}

	// Read up to BatchSize rows
	for i := batchCount; i < end; i++ {
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
		addRowToData(v, row)
	}

	return nil
}

func addRowToData(v interface{}, row []string) {

	format := "2006-01-02 15:04:05"

	switch v.(type) {
	case *[]Transaction:
		store_id, _ := strconv.Atoi(row[1])
		payment_method_id, _ := strconv.Atoi(row[2])
		voucher_id, _ := strconv.ParseFloat(row[3], 64)
		user_id, _ := strconv.ParseFloat(row[4], 64)
		original_amount, _ := strconv.ParseFloat(row[5], 64)
		discount_applied, _ := strconv.ParseFloat(row[6], 64)
		final_amount, _ := strconv.ParseFloat(row[7], 64)
		created_at, _ := time.Parse(format, row[8])

		*v.(*[]Transaction) = append(*v.(*[]Transaction), Transaction{
			transaction_id:    row[0],
			store_id:          store_id,
			payment_method_id: payment_method_id,
			voucher_id:        voucher_id,
			user_id:           user_id,
			original_amount:   original_amount,
			discount_applied:  discount_applied,
			final_amount:      final_amount,
			created_at:        created_at,
		})
	case *[]TransactionItem:
		item_id, _ := strconv.Atoi(row[1])
		quantity, _ := strconv.Atoi(row[2])
		unit_price, _ := strconv.ParseFloat(row[3], 64)
		subtotal, _ := strconv.ParseFloat(row[4], 64)
		created_at, _ := time.Parse(format, row[5])

		*v.(*[]TransactionItem) = append(*v.(*[]TransactionItem), TransactionItem{
			transaction_id: row[0],
			item_id:        item_id,
			quantity:       quantity,
			unit_price:     unit_price,
			subtotal:       subtotal,
			created_at:     created_at,
		})
	case *[]User:
		user_id, _ := strconv.Atoi(row[0])
		registered_at, _ := time.Parse(format, row[3])

		*v.(*[]User) = append(*v.(*[]User), User{
			user_id:       user_id,
			gender:        row[1],
			birthdate:     row[2],
			registered_at: registered_at,
		})
	case *[]MenuItem:
		item_id, _ := strconv.Atoi(row[0])
		price, _ := strconv.ParseFloat(row[3], 64)
		is_seasonal, _ := strconv.ParseBool(row[4])

		*v.(*[]MenuItem) = append(*v.(*[]MenuItem), MenuItem{
			item_id:        item_id,
			item_name:      row[1],
			category:       row[2],
			price:          price,
			is_seasonal:    is_seasonal,
			available_from: row[5],
			available_to:   row[6],
		})
	}
}
