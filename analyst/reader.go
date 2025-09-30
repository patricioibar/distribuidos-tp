package main

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
	end := batchCount + r.BatchSize + 1

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
	eof_reached := false
	for i := batchCount + 1; i < end; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			eof_reached = true
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

	if eof_reached {
		return io.EOF
	}

	return nil
}

func addRowToData(v interface{}, row []string) {

	format := "2006-01-02 15:04:05"

	switch v := v.(type) {
	case *[]Transaction:
		store_id, _ := strconv.Atoi(row[1])
		payment_method_id, _ := strconv.Atoi(row[2])
		voucher_id, _ := strconv.ParseFloat(row[3], 64)
		user_id, _ := strconv.ParseFloat(row[4], 64)
		original_amount, _ := strconv.ParseFloat(row[5], 64)
		discount_applied, _ := strconv.ParseFloat(row[6], 64)
		final_amount, _ := strconv.ParseFloat(row[7], 64)
		created_at, _ := time.Parse(format, row[8])

		*v = append(*v, Transaction{
			Transaction_id:    row[0],
			Store_id:          store_id,
			Payment_method_id: payment_method_id,
			Voucher_id:        voucher_id,
			User_id:           user_id,
			Original_amount:   original_amount,
			Discount_applied:  discount_applied,
			Final_amount:      final_amount,
			Created_at:        created_at,
		})
	case *[]TransactionItem:
		item_id, _ := strconv.Atoi(row[1])
		quantity, _ := strconv.Atoi(row[2])
		unit_price, _ := strconv.ParseFloat(row[3], 64)
		subtotal, _ := strconv.ParseFloat(row[4], 64)
		created_at, _ := time.Parse(format, row[5])

		*v = append(*v, TransactionItem{
			Transaction_id: row[0],
			Item_id:        item_id,
			Quantity:       quantity,
			Unit_price:     unit_price,
			Subtotal:       subtotal,
			Created_at:     created_at,
		})
	case *[]User:
		user_id, _ := strconv.Atoi(row[0])
		registered_at, _ := time.Parse(format, row[3])

		*v = append(*v, User{
			User_id:       user_id,
			Gender:        row[1],
			Birthdate:     row[2],
			Registered_at: registered_at,
		})
	case *[]MenuItem:
		item_id, _ := strconv.Atoi(row[0])
		price, _ := strconv.ParseFloat(row[3], 64)
		is_seasonal, _ := strconv.ParseBool(row[4])

		*v = append(*v, MenuItem{
			Item_id:        item_id,
			Item_name:      row[1],
			Category:       row[2],
			Price:          price,
			Is_seasonal:    is_seasonal,
			Available_from: row[5],
			Available_to:   row[6],
		})
	}
}
