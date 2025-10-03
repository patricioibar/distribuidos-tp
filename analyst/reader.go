package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	"communication"
)

type Reader struct {
	FilePath  string
	BatchSize int
}

func (r *Reader) getHeader() ([]string, error) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (r *Reader) getBatch(batchCount int, columnsIdxs []int) ([][]string, error) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		return nil, err
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
			return nil, err
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
			return nil, err
		}
		filtered := make([]string, len(columnsIdxs))
		skip := false
		for i, idx := range columnsIdxs {
			if idx < 0 || idx >= len(record) {
				skip = true
				break
			}
			filtered[i] = record[idx]
		}
		if skip {
			continue
		}
		rows = append(rows, filtered)
	}

	/*
		for _, row := range rows {
			addRowToData(v, row)
		}*/

	if eof_reached {
		return rows, io.EOF
	}

	return rows, nil
}

func (r *Reader) SendFileTroughSocket(columnsIdxs []int, socket communication.Socket) {
	file, err := os.Open(r.FilePath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", r.FilePath, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, _ = reader.Read() // Skip header

	eof := false
	errCount := 0
	rows := [][]string{}
	for {
		for i := 0; i < r.BatchSize; i++ {
			record, err := reader.Read()
			if err == io.EOF {
				eof = true
				break
			}
			if err != nil {
				errCount++
				if errCount > 5 {
					log.Fatalf("Failed to read record from file %s: %v", r.FilePath, err)
					return
				}
				continue
			}
			filtered := make([]string, len(columnsIdxs))
			skip := false
			for i, idx := range columnsIdxs {
				if idx < 0 || idx >= len(record) {
					skip = true
					break
				}
				if record[idx] == "" {
					filtered[i] = "NULL"
					continue
				}
				filtered[i] = record[idx]
			}
			if skip {
				continue
			}
			rows = append(rows, filtered)
		}

		if len(rows) != 0 {
			sendRowsTroughSocket(rows, socket)
			rows = [][]string{}
		}

		if eof {
			break
		}
	}
}

func addRowToData(v interface{}, row []string) {

	format := "2006-01-02 15:04:05"

	switch v := v.(type) {
	case *[]communication.Transaction:
		store_id, _ := strconv.Atoi(row[1])
		payment_method_id, _ := strconv.Atoi(row[2])
		voucher_id, _ := strconv.ParseFloat(row[3], 64)
		user_id, _ := strconv.ParseFloat(row[4], 64)
		original_amount, _ := strconv.ParseFloat(row[5], 64)
		discount_applied, _ := strconv.ParseFloat(row[6], 64)
		final_amount, _ := strconv.ParseFloat(row[7], 64)
		created_at, _ := time.Parse(format, row[8])

		*v = append(*v, communication.Transaction{
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
	case *[]communication.TransactionItem:
		item_id, _ := strconv.Atoi(row[1])
		quantity, _ := strconv.Atoi(row[2])
		unit_price, _ := strconv.ParseFloat(row[3], 64)
		subtotal, _ := strconv.ParseFloat(row[4], 64)
		created_at, _ := time.Parse(format, row[5])

		*v = append(*v, communication.TransactionItem{
			Transaction_id: row[0],
			Item_id:        item_id,
			Quantity:       quantity,
			Unit_price:     unit_price,
			Subtotal:       subtotal,
			Created_at:     created_at,
		})
	case *[]communication.User:
		user_id, _ := strconv.Atoi(row[0])
		registered_at, _ := time.Parse(format, row[3])

		*v = append(*v, communication.User{
			User_id:       user_id,
			Gender:        row[1],
			Birthdate:     row[2],
			Registered_at: registered_at,
		})
	case *[]communication.MenuItem:
		item_id, _ := strconv.Atoi(row[0])
		price, _ := strconv.ParseFloat(row[3], 64)
		is_seasonal, _ := strconv.ParseBool(row[4])

		*v = append(*v, communication.MenuItem{
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
