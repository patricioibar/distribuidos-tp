package main

import (
	"encoding/json"
	"time"
)

type Transaction struct {
	Transaction_id    string
	Store_id          int
	Payment_method_id int
	Voucher_id        float64
	User_id           float64
	Original_amount   float64
	Discount_applied  float64
	Final_amount      float64
	Created_at        time.Time
}

type TransactionItem struct {
	Transaction_id string
	Item_id        int
	Quantity       int
	Unit_price     float64
	Subtotal       float64
	Created_at     time.Time
}

type User struct {
	User_id       int
	Gender        string
	Birthdate     string //quizas deberia ser time.Time, formato YYYY-MM-DD
	Registered_at time.Time
}

type MenuItem struct {
	Item_id        int
	Item_name      string
	Category       string
	Price          float64
	Is_seasonal    bool
	Available_from string
	Available_to   string
}

/*
type FileType int

const (
	Transactions FileType = iota
	TransactionItems
	Users
	MenuItems
)
*/

type Message struct {
	Type string
	Data json.RawMessage
}
