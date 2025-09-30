package analyst

import "time"

type Transaction struct {
	transaction_id    string
	store_id          int
	payment_method_id int
	voucher_id        float64
	user_id           float64
	original_amount   float64
	discount_applied  float64
	final_amount      float64
	created_at        time.Time
}

type TransactionItem struct {
	transaction_id string
	item_id        int
	quantity       int
	unit_price     float64
	subtotal       float64
	created_at     time.Time
}

type User struct {
	user_id       int
	gender        string
	birthdate     string //quizas deberia ser time.Time, formato YYYY-MM-DD
	registered_at time.Time
}

type MenuItem struct {
	item_id        int
	item_name      string
	category       string
	price          float64
	is_seasonal    bool
	available_from string
	available_to   string
}

type FileType int

const (
	Transactions FileType = iota
	TransactionItems
	Users
	MenuItems
)
