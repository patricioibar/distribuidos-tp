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
