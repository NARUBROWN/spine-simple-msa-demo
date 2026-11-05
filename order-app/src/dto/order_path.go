package dto

// OrderPath는 라우트의 order_id를 이름으로 연결합니다.
type OrderPath struct {
	OrderID int64 `path:"order_id"`
}
