package events

const (
	OrderCreatedDestination  = "order.created"
	StockReservedDestination = "stock.reserved"
	StockRejectedDestination = "stock.rejected"
)

// Destinations는 각 앱이 발행하는 논리 이름만 Kafka 팩토리에 전달합니다.
type Destinations []string

func (d Destinations) PublishDestinations() []string { return d }
