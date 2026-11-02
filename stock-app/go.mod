module github.com/NARUBROWN/spine-simple-msa/stock-app

go 1.25.5

require github.com/NARUBROWN/spine v0.6.0

require golang.org/x/net v0.57.0 // indirect

require (
	github.com/NARUBROWN/spine-simple-msa v0.0.0
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	github.com/segmentio/kafka-go v0.4.50 // indirect
)

replace github.com/NARUBROWN/spine-simple-msa => ..
