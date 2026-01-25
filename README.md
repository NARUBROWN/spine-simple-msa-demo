# Spine Simple MSA Demo

Spine 프레임워크를 활용한 간단한 마이크로서비스 아키텍처(MSA) 데모 애플리케이션입니다.

## 개요

이 프로젝트는 주문(Order)과 재고(Stock) 서비스 간의 이벤트 기반 통신을 보여주는 간단한 MSA 데모입니다. Order Service에서 주문이 생성되면 Kafka를 통해 이벤트를 발행하고, Stock Service가 해당 이벤트를 구독하여 처리합니다.

## 아키텍처

```
┌─────────────────┐      Kafka Event       ┌─────────────────┐
│  Order Service  │ ──────────────────────> │  Stock Service  │
│   (HTTP API)    │   "order.created"       │    (Consumer)   │
│   Port: 8080    │                         │   Port: 8081    │
└─────────────────┘                         └─────────────────┘
```

### 서비스 구성

- **order-app**: 주문 생성 API를 제공하는 HTTP 서버
  - 엔드포인트: `POST /orders/:order_id`
  - Kafka로 주문 생성 이벤트 발행

- **stock-app**: 주문 이벤트를 소비하는 Kafka Consumer
  - `order.created` 이벤트 구독
  - 주문 정보를 로그로 출력

- **shared**: 두 서비스 간 공유되는 코드 (이벤트 정의 등)

## 사전 요구사항

- Go 1.25.5 이상
- Kafka (localhost:9092)

## 설치

```bash
# 의존성 설치
go mod download
```

## 실행 방법

### 1. Stock Service 실행

```bash
cd stock-app
go run main.go
```

Stock Service가 포트 8081에서 실행되며, Kafka로부터 이벤트를 대기합니다.

### 2. Order Service 실행

새 터미널을 열고:

```bash
cd order-app
go run main.go
```

Order Service가 포트 8080에서 실행됩니다.

## API 사용법

### 주문 생성

```bash
curl -X POST http://localhost:8080/orders/12345
```

성공 시 응답:
```
OK
```

주문이 생성되면:
1. Order Service가 `order.created` 이벤트를 Kafka로 발행
2. Stock Service가 해당 이벤트를 소비하여 로그 출력

Stock Service 콘솔에서 다음과 같은 로그를 확인할 수 있습니다:
```
Kafka Event: order.created
OrderID: 12345
```

## 프로젝트 구조

```
spine-simple-msa-demo/
├── order-app/                 # 주문 서비스
│   ├── controller/
│   │   └── order_controller.go
│   └── main.go
├── stock-app/                 # 재고 서비스
│   ├── consumer/
│   │   └── order_consumer.go
│   └── main.go
├── shared/                    # 공유 코드
│   └── events/
│       ├── order_created.go
│       └── stock_created.go
├── go.mod
└── go.sum
```
