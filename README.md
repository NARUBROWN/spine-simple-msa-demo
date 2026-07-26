# Spine Simple MSA Demo

Spine 프레임워크를 활용한 간단한 마이크로서비스 아키텍처(MSA) 데모 애플리케이션입니다.

이 예제는 Spine `v0.5.1`을 사용합니다.

## 개요

이 프로젝트는 주문(Order)과 재고(Stock) 서비스 간의 이벤트 기반 통신을 보여주는 간단한 MSA 데모입니다. Order Service에서 주문이 생성되면 Kafka를 통해 이벤트를 발행하고, Stock Service가 해당 이벤트를 구독하여 처리합니다.

## 아키텍처

```
┌─────────────────┐      Kafka Event        ┌─────────────────┐
│  Order Service  │ ──────────────────────> │  Stock Service  │
│   (HTTP API)    │   "order.created"       │    (Consumer)   │
│   Port: 8080    │                         │  Kafka Worker  │
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
- Kafka (localhost:9092, 로컬 개발용 평문 연결)

다른 브로커 주소를 사용할 때는 두 서비스에 동일한 환경변수를 지정하십시오.

```bash
export SPINE_DEMO_KAFKA_BROKER=localhost:19092
```

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

Stock Service는 HTTP 포트를 열지 않고 Kafka 이벤트를 대기합니다.

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

Spine의 Kafka 전달은 최소 한 번(at-least-once) 방식입니다. 실제 재고 변경 로직을 추가할 때는 `OrderID` 같은 안정적인 키로 중복 처리를 방지해야 합니다. 영구적으로 실패하는 레코드는 해결될 때까지 해당 파티션 진행을 막을 수 있습니다.

Stock Service 콘솔에서 다음과 같은 로그를 확인할 수 있습니다:
```
Kafka Event: order.created
OrderID: 12345
```

이 저장소의 `AllowInsecureTransport: true` 설정은 `localhost:9092` 개발 환경만을 위한 것입니다. 운영 환경에서는 이 옵션을 제거하고 Kafka TLS를 구성하십시오.

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
