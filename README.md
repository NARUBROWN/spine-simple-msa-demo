# Spine 주문, 재고 MSA 예제

HTTP 주문 요청이 Kafka 이벤트를 거쳐 재고 처리 결과로 돌아오는 예제입니다. Spine `v0.7.5`, CLI v0.4.4, Echo `v0.2.1`, Kafka와 playground 플랫폼 `v0.2.0`을 사용합니다.

HTTP와 메시지 소비를 따로 구현하면 JSON 해석, 입력 검증, 의존성 구성, 오류 처리와 종료 흐름을 각 진입점에서 반복하기 쉽습니다. 이 예제는 **핸들러의 타입과 등록으로 실행 흐름을 구성하고, 업무 규칙과 전송 구현을 분리하는 Spine의 방식**을 보여줍니다.

## 예제 채널 선택

| 채널 | 브랜치 | 용도 |
| --- | --- | --- |
| Preview | [`preview`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/preview) | 개발 중인 변경과 실험 버전을 확인합니다. |
| RC | [`rc`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/rc) | 정식 공개 전 릴리스 후보를 확인합니다. |
| Stable | [`main`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/main) | 정식 의존성을 사용하는 공개 예제이며 기본 브랜치입니다. |

예제 코드는 `preview → rc → main` 순서로 승격합니다. 예제 태그는 `vX.Y.Z-preview.N`, `vX.Y.Z-rc.N`, `vX.Y.Z`로 구분하며, **GitHub Release는 Stable에서만 발행합니다.** 실제 Spine 버전은 선택한 브랜치의 `go.mod`를 확인하세요. [채널 검사와 발행 절차](RELEASE_CHANNELS.md)를 참고하세요.

## 코드에서 볼 부분

```text
POST /orders {order_id, quantity}
        │ JSON 바인딩,  DTO 검증,  DI
        ▼
 OrderController → OrderService → publish.Event(order.created)
                                          │ 후처리에서 Kafka 발행
                                          ▼
                           OrderConsumer → ReservationService
                                          │ 성공 후 결과 발행 → ACK
                                          │ 실패 → NACK → 재전달
        ┌──────── stock.reserved / stock.rejected ─────┘
        ▼
 StockConsumer → OrderTrackerService
        │
 GET /orders/:order_id/status
```

| 책임 | 코드 |
|---|---|
| HTTP, Kafka, 검증기 선택, 생성자와 실행 정책 연결 | [주문 BuildApplication](order-app/internal/application/application.go), [재고 BuildApplication](stock-app/internal/application/application.go) |
| JSON DTO와 경로 DTO를 받아 응답 상태와 오류로 변환 | [OrderController](order-app/src/controller/order_controller.go) |
| 주문 접수, 충돌 확인과 도메인 이벤트 기록 | [OrderService](order-app/src/service/order_service.go) |
| 논리 구독 이름과 핸들러 연결 | [주문 결과 구독](order-app/src/consumer/stock/module.go), [주문 생성 구독](stock-app/src/consumer/order/module.go) |
| 재고 결정, 처리 시도 기록과 중복 결정 재사용 | [ReservationService](stock-app/src/service/reservation_service.go) |
| 결과 중복 제거와 주문 상태 전이 | [OrderTrackerService](order-app/src/service/order_tracker_service.go) |
| 프로세스 재시작 후 재고 결정 복구 | [IdempotencyStore](stock-app/internal/store/idempotency_store.go) |
| 공유 이벤트 형식과 검증 태그 | [shared/events](shared/events) |

HTTP 핸들러는 `context.Context`와 본문용 `*dto.CreateOrderRequest` 또는 경로용 `*dto.OrderPath`를 받습니다. `OrderPath`의 `path:"order_id"`는 라우트 이름과 연결됩니다. consumer는 `context.Context`, 이벤트 이름과 `events.OrderCreated`를 받습니다. Spine이 입력을 해석하고 선택된 검증기를 호출한 뒤 DI로 만든 핸들러를 실행합니다. 서비스에는 HTTP request나 Kafka reader/writer가 없습니다.

숫자가 아닌 주문 ID는 HTTP 400과 `{"message":"invalid path field order_id","in":"path"}`를 반환합니다. v0.6.5 예제도 HTTP 400을 반환했지만 오류 메시지가 다르고 `in` 필드는 없었습니다.

`publish.Event`는 실행 context에 도메인 이벤트를 기록합니다. Spine은 핸들러 후처리에서 발행하고, consumer의 핸들러와 후처리가 성공하면 ACK합니다. 실패하면 NACK합니다. Kafka의 실패 offset 보호와 전송은 Kafka 플랫폼이, reader 재생성과 backoff, 종료 순서는 Spine이 담당합니다.

주문 앱은 Echo, Kafka, 검증기를 선택합니다. 재고 앱은 Kafka, 검증기만 선택한 워커이며 HTTP 플랫폼과 리스너를 두지 않습니다. consumer는 `order.created` 같은 **논리 이름**을 등록하고, `TopicPrefix`는 Kafka factory에서 구독과 발행에 한 번만 적용합니다.

## 실행하기

playground 검증기를 선택했으므로 Go `1.26.0` 이상이 필요합니다. Docker는 아래 격리 브로커와 E2E에 사용합니다.

```sh
go install github.com/NARUBROWN/spine-cli/cmd/spine@v0.4.4
spine version
# 워크스페이스의 각 모듈 의존성을 받습니다.
go mod download
go -C order-app mod download
go -C stock-app mod download
```

CLI는 정식 버전 `v0.4.4`를 설치합니다. CI 검증 버전도 `.github/example-channels.json`에 고정합니다.

Kafka나 Redpanda가 이미 있다면 그 주소를 사용합니다. 이 저장소의 개발용 Redpanda는 다음 명령으로 시작할 수 있습니다.

```sh
docker compose -f docker-compose.e2e.yml up -d --wait
```

기본 공개 포트는 `19092`입니다. 각 서비스 터미널에 같은 브로커와 접두사를 설정합니다.

```sh
export SPINE_DEMO_KAFKA_BROKER=localhost:19092
export SPINE_DEMO_TOPIC_PREFIX=my-demo.
```

터미널 1에서 재고 워커를 실행합니다.

```sh
spine dev --root ./stock-app
```

터미널 2에서 주문 서버를 실행합니다.

```sh
spine dev --root ./order-app
```

코드가 바뀌면 CLI가 다시 빌드하고 빌드 성공 후 프로세스를 교체합니다. Order Service는 consumer startup readiness를 통과한 뒤 기본 `:8080` HTTP 리스너를 엽니다. consumer group의 실제 partition 할당과 결과 처리는 별도로 이어지므로 첫 주문의 완료 상태까지 확인하십시오.

개발용 브로커 사용을 마치면 정리합니다.

```sh
docker compose -f docker-compose.e2e.yml down --volumes
```

## 입력 검증부터 주문 결과까지

수량을 지정해 주문을 접수합니다.

```sh
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":101,"quantity":3}'
curl http://localhost:8080/orders/101/status
```

처음 응답은 HTTP `202 Accepted`와 `pending`입니다. 비동기 결과가 도착하면 상태가 `reserved`로 바뀝니다.

```json
{"event_id":"order-101-normal-v1","order_id":101,"status":"pending"}
```

`order_id`와 `quantity`는 양수여야 합니다. 다음 요청은 DTO 바인딩 뒤 검증 단계에서 HTTP 400으로 거부되며 주문 상태와 이벤트를 만들지 않습니다. 잘못된 JSON이나 필드 타입도 400입니다.

```sh
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":102,"quantity":0}'
curl -i http://localhost:8080/orders/102/status
# 404: 거부된 입력으로 주문이 만들어지지 않음
```

공유 이벤트에도 `validate` 태그를 적용했습니다. 잘못된 Kafka DTO는 재고 서비스 호출 전에 거부되고 NACK 정책으로 이어집니다. 검증기를 선택하지 않은 앱에서는 이 태그가 자동 검사되지 않습니다.

기존 시나리오 경로도 사용할 수 있습니다.

| 요청 | 결과 |
|---|---|
| `POST /orders/:order_id` | 수량 1의 정상 주문 |
| `POST /orders/:order_id/large` | 수량 1000 → `rejected`, 사유 `insufficient stock` |
| `POST /scenarios/transient/:order_id` | 첫 처리 실패 → 같은 이벤트 재전달 → 두 번째 성공 |
| `POST /scenarios/poison/:order_id` | 매번 실패 → 계속 재전달 |
| `GET /orders/:order_id/status` | 현재 상태, 없는 주문은 404 |

재고 판정은 **요청별 수량 상한 100**을 사용하는 축소 예제입니다. SKU별 누적 재고 차감이나 여러 주문 간 재고 경쟁을 구현하지 않습니다.

## 실패와 중복을 확인하기

일시 실패는 첫 시도의 오류로 offset을 커밋하지 않고 reader를 재생성하는 흐름을 보여줍니다.

```sh
curl -X POST http://localhost:8080/scenarios/transient/103
curl http://localhost:8080/orders/103/status
```

로그에서 `attempt=2`의 성공을 확인할 수 있습니다. `ConsumerRetry`는 연결, 읽기, ACK/NACK 실패 뒤 reader를 재생성하는 정책입니다. 핸들러 호출 횟수를 제한하거나 poison message를 건너뛰는 정책이 아닙니다.

같은 요청을 다시 보내면 같은 `event_id`가 발행됩니다.

```sh
curl -X POST http://localhost:8080/orders/104
curl -X POST http://localhost:8080/orders/104
```

재고 워커는 저장된 결정을 재사용하고 결과 이벤트를 다시 발행할 수 있습니다. 주문 앱도 결과 ID로 중복을 제거합니다. 같은 주문의 완료 상태는 재요청 때문에 `pending`으로 돌아가지 않습니다. 이미 사용한 주문 ID에 다른 수량이나 시나리오를 요청하면 HTTP 409입니다. 재고 워커도 같은 `event_id`에 다른 주문 ID나 수량이 오면 오류를 반환합니다. 한번 확정된 주문 결과는 다른 결과 이벤트로 덮어쓰지 않습니다.

데모 이벤트 ID는 `order-{order_id}-{scenario}-v1`입니다. 실제 서비스에서는 요청자의 idempotency key나 영구 저장소의 UUID와 유일성 제약을 사용하십시오.

마지막에 poison 시나리오를 실행합니다.

```sh
curl -X POST http://localhost:8080/scenarios/poison/105
```

실패 offset이 커밋되지 않아 같은 partition의 뒤 메시지가 정체될 수 있습니다. 잘못된 DTO와 상충하는 이벤트도 이 경계를 공유합니다. 이 예제는 자동 건너뛰기를 하지 않습니다. 운영에서는 retry/DLQ 토픽과 수동 격리, 재처리 정책을 설계해야 합니다.

## 발행과 저장의 원자성

Spine의 실행 파이프라인과 ACK/NACK이 데이터베이스 커밋과 이벤트 발행을 원자적으로 만들지는 않습니다. 주문 앱의 상태는 메모리에 있어 재시작하면 사라집니다. 재고 결정 파일도 단일 프로세스 데모이며 다중 워커가 공유할 DB가 아닙니다.

[scenarios/outbox](scenarios/outbox)는 주문과 outbox 레코드를 하나의 파일 교체로 저장한 뒤 릴레이하는 별도 축소 구현입니다. 서비스의 `publish.Event` 경로에 자동으로 적용되지는 않습니다.

```sh
SPINE_DEMO_KAFKA_BROKER=localhost:19092 \
  go run ./scenarios/outbox/cmd -order-id 201 -quantity 3
```

브로커가 중단되면 `.spine-demo/outbox-state.json`에 주문과 미발행 레코드가 남습니다. 같은 명령을 다시 실행하면 기존 주문을 재사용하고 대기 메시지를 발행합니다. 발행 성공 후 완료 기록 전에 종료되면 중복 발행이 가능하므로 consumer 멱등 처리가 계속 필요합니다.

운영 환경에서는 주문, outbox 테이블을 같은 DB 트랜잭션에서 변경하고 릴레이 worker 간 행 잠금이나 lease를 구성해야 합니다. outbox 테스트는 발행 성공 뒤 완료 표시 저장에 실패한 상태에서의 재시작, 재발행도 검사합니다.

## 시작, 종료와 환경 설정

`BuildApplication`과 `Validate`는 브로커 접속, 리스너 시작, 상태 파일 열기를 하지 않습니다. `Run`의 준비 과정에서 publisher와 구독별 reader를 준비합니다. 재고 상태 저장소도 실제 실행 준비에서 생성합니다.

- reader 재연결: 최초 200ms, 최대 2초, 배수 2, 지터 20%, 무제한 재시도
- 전달 확인 제한 시간: `WithEventSettlementTimeout(5*time.Second)`
- 종료: `SIGINT`/`SIGTERM` 후 새 작업 중단, 진행 중인 실행, ACK/NACK, reader 정리, publisher 종료
- 종료 대기 제한 시간: 10초

무제한 startup retry를 사용하는 배포에서는 프로세스 생존과 readiness를 구분해야 합니다. 빠른 실패가 필요하면 `MaxAttempts`를 양수로 설정합니다.

| 환경변수 | 기본값 | 의미 |
|---|---|---|
| `SPINE_DEMO_KAFKA_BROKER` | `localhost:9092` | 브로커 주소 |
| `SPINE_DEMO_TOPIC_PREFIX` | 비어 있음 | 발행, 구독에 같은 물리 토픽 접두사 적용 |
| `SPINE_DEMO_ORDER_GROUP` | `order-service` | 주문 결과 consumer group |
| `SPINE_DEMO_STOCK_GROUP` | `stock-service` | 재고 consumer group |
| `SPINE_DEMO_ORDER_ADDRESS` | `SPINE_PORT` 또는 `:8080` | 주문 HTTP 주소 |
| `SPINE_DEMO_STOCK_STATE` | `.spine-demo/stock-state.json` | 워커 기준 상대 경로. 고정 위치에는 절대 경로 지정 |

`AllowInsecureTransport: true`는 격리된 로컬 Kafka/Redpanda 평문 연결을 위한 명시적 선택입니다. 운영 환경에서는 이 옵션을 제거하고 Kafka 플랫폼 `Config`의 TLS, SASL 또는 `Dialer`, `Transport`를 구성합니다. consumer와 publisher 양쪽에 같은 보안 정책이 필요합니다. 브로커 접근 제어와 메시지 발신자 인증은 이 예제의 멱등 처리로 제공되지 않습니다.

## CLI 계약과 실제 브로커 검증

각 서비스는 독립 Go 모듈과 CLI 프로젝트입니다. 루트 모듈은 공유 이벤트, outbox, E2E를 담고 `go.work`가 세 모듈을 연결합니다. CLI 정적 모듈 검사는 활성 workspace를 지원하지 않으므로 `GOWORK=off`와 서비스의 공유 모듈 `replace`를 사용합니다.

```sh
GOWORK=off spine context --root ./order-app --json
GOWORK=off spine contract check --root ./order-app --warnings-as-errors --json
GOWORK=off spine verify --root ./order-app --warnings-as-errors --json
GOWORK=off spine build --root ./order-app
GOWORK=off spine contract probe --root ./order-app --json

GOWORK=off spine context --root ./stock-app --json
GOWORK=off spine contract check --root ./stock-app --warnings-as-errors --json
GOWORK=off spine verify --root ./stock-app --warnings-as-errors --json
GOWORK=off spine build --root ./stock-app
GOWORK=off spine contract probe --root ./stock-app --json

./scripts/check.sh
```

`check.sh`는 두 앱의 CLI 검사와 세 모듈의 vet, race 테스트를 실행합니다. 루트의 `go test ./...`만으로는 서비스 모듈을 검사하지 않습니다. 부분 probe는 registry와 `Validate`를 대조하며 브로커 연결이나 전달 성공을 검증하지 않습니다. 정적 검사는 factory 설정값, 검증 규칙을 실행하지 않으므로 실제 네트워크 정책도 증명하지 않습니다.

Docker E2E는 전용 Redpanda를 시작하고 실제 바이너리 두 개를 실행한 뒤 컨테이너와 volume을 정리합니다.

```sh
./scripts/e2e.sh
# 기본 포트 19092가 사용 중이면:
SPINE_DEMO_KAFKA_PORT=29092 ./scripts/e2e.sh
```

검사 범위는 브로커 미준비 시 startup retry, HTTP DTO 거부 후 상태 미생성, 유효 DTO의 주문 왕복, 정상, 재고 부족, 일시 실패, 중복, 409 응답, poison 재전달과 종료 drain입니다. 별도 워커로 잘못된 consumer DTO의 반복 NACK과 재고 처리 시도, 결정 미생성도 확인합니다. 각 실행의 고유 topic prefix로 물리 토픽 연결을 함께 검증합니다.

[security_test.go](stock-app/src/consumer/order/security_test.go)는 필수 필드 오류, 재시작 후 결정 재발행, 같은 ID의 상충 내용과 동시 전달을 검사합니다. 이 테스트는 애플리케이션의 멱등 동작에 대한 증거이며 인프라의 접근 제어를 검사하지 않습니다.

## CLI로 확장하기

같은 플랫폼 선택으로 새 앱을 시작하는 명령은 다음과 같습니다.

```sh
spine new order-api --module example.com/order-api --event-platform kafka --validator playground
spine new stock-worker --module example.com/stock-worker --platform none --event-platform kafka --validator playground
```

생성기는 플랫폼, 검증기 조립을 만들며 주문, 재고 업무 규칙과 내구성 정책은 직접 구현합니다. 추가 consumer는 다음과 같이 계획을 검토합니다.

```sh
GOWORK=off spine g consumer restock --root ./stock-app --topic stock.restock --field quantity:int --dry-run --json
```

`changes`, `diff`, `base_hash`를 확인하고 `--dry-run`을 `--if-match <base_hash>`로 바꿔 적용합니다. 의존성만 갱신하는 `spine update`는 현재의 workspace와 로컬 replace 프로젝트를 지원하지 않으므로 이 저장소에 바로 적용하지 않습니다. [공식 CLI 문서](https://github.com/NARUBROWN/spine-cli#readme)에서 선택한 레시피와 한계를 확인하고 모듈 변경, 소스 변경과 브로커 E2E를 각각 검증하십시오.
