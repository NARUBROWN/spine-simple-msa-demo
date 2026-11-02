# Spine Simple MSA Demo

Spine `v0.6.0`과 Kafka로 주문, 재고 서비스의 왕복 이벤트 흐름과 전달 신뢰성 경계를 보여주는 예제입니다.

**[Spine CLI](https://github.com/NARUBROWN/spine-cli)로 두 서비스를 빌드하고 검증한 예제입니다.**
주문 서버와 재고 워커를 각각 CLI 프로젝트로 구성했습니다. 서비스 디렉터리에서 `spine dev`를
실행하면 코드 변경을 감지하고, 자동 재빌드가 성공할 때 서비스를 새 코드로 교체합니다.
개발 서버 실행, 실행 계약 검사와 구성 요소 생성을 한 도구로 이어가고 싶다면
[Spine CLI의 명령과 사용 예제](https://github.com/NARUBROWN/spine-cli#readme)를 확인하세요.

## 처리 흐름

```text
POST /orders/:id
        │
        ▼
 Order Service ── order.created ──▶ Stock Service
        ▲                                │
        └── stock.reserved/rejected ─────┘
        │
GET /orders/:id/status
```

- Order Service는 HTTP 요청을 받아 안정적인 `event_id`가 포함된 `order.created`를 발행합니다.
- Stock Service는 파일 기반 멱등 저장소에 처리 시도와 재고 결정을 기록합니다.
- Stock Service는 `stock.reserved` 또는 `stock.rejected`를 발행합니다.
- Order Service는 결과 이벤트를 멱등하게 반영하고 HTTP 상태 조회를 제공합니다.

이 예제의 `event_id`는 `order-{order_id}-{scenario}-v1` 규칙을 사용합니다. 실제 시스템에서는 요청자가 제공하는 idempotency key나 영구 저장소에서 발급한 UUID를 사용하고, 주문 레코드와 함께 유일성 제약을 걸어야 합니다.

## 권장 환경

- 최소 Go 버전: 1.25.5
- 검증한 Go 버전: 1.26.0

## Spine CLI 워크스페이스

공개 Spine CLI `v0.4.1`로 새 프로젝트를 생성하고 기존 API와 처리 로직을 이식했습니다.
Go `1.25.5` 이상에서 다음 명령으로 같은 CLI를 설치할 수 있습니다. 이번 검증에는 Go `1.26.0`을 사용했습니다.

```sh
go install github.com/NARUBROWN/spine-cli/cmd/spine@v0.4.1
spine version
# spine-cli 0.4.1
```

HTTP 앱은 Spine `v0.6.0`, Echo 플랫폼 `v0.1.0`, Echo `v5.4.4`를 사용합니다.
설치와 전체 명령은 [공식 CLI 저장소](https://github.com/NARUBROWN/spine-cli#readme)를 참고하세요.

각 서비스가 독립적인 Go 모듈, CLI 프로젝트입니다. 루트는 공유 이벤트, outbox, E2E 모듈이고
go.work가 세 모듈을 연결합니다. 서비스는 로컬 replace로 공유 모듈을 참조합니다.
CLI의 `--root`에는 `order-app` 또는 `stock-app`을 지정합니다.
공개 CLI의 정적 모듈 검사는 활성 `go.work`를 지원하지 않으므로 계약 검사와 부분 probe에는
`GOWORK=off`를 전달합니다. 서비스의 로컬 `replace`가 공유 모듈을 연결합니다.
`spine dev`, `spine build`와 Kafka E2E는 기존 워크스페이스에서 실행합니다.

```bash
GOWORK=off spine context --root ./order-app --json
GOWORK=off spine contract check --root ./order-app --json
GOWORK=off spine verify --root ./order-app --json
GOWORK=off spine contract probe --root ./order-app --json

GOWORK=off spine context --root ./stock-app --json
GOWORK=off spine contract check --root ./stock-app --json
GOWORK=off spine verify --root ./stock-app --json
GOWORK=off spine contract probe --root ./stock-app --json

./scripts/check.sh
```

BuildApplication은 브로커 접속과 리스너 시작을 하지 않으며, 재고 상태 파일도 열지 않습니다.
평문 로컬 브로커 설정은 INSECURE_TRANSPORT_ENABLED 경고로 표시되며,
--warnings-as-errors를 붙이면 실패합니다. 토픽 접두사는 검사 시점의 환경변수 값으로 해석하므로
CLI 검사와 실제 서비스에 같은 값을 전달해야 합니다. 부분 probe와 외부 Kafka E2E는 별개입니다.

## Spine CLI로 실행하기

Go와 Spine CLI를 설치하고 Kafka 또는 Redpanda를 먼저 실행합니다.
기본 브로커 주소는 `localhost:9092`입니다. 다른 주소를 사용한다면 아래처럼 두 서비스의 터미널에
같은 값을 설정합니다. 다음은 브로커가 `localhost:19092`에서 실행 중인 경우의 예입니다.

```bash
export SPINE_DEMO_KAFKA_BROKER=localhost:19092
```

공유 브로커에서 다른 실행과 topic을 격리하려면 두 프로세스에 같은 접두사를 지정합니다.

```bash
export SPINE_DEMO_TOPIC_PREFIX=my-demo.
```

터미널 1에서 저장소 루트의 `stock-app` 디렉터리로 이동해 Stock Service를 실행합니다.

```bash
cd stock-app
spine dev
```

터미널 2에서 저장소 루트의 `order-app` 디렉터리로 이동해 Order Service를 실행합니다.

```bash
cd order-app
spine dev
```

저장소 루트에서 실행하려면 각각 `spine dev --root ./stock-app`, `spine dev --root ./order-app`을
사용할 수도 있습니다. 실행 중인 터미널과 별도로, 각 서비스 디렉터리에서 다음 명령으로 빌드와 검증을 수행합니다.

```bash
spine build
GOWORK=off spine verify
```

Stock Service는 HTTP 포트를 열지 않는 Kafka worker입니다. Order Service는 Kafka 결과 consumer의 startup validation을 통과한 뒤에만 `:8080` HTTP listener를 엽니다.

## 시나리오

### 정상 예약

```bash
curl -i -X POST http://localhost:8080/orders/101
curl http://localhost:8080/orders/101/status
```

첫 요청은 `202 Accepted`와 `pending`을 반환합니다. 비동기 처리가 끝나면 상태가 `reserved`로 바뀝니다.

### 재고 부족

```bash
curl -i -X POST http://localhost:8080/orders/102/large
curl http://localhost:8080/orders/102/status
```

재고 수량 100보다 큰 주문이므로 결과는 `rejected`, 사유는 `insufficient stock`입니다.

### 일시 실패와 재전달

```bash
curl -i -X POST http://localhost:8080/scenarios/transient/103
curl http://localhost:8080/orders/103/status
```

Stock handler가 첫 처리에서 의도적으로 오류를 반환합니다. Kafka 메시지는 커밋되지 않고 reader가 재생성된 뒤 같은 offset이 다시 전달됩니다. 두 번째 처리에서 성공하며 로그에 `attempt=2`가 나타납니다.

`Kafka.ConsumerRetry`는 reader 연결, 읽기 오류나 실패한 ACK/NACK 뒤의 전송 계층 재생성을 제어합니다. handler 호출 횟수를 제한하거나 poison message를 건너뛰는 정책은 아닙니다.

### 중복 이벤트와 멱등 처리

동일한 요청을 다시 보내면 같은 `event_id`가 발행됩니다.

```bash
curl -X POST http://localhost:8080/orders/101
curl -X POST http://localhost:8080/orders/101
```

Stock Service는 `.spine-demo/stock-state.json`의 기존 결정을 재사용하므로 재고 결정을 다시 수행하지 않습니다. 결과 이벤트는 다시 발행될 수 있으며 Order Service도 결과 `event_id`로 중복을 제거합니다. 실서비스에서는 이 파일 대신 트랜잭션과 유일성 제약을 제공하는 데이터베이스를 사용해야 합니다.

동일한 주문 ID와 동일한 `event_id`를 재요청하면 완료 상태를 `pending`으로 되돌리지 않고 기존 상태를 유지합니다. 반대로 이미 사용된 주문 ID에 다른 시나리오를 요청하면 `409 Conflict`를 반환해 하나의 주문 ID가 서로 다른 이벤트를 가리키지 않도록 합니다.

### Poison message와 파티션 정체

다른 시나리오를 모두 실행한 뒤 마지막에 실행하십시오.

```bash
curl -X POST http://localhost:8080/scenarios/poison/104
```

Stock handler가 계속 오류를 반환하므로 해당 offset은 커밋되지 않습니다. Kafka는 같은 partition의 더 높은 offset만 안전하게 커밋할 수 없으므로 reader가 다시 만들어지고 poison event가 계속 재전달됩니다. 이 데모는 위험을 숨기지 않기 위해 자동 건너뛰기를 하지 않습니다. 운영에서는 재시도 횟수와 별도 retry/DLQ topic, 수동 격리 절차를 애플리케이션 정책으로 설계해야 합니다.

## Transactional outbox 시나리오

`scenarios/outbox`는 주문과 outbox 레코드를 하나의 파일 교체 연산으로 저장한 뒤 Kafka에 릴레이하는 컴파일 가능한 축소 구현입니다.

```bash
SPINE_DEMO_KAFKA_BROKER=localhost:9092 \
  go run ./scenarios/outbox/cmd -order-id 201 -quantity 3
```

브로커가 중단되면 주문과 미발행 outbox 레코드는 `.spine-demo/outbox-state.json`에 남습니다. 같은 명령을 다시 실행하면 주문을 중복 생성하지 않고 대기 중인 메시지를 재발행합니다. 발행 성공과 `published_at` 기록 사이에 프로세스가 종료되면 중복 발행은 여전히 가능하므로 Stock의 멱등 처리가 필요합니다.

이 파일 구현은 원리를 보여주기 위한 것입니다. 운영 환경에서는 주문 테이블과 outbox 테이블을 같은 데이터베이스 트랜잭션에서 변경하고, 여러 relay worker가 경쟁해도 안전하도록 행 잠금이나 lease를 사용해야 합니다.

## Startup readiness와 종료 drain

두 서비스는 `ConsumerRetry`를 명시적으로 설정합니다.

- 최초 지연 200ms, 최대 지연 2초, 배수 2, 지터 20%
- `MaxAttempts`를 생략해 일시적인 브로커 장애에서 무제한 재시도
- consumer startup validation이 끝나기 전에는 Order HTTP listener를 노출하지 않음
- `SIGINT`/`SIGTERM`을 받으면 새 작업을 중단하고 실행 중인 consumer handler와 publisher를 정리

무제한 startup retry는 오케스트레이터가 프로세스 생존과 readiness를 구분한다는 전제입니다. 빠른 실패가 필요한 배포 환경에서는 `MaxAttempts`를 양수로 설정하십시오.

## Docker E2E

다음 명령은 Redpanda 컨테이너를 시작하고 실제 TCP/Kafka E2E를 실행한 뒤 컨테이너와 volume을 제거합니다.

```bash
./scripts/e2e.sh
```

포트가 사용 중이면 변경할 수 있습니다.

```bash
SPINE_DEMO_KAFKA_PORT=29092 ./scripts/e2e.sh
```

E2E는 다음을 검증합니다.

- 브로커가 없을 때 consumer가 준비 완료 상태로 진행하지 않고 startup retry 수행
- `order.created → stock.reserved/rejected` 실제 왕복
- 일시 실패 뒤 같은 이벤트의 재전달과 두 번째 시도 성공
- 동일 `event_id`의 영구 멱등 결정
- poison event가 반복 전달되어 partition을 막는 동작
- 종료 시 Order/Stock consumer drain

기본 검증은 외부 인프라 없이 각 CLI 프로젝트와 루트 모듈을 순회합니다.
루트에서 `go test ./...`만 실행하면 서비스 모듈은 제외되므로 전체 검증에는 다음 스크립트를 사용합니다.

```bash
./scripts/check.sh
```

## 보안 경계

이 저장소의 `AllowInsecureTransport: true`는 격리된 로컬 Kafka/Redpanda 평문 연결을 위한 명시적 opt-in입니다. 인터넷이나 신뢰할 수 없는 네트워크에 그대로 배포하면 안 됩니다. 운영 환경에서는 이 옵션을 제거하고 `KafkaOptions.TLS` 또는 인증 정보를 포함한 `Dialer`와 `Transport`를 함께 구성하십시오. consumer와 publisher 양쪽이 같은 보안 정책을 사용해야 합니다.

## 구조

공개 CLI의 `spine new` 기본 구조와 역할별 생성기로 두 앱을 다시 구성했습니다.
주문 앱은 `platform.New()`로 Echo v5를 선택하고, HTTP 없는 재고 워커는 `spine.New()`를
사용합니다. 재고 워커에는 Echo 플랫폼 의존성과 HTTP 리스너를 추가하지 않습니다.

```text
.
├── go.mod / go.sum                  # 공유 이벤트, outbox와 Kafka E2E
├── go.work                         # 루트, 주문, 재고 모듈 연결
├── order-app/
│   ├── go.mod / go.sum / spine.toml # layered-v1, Echo 플랫폼
│   ├── main.go
│   ├── internal/application/       # 생성자, 라우트, 소비자와 실행 옵션
│   └── src/
│       ├── controller/             # 기존 주문 HTTP 핸들러
│       ├── service/                # 주문 상태와 결과 이벤트 멱등 반영
│       ├── route/route.go          # 기존 5개 HTTP 경로
│       └── consumer/stock/         # stock.reserved / stock.rejected
├── stock-app/
│   ├── go.mod / go.sum / spine.toml # layered-v1, HTTP 없는 워커
│   ├── main.go
│   ├── internal/application/
│   ├── internal/store/             # 기존 파일 기반 멱등 저장소
│   └── src/consumer/order/         # order.created와 지연 저장소 생성자
├── shared/events/
├── scenarios/outbox/
├── e2e/
└── scripts/
    ├── check.sh                    # 각 모듈의 CLI 검사, vet와 race
    └── e2e.sh                      # Redpanda와 실제 Kafka 왕복
```

주문 상태는 `spine g service order-tracker`, 컨트롤러는
`spine g controller order --depends src/service:OrderTrackerService`로 생성한 파일에
옮겼습니다. HTTP 경로는 `spine g route`로 메서드, 경로와 기존 핸들러를 지정해 등록했습니다.
생성자는 `BuildApplication`에서 연결하고 경로는 `route.Register(app)`로 등록합니다.

소비자는 `spine g consumer stock --topic stock.reserved --field event_id:string`과
`spine g consumer order --topic order.created --field event_id:string`으로 생성한 모듈에
기존 구현을 옮겼습니다. 생성된 메시지 대신 기존 `shared/events/` 타입을 사용하며,
`module.go`는 환경변수의 토픽 접두사와 재고 결과의 두 수신 경로를 보존합니다.

추가 소비자의 생성 계획은 다음처럼 확인합니다.

```sh
GOWORK=off spine g consumer restock --root ./stock-app --topic stock.restock --field quantity:int --dry-run --json
```

`changes`, `diff`, `base_hash`를 확인한 뒤 같은 명령에 `--if-match <base_hash>`를 전달합니다.
생성된 핸들러의 이벤트 처리와 재전달 정책은 직접 구현합니다.

CLI는 서비스 디렉터리에서 실행하므로 재고 상태 파일의 기본 위치는
`stock-app/.spine-demo/stock-state.json`입니다. 저장 위치를 고정하려면
`SPINE_DEMO_STOCK_STATE`에 절대 경로를 지정합니다. `SPINE_DEMO_ORDER_ADDRESS`가 없으면
CLI의 `SPINE_PORT`를 사용합니다.
