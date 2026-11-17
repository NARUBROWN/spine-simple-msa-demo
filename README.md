English | [Korean](README.ko.md)

# Spine Order and Inventory Microservices Example

An HTTP order flows through Kafka events and returns an inventory decision. Uses Spine `v0.7.5`, CLI v0.4.4, Echo `v0.2.1`, and Kafka and playground platforms `v0.2.0`.

Implementing HTTP and message consumption separately can repeat JSON decoding, validation, dependency composition, errors, and shutdown at each entry point. This example demonstrates **Spine's typed handler registration and the separation of business rules from transport implementations**.

## Example channels

| Channel | Branch | Purpose |
| --- | --- | --- |
| Preview | [`preview`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/preview) | Development changes and experimental versions |
| RC | [`rc`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/rc) | Release candidates before stable publication |
| Stable | [`main`](https://github.com/NARUBROWN/spine-simple-msa-demo/tree/main) | Public example using stable dependencies; the default branch |

Example code is promoted through `preview → rc → main`. Tags use `vX.Y.Z-preview.N`, `vX.Y.Z-rc.N`, and `vX.Y.Z`. **GitHub Releases are published only for Stable.** Check the selected branch's `go.mod` for the actual Spine version. See [channel checks and publication](RELEASE_CHANNELS.md).

## Code guide

```text
POST /orders {order_id, quantity}
        │ JSON binding, DTO validation, DI
        ▼
 OrderController → OrderService → publish.Event(order.created)
                                          │ Kafka publish during post-processing
                                          ▼
                           OrderConsumer → ReservationService
                                          │ Success: publish result → ACK
                                          │ Failure → NACK → redelivery
        ┌──────── stock.reserved / stock.rejected ─────┘
        ▼
 StockConsumer → OrderTrackerService
        │
 GET /orders/:order_id/status
```

| Responsibility | Code |
| --- | --- |
| Select HTTP, Kafka, and validation; compose constructors and execution policies | [Order BuildApplication](order-app/internal/application/application.go), [inventory BuildApplication](stock-app/internal/application/application.go) |
| Map JSON/path DTOs to response statuses and errors | [OrderController](order-app/src/controller/order_controller.go) |
| Accept orders, detect conflicts, and record domain events | [OrderService](order-app/src/service/order_service.go) |
| Connect logical subscriptions to handlers | [Order result subscriptions](order-app/src/consumer/stock/stock_handler.go), [order creation subscription](stock-app/src/consumer/order/order_handler.go) |
| Make inventory decisions, record attempts, and reuse duplicate decisions | [ReservationService](stock-app/src/service/reservation_service.go) |
| Deduplicate results and transition order state | [OrderTrackerService](order-app/src/service/order_tracker_service.go) |
| Recover inventory decisions after process restarts | [IdempotencyStore](stock-app/internal/store/idempotency_store.go) |
| Share event formats and validation tags | [shared/events](shared/events) |

HTTP handlers receive `spine.Ctx` and `input.Body[dto.CreateOrderRequest]` or `input.Path[dto.OrderPath]`. `path:"order_id"` in `OrderPath` matches the route name. Consumers receive `spine.Ctx`, the event name, and an event DTO. Services receive the standard context from `ctx.ExecutionContext().Context()`. Spine resolves input, invokes the selected validator, and runs the handler built through DI. Services contain no HTTP request or Kafka reader/writer.

HTTP routes and inline callbacks live in `OrderController.RegisterRoutes`, using `/orders` and `/scenarios` groups. There is no separate `src/route` or `app.Route`. Each consumer handler file contains its subscription registration function and receiver methods; application composition invokes that registration function.

```go
app.Constructor(
    constructor.Service(&service.OrderTrackerService{}),
    constructor.Service(&service.OrderService{}),
    constructor.Controller(&controller.OrderController{}),
    constructor.Custom(&stockconsumer.StockConsumer{}),
)
if err := stockconsumer.Register(app); err != nil {
    return nil, boot.Options{}, err
}
```

Register constructors in `BuildApplication`. `Register` in `order-app/src/consumer/stock/stock_handler.go` connects topics to receiver methods in the same file.

```go
func Register(app spine.App) error {
    if err := app.Consumers().Register("stock.reserved", (*StockConsumer).OnReserved); err != nil {
        return err
    }
    return app.Consumers().Register("stock.rejected", (*StockConsumer).OnRejected)
}
```

Components implement `Constructor(dependencies...) *Component`. Event receivers have no dedicated role, so use `constructor.Custom(&Component{})`. For initialization that can return an error, such as the inventory store, register a named factory from the application composition package with `constructor.Custom(NewStockStore)`; it runs during actual preparation. `constructor` is `github.com/NARUBROWN/spine/pkg/constructor`.

Nonnumeric order IDs return HTTP 400 with `{"message":"invalid path field order_id","in":"path"}`. The v0.6.5 example also returned HTTP 400, but with a different message and no `in` field.

`publish.Event` records a domain event in the execution context. Spine publishes during handler post-processing, then ACKs after the consumer handler and post-processing succeed; failures NACK. The Kafka platform protects failed offsets and handles transport. Spine manages reader recreation, backoff, and shutdown order.

The order application selects Echo, Kafka, and a validator. The inventory application selects only Kafka and a validator, with no HTTP platform or listener. Consumers register **logical names** such as `order.created`; the Kafka factory applies `TopicPrefix` once to subscriptions and publishing.

## Running

The playground validator requires Go `1.26.0` or later. Docker is used for the isolated broker and E2E below.

```sh
go install github.com/NARUBROWN/spine-cli/cmd/spine@v0.4.4
spine version
# Download dependencies for each workspace module.
go mod download
go -C order-app mod download
go -C stock-app mod download
```

Install stable CLI `v0.4.4`. CI pins the verified version in `.github/example-channels.json`.

Use an existing Kafka or Redpanda broker, or start this repository's development Redpanda:

```sh
docker compose -f docker-compose.e2e.yml up -d --wait
```

The default exposed port is `19092`. Configure the same broker and prefix in each service terminal.

```sh
export SPINE_DEMO_KAFKA_BROKER=localhost:19092
export SPINE_DEMO_TOPIC_PREFIX=my-demo.
```

Start the inventory worker in terminal 1.

```sh
spine dev --root ./stock-app
```

Start the order server in terminal 2.

```sh
spine dev --root ./order-app
```

The CLI rebuilds on code changes and replaces the process after a successful build. Order Service opens its default `:8080` HTTP listener after consumer startup readiness succeeds. Consumer group partition assignment and result processing continue separately; verify that the first order reaches a completed state.

Clean up the development broker after use.

```sh
docker compose -f docker-compose.e2e.yml down --volumes
```

## From input validation to the order result

Submit an order with a quantity.

```sh
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":101,"quantity":3}'
curl http://localhost:8080/orders/101/status
```

The first response is HTTP `202 Accepted` with `pending`. When the asynchronous result arrives, the state changes to `reserved`.

```json
{"event_id":"order-101-normal-v1","order_id":101,"status":"pending"}
```

`order_id` and `quantity` must be positive. The next request is rejected with HTTP 400 during validation after DTO binding, without creating order state or events. Invalid JSON and field types also return 400.

```sh
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":102,"quantity":0}'
curl -i http://localhost:8080/orders/102/status
# 404: rejected input did not create an order
```

Shared events also have `validate` tags. Invalid Kafka DTOs are rejected before calling the inventory service and follow the NACK policy. Applications without a selected validator do not check these tags automatically.

Existing scenario routes are also available.

| Request | Result |
| --- | --- |
| `POST /orders/:order_id` | Normal order with quantity 1 |
| `POST /orders/:order_id/large` | Quantity 1000 → `rejected`, reason `insufficient stock` |
| `POST /scenarios/transient/:order_id` | First attempt fails → event redelivery → second attempt succeeds |
| `POST /scenarios/poison/:order_id` | Every attempt fails → repeated redelivery |
| `GET /orders/:order_id/status` | Current state; missing orders return 404 |

Inventory decisions are a reduced example with a **per-request quantity limit of 100**. It does not decrement cumulative SKU inventory or resolve inventory contention across orders.

## Failures and duplicates

The transient scenario shows reader recreation without committing the offset after the first attempt fails.

```sh
curl -X POST http://localhost:8080/scenarios/transient/103
curl http://localhost:8080/orders/103/status
```

Look for success with `attempt=2` in the logs. `ConsumerRetry` controls reader recreation after connection, read, or ACK/NACK failure. It neither limits handler invocation counts nor skips poison messages.

Repeating the same request publishes the same `event_id`.

```sh
curl -X POST http://localhost:8080/orders/104
curl -X POST http://localhost:8080/orders/104
```

The inventory worker reuses stored decisions and can republish the result event. The order application deduplicates by result ID. Repeating a completed order does not reset it to `pending`. Reusing an order ID with a different quantity or scenario returns HTTP 409. The inventory worker also rejects the same `event_id` with a different order ID or quantity. A finalized result is not overwritten by another result event.

Demo event IDs use `order-{order_id}-{scenario}-v1`. Production services should use the caller's idempotency key or a persistent UUID with uniqueness constraints.

Run the poison scenario last.

```sh
curl -X POST http://localhost:8080/scenarios/poison/105
```

An uncommitted failed offset can block later messages in the same partition. Invalid DTOs and conflicting events share this boundary. This example does not skip them automatically. Design retry/DLQ topics, manual isolation, and reprocessing policies for production.

## Atomicity of publishing and storage

Spine's pipeline and ACK/NACK do not make database commits and event publishing atomic. Order state lives in memory and disappears on restart. The inventory decision file is a single-process demonstration, not a database shared by workers.

[scenarios/outbox](scenarios/outbox) is a separate reduced implementation that persists the order and outbox record in one file replacement before relaying. It is not automatically applied to the service's `publish.Event` path.

```sh
SPINE_DEMO_KAFKA_BROKER=localhost:19092 \
  go run ./scenarios/outbox/cmd -order-id 201 -quantity 3
```

If the broker is down, `.spine-demo/outbox-state.json` retains the order and unpublished records. Running the command again reuses the order and publishes pending messages. A crash after successful publishing but before completion is recorded can cause duplicate publishing; consumers still need idempotency.

In production, update orders and outbox tables in one database transaction and coordinate relay workers with row locks or leases. Outbox tests also cover restart and republishing after completion persistence fails following a successful publish.

## Startup, shutdown, and configuration

`BuildApplication` and `Validate` do not connect to the broker, start listeners, or open the state file. Run preparation creates the publisher and readers for each subscription. The inventory store is also created during actual preparation.

- Reader reconnection: initially 200ms, maximum 2 seconds, multiplier 2, jitter 20%, unlimited attempts.
- Settlement timeout: `WithEventSettlementTimeout(5*time.Second)`.
- Shutdown: on `SIGINT`/`SIGTERM`, stop new work, drain execution and ACK/NACK, clean up readers, then close the publisher.
- Shutdown wait timeout: 10 seconds.

With unlimited startup retries, distinguish process liveness from readiness. Set a positive `MaxAttempts` when fast failure is required.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `SPINE_DEMO_KAFKA_BROKER` | `localhost:9092` | Broker address |
| `SPINE_DEMO_TOPIC_PREFIX` | Empty | Same physical topic prefix for publishing and subscriptions |
| `SPINE_DEMO_ORDER_GROUP` | `order-service` | Order result consumer group |
| `SPINE_DEMO_STOCK_GROUP` | `stock-service` | Inventory consumer group |
| `SPINE_DEMO_ORDER_ADDRESS` | `SPINE_PORT` or `:8080` | Order HTTP address |
| `SPINE_DEMO_STOCK_STATE` | `.spine-demo/stock-state.json` | Path relative to the worker; use an absolute path for a fixed location |

`AllowInsecureTransport: true` explicitly permits plaintext for isolated local Kafka/Redpanda. Remove it in production and configure TLS, SASL, or `Dialer`/`Transport` in the Kafka platform's Config. Apply the same security policy to consumers and publishers. The example's idempotency does not provide broker access control or authenticate message producers.

## CLI contracts and real broker verification

Each service is an independent Go module and CLI project. The root module contains shared events, the outbox, and E2E; `go.work` connects all three modules. CLI static module checks do not support an active workspace, so use `GOWORK=off` and each service's shared-module `replace`.

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

`check.sh` runs CLI checks for both applications and vet/race checks for all three modules. Root `go test ./...` alone does not test the service modules. Partial probes compare registries and `Validate`; they do not prove broker connectivity or delivery. Static checks do not execute factory settings or validation rules, so they also do not prove runtime network policies.

Docker E2E starts a dedicated Redpanda, runs both real binaries, then removes containers and volumes.

```sh
./scripts/e2e.sh
# If default port 19092 is already in use:
SPINE_DEMO_KAFKA_PORT=29092 ./scripts/e2e.sh
```

Checks cover startup retries before broker readiness, HTTP DTO rejection without state creation, valid order round trips, normal and insufficient-stock decisions, transient failure, duplicates, 409 responses, poison redelivery, and shutdown draining. A separate worker verifies repeated NACK of invalid consumer DTOs without inventory attempts or decisions. Unique topic prefixes per run also verify physical topic wiring.

[security_test.go](stock-app/src/consumer/order/security_test.go) checks required fields, decision republishing after restart, conflicting payloads for the same ID, and concurrent delivery. It validates application idempotency, not infrastructure access control.

## Extend with the CLI

Create an application with the same platform selection:

```sh
spine new order-api --module example.com/order-api --event-platform kafka --validator playground
spine new stock-worker --module example.com/stock-worker --platform none --event-platform kafka --validator playground
```

The generator composes platforms and validation. Implement order/inventory business rules and durability policies yourself. Review additional consumer generation with:

```sh
GOWORK=off spine g consumer restock --root ./stock-app --topic stock.restock --field quantity:int --dry-run --json
```

Review `changes`, `diff`, and `base_hash`, then replace `--dry-run` with `--if-match <base_hash>`. Dependency-only `spine update` does not support this repository's workspace and local replacements; do not apply it directly here. Check the selected recipe and limitations in the [official CLI documentation](https://github.com/NARUBROWN/spine-cli#readme), then verify module changes, source changes, and broker E2E separately.
