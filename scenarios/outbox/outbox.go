package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
)

type Publisher interface {
	Publish(context.Context, string, []byte) error
}

type Order struct {
	OrderID   int64     `json:"order_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

type Message struct {
	ID          string     `json:"id"`
	Topic       string     `json:"topic"`
	Payload     []byte     `json:"payload"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
}

type state struct {
	Orders map[int64]Order `json:"orders"`
	Outbox []Message       `json:"outbox"`
}

type Store struct {
	mu    sync.Mutex
	path  string
	state state
}

func Open(path string) (*Store, error) {
	s := &Store{path: path, state: state{Orders: make(map[int64]Order)}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read outbox state: %w", err)
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return nil, fmt.Errorf("decode outbox state: %w", err)
	}
	if s.state.Orders == nil {
		s.state.Orders = make(map[int64]Order)
	}
	return s, nil
}

// PlaceOrder는 주문 상태와 발행 대기 이벤트를 같은 파일 교체 연산으로 저장합니다.
func (s *Store) PlaceOrder(orderID int64, quantity int) (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.state.Orders[orderID]; exists {
		for _, message := range s.state.Outbox {
			if message.ID == fmt.Sprintf("outbox-order-%d-v1", orderID) {
				return message, false, nil
			}
		}
		return Message{}, false, fmt.Errorf("order %d exists without an outbox message", orderID)
	}

	now := time.Now().UTC()
	event := events.OrderCreated{
		EventID:  fmt.Sprintf("outbox-order-%d-v1", orderID),
		OrderID:  orderID,
		Quantity: quantity,
		Scenario: events.ScenarioNormal,
		At:       now,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return Message{}, false, fmt.Errorf("encode order event: %w", err)
	}
	message := Message{ID: event.EventID, Topic: event.Name(), Payload: payload, CreatedAt: now}
	s.state.Orders[orderID] = Order{OrderID: orderID, Quantity: quantity, CreatedAt: now}
	s.state.Outbox = append(s.state.Outbox, message)
	if err := s.persistLocked(); err != nil {
		delete(s.state.Orders, orderID)
		s.state.Outbox = s.state.Outbox[:len(s.state.Outbox)-1]
		return Message{}, false, err
	}
	return message, true, nil
}

// Relay는 미발행 레코드만 순서대로 전송하고 성공한 항목을 영구 표시합니다.
// 발행 성공 직후 프로세스가 종료되면 같은 이벤트가 다시 발행될 수 있으므로 소비자는 멱등해야 합니다.
func (s *Store) Relay(ctx context.Context, publisher Publisher) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	published := 0
	for i := range s.state.Outbox {
		message := &s.state.Outbox[i]
		if message.PublishedAt != nil {
			continue
		}
		if err := publisher.Publish(ctx, message.Topic, message.Payload); err != nil {
			return published, fmt.Errorf("publish outbox message %s: %w", message.ID, err)
		}
		now := time.Now().UTC()
		message.PublishedAt = &now
		if err := s.persistLocked(); err != nil {
			message.PublishedAt = nil
			return published, fmt.Errorf("mark outbox message %s published: %w", message.ID, err)
		}
		published++
	}
	return published, nil
}

func (s *Store) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create outbox directory: %w", err)
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode outbox state: %w", err)
	}
	temporaryPath := s.path + ".tmp"
	if err := os.WriteFile(temporaryPath, data, 0o600); err != nil {
		return fmt.Errorf("write outbox state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace outbox state: %w", err)
	}
	return nil
}
