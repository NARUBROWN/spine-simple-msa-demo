package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Decision struct {
	EventID  string    `json:"event_id"`
	OrderID  int64     `json:"order_id"`
	Quantity int       `json:"quantity"`
	Outcome  string    `json:"outcome"`
	Reason   string    `json:"reason,omitempty"`
	At       time.Time `json:"at"`
}

type persistedState struct {
	Decisions map[string]Decision `json:"decisions"`
	Attempts  map[string]int      `json:"attempts"`
}

type IdempotencyStore struct {
	mu    sync.Mutex
	path  string
	state persistedState
}

func NewIdempotencyStore(path string) (*IdempotencyStore, error) {
	s := &IdempotencyStore{
		path: path,
		state: persistedState{
			Decisions: make(map[string]Decision),
			Attempts:  make(map[string]int),
		},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *IdempotencyStore) Decision(eventID string) (Decision, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	decision, ok := s.state.Decisions[eventID]
	return decision, ok
}

func (s *IdempotencyStore) Attempt(eventID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Attempts[eventID]++
	if err := s.persistLocked(); err != nil {
		s.state.Attempts[eventID]--
		return 0, err
	}
	return s.state.Attempts[eventID], nil
}

func (s *IdempotencyStore) Save(decision Decision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.state.Decisions[decision.EventID]; exists {
		return nil
	}
	s.state.Decisions[decision.EventID] = decision
	if err := s.persistLocked(); err != nil {
		delete(s.state.Decisions, decision.EventID)
		return err
	}
	return nil
}

func (s *IdempotencyStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read idempotency state: %w", err)
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return fmt.Errorf("decode idempotency state: %w", err)
	}
	if s.state.Decisions == nil {
		s.state.Decisions = make(map[string]Decision)
	}
	if s.state.Attempts == nil {
		s.state.Attempts = make(map[string]int)
	}
	return nil
}

func (s *IdempotencyStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create idempotency state directory: %w", err)
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode idempotency state: %w", err)
	}
	temporaryPath := s.path + ".tmp"
	if err := os.WriteFile(temporaryPath, data, 0o600); err != nil {
		return fmt.Errorf("write idempotency state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace idempotency state: %w", err)
	}
	return nil
}
