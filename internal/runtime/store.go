package runtime

import (
	"sync"
	"time"
)

type StreamValue struct {
	Value     any
	Seq       uint64
	UpdatedAt time.Time
}

type StreamStore struct {
	mu     sync.RWMutex
	values map[string]StreamValue
}

func NewStreamStore() *StreamStore {
	return &StreamStore{
		values: make(map[string]StreamValue),
	}
}

func (s *StreamStore) Set(streamID string, value any) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.values[streamID]
	current.Seq++
	current.Value = value
	current.UpdatedAt = time.Now()
	s.values[streamID] = current

	return current.Seq
}

func (s *StreamStore) Get(streamID string) (any, uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.values[streamID]
	if !ok {
		return nil, 0, false
	}

	return value.Value, value.Seq, true
}

func (s *StreamStore) GetValue(streamID string) (StreamValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.values[streamID]
	return value, ok
}
