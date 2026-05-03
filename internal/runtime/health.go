package runtime

import "sync"

const (
	OperationHealthHealthy     = "healthy"
	OperationHealthUnavailable = "unavailable"
)

type OperationHealth struct {
	Status              string
	ConsecutiveFailures int
	LastError           string
}

type OperationHealthTracker struct {
	mu     sync.RWMutex
	states map[string]OperationHealth
}

func NewOperationHealthTracker() *OperationHealthTracker {
	return &OperationHealthTracker{
		states: make(map[string]OperationHealth),
	}
}

func (t *OperationHealthTracker) Status(opID string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	state, ok := t.states[opID]
	if !ok || state.Status == "" {
		return OperationHealthHealthy
	}
	return state.Status
}

func (t *OperationHealthTracker) RecordSuccess(opID string) (OperationHealth, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	previous := t.states[opID]
	next := OperationHealth{Status: OperationHealthHealthy}
	t.states[opID] = next
	return next, previous.Status != "" && previous.Status != next.Status
}

func (t *OperationHealthTracker) RecordFailure(opID string, message string) (OperationHealth, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	previous := t.states[opID]
	next := OperationHealth{
		Status:              OperationHealthUnavailable,
		ConsecutiveFailures: previous.ConsecutiveFailures + 1,
		LastError:           message,
	}
	t.states[opID] = next
	return next, previous.Status != next.Status
}
