package runtime

import (
	"sync"
	"time"

	"LunaStreams/internal/ir"
)

const telemetryHistoryLimit = 256

type RuntimeEvent struct {
	Type      string   `json:"type"`
	AtUnixMs  int64    `json:"at_unix_ms"`
	Operation string   `json:"operation,omitempty"`
	Kind      string   `json:"kind,omitempty"`
	Mode      string   `json:"mode,omitempty"`
	Impl      string   `json:"impl,omitempty"`
	Inputs    []string `json:"inputs,omitempty"`
	Outputs   []string `json:"outputs,omitempty"`
	Stream    string   `json:"stream,omitempty"`
	Seq       uint64   `json:"seq,omitempty"`
	Status    string   `json:"status,omitempty"`
	Strategy  string   `json:"strategy,omitempty"`
	Cost      float64  `json:"cost,omitempty"`
	Weight    float64  `json:"weight,omitempty"`
	Message   string   `json:"message,omitempty"`
}

type TelemetryHub struct {
	mu          sync.RWMutex
	nextID      int
	topology    map[string]RuntimeEvent
	history     []RuntimeEvent
	clients     map[int]chan RuntimeEvent
	topologySeq []string
}

func NewTelemetryHub() *TelemetryHub {
	return &TelemetryHub{
		topology: make(map[string]RuntimeEvent),
		clients:  make(map[int]chan RuntimeEvent),
	}
}

func (h *TelemetryHub) Publish(event RuntimeEvent) {
	if event.AtUnixMs == 0 {
		event.AtUnixMs = time.Now().UnixMilli()
	}

	h.mu.Lock()
	if event.Type == "operation_defined" && event.Operation != "" {
		if _, exists := h.topology[event.Operation]; !exists {
			h.topologySeq = append(h.topologySeq, event.Operation)
		}
		h.topology[event.Operation] = event
	}
	h.history = append(h.history, event)
	if len(h.history) > telemetryHistoryLimit {
		h.history = h.history[len(h.history)-telemetryHistoryLimit:]
	}

	clients := make([]chan RuntimeEvent, 0, len(h.clients))
	for _, ch := range h.clients {
		clients = append(clients, ch)
	}
	h.mu.Unlock()

	for _, ch := range clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *TelemetryHub) Subscribe(buffer int) (<-chan RuntimeEvent, func()) {
	if buffer <= 0 {
		buffer = 64
	}

	ch := make(chan RuntimeEvent, buffer)

	h.mu.Lock()
	id := h.nextID
	h.nextID++
	h.clients[id] = ch
	topology := make([]RuntimeEvent, 0, len(h.topologySeq))
	for _, opID := range h.topologySeq {
		if event, ok := h.topology[opID]; ok {
			topology = append(topology, event)
		}
	}
	history := append([]RuntimeEvent(nil), h.history...)
	h.mu.Unlock()

	events := append(topology, history...)
	for _, event := range events {
		select {
		case ch <- event:
		default:
			return ch, unsubscribeTelemetry(h, id)
		}
	}

	return ch, unsubscribeTelemetry(h, id)
}

func (h *TelemetryHub) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.topology = make(map[string]RuntimeEvent)
	h.topologySeq = nil
	h.history = nil
}

func unsubscribeTelemetry(h *TelemetryHub, id int) func() {
	return func() {
		h.mu.Lock()
		delete(h.clients, id)
		h.mu.Unlock()
	}
}

var defaultTelemetryHub = NewTelemetryHub()

func PublishRuntimeEvent(event RuntimeEvent) {
	defaultTelemetryHub.Publish(event)
}

func ResetRuntimeTelemetry() {
	defaultTelemetryHub.Reset()
}

func SubscribeRuntimeEvents(buffer int) (<-chan RuntimeEvent, func()) {
	return defaultTelemetryHub.Subscribe(buffer)
}

func OperationDefinedEvent(op ir.Operation) RuntimeEvent {
	return RuntimeEvent{
		Type:      "operation_defined",
		Operation: op.ID,
		Kind:      string(op.Kind),
		Mode:      string(op.Mode),
		Impl:      op.Impl,
		Inputs:    streamIDs(op.Inputs),
		Outputs:   streamIDs(op.Outputs),
	}
}

func streamIDs(refs []ir.StreamRef) []string {
	if len(refs) == 0 {
		return nil
	}
	result := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.Stream != "" {
			result = append(result, ref.Stream)
		}
	}
	return result
}
