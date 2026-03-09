package runtime

import "context"

type Event struct {
	StreamID string
	Seq      uint64
}

type EventBus struct {
	ch chan Event
}

func NewEventBus(buffer int) *EventBus {
	return &EventBus{
		ch: make(chan Event, buffer),
	}
}

func (b *EventBus) Publish(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.ch <- event:
		return nil
	}
}

func (b *EventBus) Events() <-chan Event {
	return b.ch
}
