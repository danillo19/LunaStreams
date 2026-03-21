package runtime

import (
	"context"
	"fmt"
	"time"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
)

type Engine struct {
	doc        *ir.Document
	graph      *graph.Graph
	registry   *Registry
	store      *StreamStore
	bus        *EventBus
	logger     *BeautifulLogger
	operators  map[string]Operator
	semaphores map[string]chan struct{}
}

func NewEngine(doc *ir.Document, g *graph.Graph, registry *Registry, logger *BeautifulLogger) (*Engine, error) {
	if doc == nil {
		return nil, fmt.Errorf("ir document is nil")
	}
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if registry == nil {
		return nil, fmt.Errorf("registry is nil")
	}

	engine := &Engine{
		doc:        doc,
		graph:      g,
		registry:   registry,
		store:      NewStreamStore(),
		bus:        NewEventBus(256),
		logger:     logger,
		operators:  make(map[string]Operator, len(doc.Operations)),
		semaphores: make(map[string]chan struct{}, len(doc.Operations)),
	}
	if engine.logger == nil {
		engine.logger = DefaultLogger()
	}

	for _, op := range doc.Operations {
		resolvedOp := resolveOperationSpec(op, g)
		operator, err := registry.Create(resolvedOp)
		if err != nil {
			return nil, err
		}

		engine.operators[op.ID] = operator
		engine.semaphores[op.ID] = make(chan struct{}, 1)
	}

	return engine, nil
}

func resolveOperationSpec(op ir.Operation, g *graph.Graph) ir.Operation {
	if g == nil || op.Impl != "selector.redundant_choice" {
		return op
	}

	config, ok := cloneMap(op.Config)
	if !ok {
		return op
	}

	rawVariants, ok := config["variants"]
	if !ok {
		op.Config = config
		return op
	}

	items, ok := rawVariants.([]any)
	if !ok {
		op.Config = config
		return op
	}

	clonedItems := make([]any, 0, len(items))
	for _, item := range items {
		entry, ok := cloneMap(item)
		if !ok {
			clonedItems = append(clonedItems, item)
			continue
		}

		streamID, _ := entry["stream"].(string)
		if streamID != "" {
			if producerID, exists := g.ProducerByStream[streamID]; exists {
				if producer, exists := g.Operations[producerID]; exists {
					if _, hasCost := entry["cost"]; !hasCost && producer.Domain.Cost != nil {
						entry["cost"] = *producer.Domain.Cost
					}
					if _, hasWeight := entry["weight"]; !hasWeight && producer.Domain.Weight != nil {
						entry["weight"] = *producer.Domain.Weight
					}
				}
			}
		}

		clonedItems = append(clonedItems, entry)
	}

	config["variants"] = clonedItems
	op.Config = config
	return op
}

func cloneMap(value any) (map[string]any, bool) {
	source, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}

	cloned := make(map[string]any, len(source))
	for key, item := range source {
		cloned[key] = item
	}
	return cloned, true
}

func (e *Engine) Start(ctx context.Context) error {
	go e.runDispatcher(ctx)
	go e.shutdownOnContextDone(ctx)
	e.logger.Info("engine", "dispatcher started")

	for _, op := range e.doc.Operations {
		switch {
		case op.Kind == ir.OperationKindSource && op.Mode == ir.OperationModeAlwaysOn:
			e.logger.Info("engine", "starting source %s (%s)", op.ID, op.Impl)
			go e.runAlwaysOnSource(ctx, op)
		case op.Kind == ir.OperationKindSelector && op.Mode == ir.OperationModeAlwaysOn:
			e.logger.Info("engine", "starting selector %s (%s)", op.ID, op.Impl)
			go e.runAlwaysOnSelector(ctx, op)
		}
	}

	return nil
}

func (e *Engine) shutdownOnContextDone(ctx context.Context) {
	<-ctx.Done()

	for opID, operator := range e.operators {
		closer, ok := operator.(ClosableOperator)
		if !ok {
			continue
		}

		if err := closer.Close(); err != nil {
			e.logger.Error(opID, "close failed: %v", err)
			continue
		}

		e.logger.Debug(opID, "closed")
	}
}

func (e *Engine) runDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-e.bus.Events():
			consumers := e.graph.ConsumersByStream[event.StreamID]
			e.logger.Debug("bus", "event stream=%s seq=%d consumers=%v", event.StreamID, event.Seq, consumers)
			for _, opID := range consumers {
				op := e.graph.Operations[opID]
				if op.Mode != ir.OperationModeTaskPerEvent {
					continue
				}

				go e.runTaskOp(ctx, op)
			}
		}
	}
}

func (e *Engine) runAlwaysOnSource(ctx context.Context, op ir.Operation) {
	operator := e.operators[op.ID]

	for {
		if ctx.Err() != nil {
			return
		}

		outputs, err := operator.Run(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.logger.Error(op.ID, "source failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		e.logger.Debug(op.ID, "produced outputs=%s", describeOutputs(outputs))
		if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
			e.logger.Error(op.ID, "publish failed: %v", err)
		}
	}
}

func (e *Engine) runAlwaysOnSelector(ctx context.Context, op ir.Operation) {
	operator := e.operators[op.ID]
	ticker := time.NewTicker(selectorTick(op.Config))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			inputs := make(map[string]any, len(op.Inputs))
			for _, input := range op.Inputs {
				value, _, ok := e.store.Get(input.Stream)
				if !ok {
					inputs[input.Stream] = false
					continue
				}

				inputs[input.Stream] = value
			}

			e.logger.Debug(op.ID, "selector tick inputs=%s", describeInputs(inputs))
			outputs, err := operator.Run(ctx, inputs)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				e.logger.Error(op.ID, "selector failed: %v", err)
				continue
			}

			e.logger.Debug(op.ID, "selector outputs=%s", describeOutputs(outputs))
			if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
				e.logger.Error(op.ID, "publish failed: %v", err)
			}
		}
	}
}

func (e *Engine) runTaskOp(ctx context.Context, op ir.Operation) {
	sem := e.semaphores[op.ID]

	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}:
	default:
		e.logger.Debug(op.ID, "skip run: already in flight")
		return
	}
	defer func() {
		<-sem
	}()

	inputs := make(map[string]any, len(op.Inputs))
	for _, input := range op.Inputs {
		value, _, ok := e.store.Get(input.Stream)
		if ok {
			inputs[input.Stream] = value
		}
	}

	e.logger.Debug(op.ID, "run inputs=%s", describeInputs(inputs))
	outputs, err := e.operators[op.ID].Run(ctx, inputs)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		e.logger.Error(op.ID, "operation failed: %v", err)
		return
	}

	if len(outputs) == 0 {
		e.logger.Debug(op.ID, "completed without outputs")
	} else {
		e.logger.Debug(op.ID, "run outputs=%s", describeOutputs(outputs))
	}
	if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
		e.logger.Error(op.ID, "publish failed: %v", err)
	}
}

func (e *Engine) persistOutputs(ctx context.Context, op ir.Operation, outputs map[string]any) error {
	if len(outputs) == 0 {
		return nil
	}

	for _, output := range op.Outputs {
		value, ok := outputs[output.Stream]
		if !ok {
			continue
		}

		seq := e.store.Set(output.Stream, value)
		e.logger.Debug(op.ID, "store set stream=%s seq=%d value=%s", output.Stream, seq, DescribeValue(value))
		if err := e.bus.Publish(ctx, Event{StreamID: output.Stream, Seq: seq}); err != nil {
			return err
		}
		e.logger.Debug(op.ID, "published event stream=%s seq=%d", output.Stream, seq)
	}

	return nil
}

func selectorTick(config map[string]any) time.Duration {
	if config == nil {
		return 100 * time.Millisecond
	}

	raw, ok := config["tick_ms"]
	if !ok {
		return 100 * time.Millisecond
	}

	switch value := raw.(type) {
	case int:
		if value > 0 {
			return time.Duration(value) * time.Millisecond
		}
	case int64:
		if value > 0 {
			return time.Duration(value) * time.Millisecond
		}
	case float64:
		if value > 0 {
			return time.Duration(value) * time.Millisecond
		}
	}

	return 100 * time.Millisecond
}

func describeInputs(inputs map[string]any) string {
	return describeValueMap(inputs)
}

func describeOutputs(outputs map[string]any) string {
	return describeValueMap(outputs)
}

func describeValueMap(values map[string]any) string {
	if len(values) == 0 {
		return "{}"
	}

	parts := make([]string, 0, len(values))
	for key, value := range values {
		parts = append(parts, fmt.Sprintf("%s=%s", key, DescribeValue(value)))
	}

	return "{" + fmt.Sprintf("%s", joinParts(parts)) + "}"
}

func joinParts(parts []string) string {
	result := ""
	for i, part := range parts {
		if i > 0 {
			result += ", "
		}
		result += part
	}
	return result
}
