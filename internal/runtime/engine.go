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
	health     *OperationHealthTracker
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
	ResetRuntimeTelemetry()

	engine := &Engine{
		doc:        doc,
		graph:      g,
		registry:   registry,
		store:      NewStreamStore(),
		bus:        NewEventBus(256),
		logger:     logger,
		operators:  make(map[string]Operator, len(doc.Operations)),
		semaphores: make(map[string]chan struct{}, len(doc.Operations)),
		health:     NewOperationHealthTracker(),
	}
	if engine.logger == nil {
		engine.logger = DefaultLogger()
	}

	failOpenOps := runtimeFailoverOperationSet(doc, g)
	for _, op := range doc.Operations {
		resolvedOp := resolveOperationSpec(op, g)
		operator, err := registry.Create(resolvedOp)
		if err != nil {
			if _, failOpen := failOpenOps[op.ID]; !failOpen {
				return nil, err
			}
			operator = failedOperator{opID: op.ID, err: err}
			engine.health.RecordFailure(op.ID, err.Error())
		}

		engine.operators[op.ID] = operator
		engine.semaphores[op.ID] = make(chan struct{}, 1)
	}
	for _, op := range doc.Operations {
		PublishRuntimeEvent(OperationDefinedEvent(op))
		if engine.health.Status(op.ID) == OperationHealthUnavailable {
			PublishRuntimeEvent(RuntimeEvent{
				Type:      "operation_health",
				Operation: op.ID,
				Status:    OperationHealthUnavailable,
				Message:   "operation failed during initialization",
			})
		}
	}

	return engine, nil
}

type failedOperator struct {
	opID string
	err  error
}

func (o failedOperator) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	return nil, o.err
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
				entry["producer_operation"] = producerID
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

func runtimeFailoverOperationSet(doc *ir.Document, g *graph.Graph) map[string]struct{} {
	result := make(map[string]struct{})
	if doc == nil || g == nil {
		return result
	}

	for _, op := range doc.Operations {
		if op.Impl != "selector.redundant_choice" {
			continue
		}
		if stringConfigValue(op.Config, "selection_mode", "") != "runtime_failover" {
			continue
		}

		for _, streamID := range selectorVariantStreams(op.Config) {
			collectUpstreamOperations(streamID, g, result)
		}
	}
	return result
}

func collectUpstreamOperations(streamID string, g *graph.Graph, result map[string]struct{}) {
	producerID, ok := g.ProducerByStream[streamID]
	if !ok {
		return
	}
	if _, seen := result[producerID]; seen {
		return
	}
	result[producerID] = struct{}{}

	producer, ok := g.Operations[producerID]
	if !ok {
		return
	}
	for _, input := range producer.Inputs {
		collectUpstreamOperations(input.Stream, g, result)
	}
}

func selectorVariantStreams(config map[string]any) []string {
	if config == nil {
		return nil
	}
	raw, ok := config["variants"].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		streamID, _ := entry["stream"].(string)
		if streamID != "" {
			result = append(result, streamID)
		}
	}
	return result
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
		if op.Mode != ir.OperationModeAlwaysOn {
			continue
		}
		if op.Kind == ir.OperationKindSink {
			continue
		}

		switch {
		case op.Kind == ir.OperationKindSource && len(op.Inputs) == 0:
			e.logger.Info("engine", "starting always_on source %s (%s)", op.ID, op.Impl)
			go e.runAlwaysOnSource(ctx, op)
		default:
			e.logger.Info("engine", "starting always_on op %s (%s)", op.ID, op.Impl)
			go e.runAlwaysOnOp(ctx, op)
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

		e.publishOperationEvent("operation_start", op, "")
		outputs, err := operator.Run(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.recordOperationFailure(op, err)
			e.publishOperationEvent("operation_error", op, err.Error())
			e.logger.Error(op.ID, "source failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		e.recordOperationSuccess(op)
		e.publishOperationEvent("operation_success", op, describeOutputs(outputs))
		e.logger.Debug(op.ID, "produced outputs=%s", describeOutputs(outputs))
		if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
			e.publishOperationEvent("operation_error", op, err.Error())
			e.logger.Error(op.ID, "publish failed: %v", err)
		}
	}
}

func (e *Engine) runAlwaysOnOp(ctx context.Context, op ir.Operation) {
	operator := e.operators[op.ID]
	ticker := time.NewTicker(operationTick(op.Config))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			inputs := e.loadInputs(op)
			e.logger.Debug(op.ID, "always_on tick inputs=%s", describeInputs(inputs))
			e.publishOperationEvent("operation_start", op, describeInputs(inputs))
			outputs, err := operator.Run(ctx, inputs)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				e.recordOperationFailure(op, err)
				e.publishOperationEvent("operation_error", op, err.Error())
				e.logger.Error(op.ID, "always_on operation failed: %v", err)
				continue
			}

			e.recordOperationSuccess(op)
			e.publishOperationEvent("operation_success", op, describeOutputs(outputs))
			e.logger.Debug(op.ID, "always_on outputs=%s", describeOutputs(outputs))
			if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
				e.publishOperationEvent("operation_error", op, err.Error())
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

	inputs := e.loadInputs(op)
	e.logger.Debug(op.ID, "run inputs=%s", describeInputs(inputs))
	e.publishOperationEvent("operation_start", op, describeInputs(inputs))
	outputs, err := e.operators[op.ID].Run(ctx, inputs)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		e.recordOperationFailure(op, err)
		e.publishOperationEvent("operation_error", op, err.Error())
		e.logger.Error(op.ID, "operation failed: %v", err)
		return
	}

	e.recordOperationSuccess(op)
	if len(outputs) == 0 {
		e.publishOperationEvent("operation_success", op, "no outputs")
		e.logger.Debug(op.ID, "completed without outputs")
	} else {
		e.publishOperationEvent("operation_success", op, describeOutputs(outputs))
		e.logger.Debug(op.ID, "run outputs=%s", describeOutputs(outputs))
	}
	if err := e.persistOutputs(ctx, op, outputs); err != nil && ctx.Err() == nil {
		e.publishOperationEvent("operation_error", op, err.Error())
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
		PublishRuntimeEvent(RuntimeEvent{
			Type:      "stream_publish",
			Operation: op.ID,
			Stream:    output.Stream,
			Seq:       seq,
			Message:   DescribeValue(value),
		})
		if err := e.bus.Publish(ctx, Event{StreamID: output.Stream, Seq: seq}); err != nil {
			return err
		}
		e.logger.Debug(op.ID, "published event stream=%s seq=%d", output.Stream, seq)
	}

	return nil
}

func (e *Engine) loadInputs(op ir.Operation) map[string]any {
	inputs := make(map[string]any, len(op.Inputs))
	for _, input := range op.Inputs {
		streamValue, ok := e.store.GetValue(input.Stream)
		if ok {
			inputs[input.Stream] = streamValue.Value
			inputs["__fresh:"+input.Stream] = streamValue.UpdatedAt.IsZero() || time.Since(streamValue.UpdatedAt) <= streamFreshness(op.Config)
		} else {
			inputs["__fresh:"+input.Stream] = false
		}
	}
	if op.Impl == "selector.redundant_choice" {
		for _, producerID := range selectorProducerOperations(op.Config) {
			inputs["__health:"+producerID] = e.health.Status(producerID)
		}
	}
	return inputs
}

func (e *Engine) recordOperationSuccess(op ir.Operation) {
	state, changed := e.health.RecordSuccess(op.ID)
	if !changed {
		return
	}
	PublishRuntimeEvent(RuntimeEvent{
		Type:      "operation_health",
		Operation: op.ID,
		Status:    state.Status,
		Message:   "operation recovered",
	})
}

func (e *Engine) recordOperationFailure(op ir.Operation, err error) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	state, changed := e.health.RecordFailure(op.ID, message)
	if !changed {
		return
	}
	PublishRuntimeEvent(RuntimeEvent{
		Type:      "operation_health",
		Operation: op.ID,
		Status:    state.Status,
		Message:   message,
	})
}

func (e *Engine) publishOperationEvent(eventType string, op ir.Operation, message string) {
	PublishRuntimeEvent(RuntimeEvent{
		Type:      eventType,
		Operation: op.ID,
		Kind:      string(op.Kind),
		Mode:      string(op.Mode),
		Impl:      op.Impl,
		Inputs:    streamIDs(op.Inputs),
		Outputs:   streamIDs(op.Outputs),
		Message:   message,
	})
}

func operationTick(config map[string]any) time.Duration {
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

func streamFreshness(config map[string]any) time.Duration {
	return time.Duration(intConfigValue(config, "freshness_ms", 1500)) * time.Millisecond
}

func selectorProducerOperations(config map[string]any) []string {
	if config == nil {
		return nil
	}
	raw, ok := config["variants"].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		producer, _ := entry["producer_operation"].(string)
		if producer != "" {
			result = append(result, producer)
		}
	}
	return result
}

func intConfigValue(config map[string]any, key string, fallback int) int {
	if config == nil {
		return fallback
	}
	raw, ok := config[key]
	if !ok {
		return fallback
	}
	switch value := raw.(type) {
	case int:
		if value > 0 {
			return value
		}
	case int64:
		if value > 0 {
			return int(value)
		}
	case float64:
		if value > 0 {
			return int(value)
		}
	}
	return fallback
}

func stringConfigValue(config map[string]any, key string, fallback string) string {
	if config == nil {
		return fallback
	}
	raw, ok := config[key]
	if !ok {
		return fallback
	}
	value, ok := raw.(string)
	if !ok {
		return fallback
	}
	return value
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
