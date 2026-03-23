package planner

import (
	"fmt"
	"strings"
	"time"

	"LunaStreams/internal/ir"
)

const (
	SelectionModeRuntimeBundle          = "runtime_bundle"
	SelectionModeCompileTimeMin         = "compile_time_min_cost"
	SelectionModeSelectedAny            = "selected_any"
	defaultWeightThreshold      float64 = 1.0
)

type PlanResult struct {
	Document  *ir.Document
	Decisions []Decision
}

type Decision struct {
	SelectorID           string
	Strategy             string
	Cost                 float64
	Weight               float64
	SelectedStreams      []string
	SelectedLabels       []string
	DisabledOperationIDs []string
}

type plannedVariant struct {
	label     string
	stream    string
	kind      string
	keyWindow time.Duration
	cost      float64
	weight    float64
	index     int
	raw       map[string]any
}

type plannedChoice struct {
	variants []plannedVariant
	labels   []string
	streams  []string
	cost     float64
	weight   float64
	order    []int
}

func CompileRedundantChoices(doc *ir.Document) (*PlanResult, error) {
	if doc == nil {
		return nil, fmt.Errorf("ir document is nil")
	}

	cloned := cloneDocument(doc)
	producerByStream := buildProducerByStream(cloned)
	operationsByID := buildOperationsByID(cloned)

	var decisions []Decision
	for index := range cloned.Operations {
		op := cloned.Operations[index]
		if op.Impl != "selector.redundant_choice" {
			continue
		}

		mode := stringConfig(op.Config, "selection_mode", SelectionModeRuntimeBundle)
		if mode != SelectionModeCompileTimeMin {
			continue
		}

		variants, err := parsePlannedVariants(op, producerByStream, operationsByID)
		if err != nil {
			return nil, err
		}
		threshold := positiveFloatConfig(op.Config, "selection_weight_threshold", defaultWeightThreshold)
		choice, ok := choosePlannedChoice(variants, threshold)
		if !ok {
			return nil, fmt.Errorf("selector %q has no plan that reaches weight threshold %.2f", op.ID, threshold)
		}

		selectedStreams := make([]string, 0, len(choice.variants))
		selectedInputs := make([]ir.StreamRef, 0, len(choice.variants))
		selectedRawVariants := make([]any, 0, len(choice.variants))
		disabledOps := make([]string, 0)
		selectedProducers := make(map[string]struct{}, len(choice.variants))

		for _, variant := range choice.variants {
			selectedStreams = append(selectedStreams, variant.stream)
			selectedInputs = append(selectedInputs, ir.StreamRef{Stream: variant.stream})
			selectedRawVariants = append(selectedRawVariants, variant.raw)
			if producerID, ok := producerByStream[variant.stream]; ok {
				selectedProducers[producerID] = struct{}{}
			}
		}

		for _, variant := range variants {
			producerID, ok := producerByStream[variant.stream]
			if !ok {
				continue
			}
			if _, selected := selectedProducers[producerID]; !selected {
				disabledOps = append(disabledOps, producerID)
			}
		}

		config := cloneMap(op.Config)
		config["selection_mode"] = SelectionModeSelectedAny
		config["planned_strategy"] = choice.describe()
		config["variants"] = selectedRawVariants

		op.Inputs = selectedInputs
		op.Config = config
		cloned.Operations[index] = op

		decisions = append(decisions, Decision{
			SelectorID:           op.ID,
			Strategy:             choice.describe(),
			Cost:                 choice.cost,
			Weight:               choice.weight,
			SelectedStreams:      append([]string(nil), choice.streams...),
			SelectedLabels:       append([]string(nil), choice.labels...),
			DisabledOperationIDs: uniqueStrings(disabledOps),
		})
	}

	pruned := pruneToRequiredSubgraph(cloned)
	return &PlanResult{
		Document:  pruned,
		Decisions: decisions,
	}, nil
}

func choosePlannedChoice(variants []plannedVariant, threshold float64) (plannedChoice, bool) {
	if len(variants) == 0 {
		return plannedChoice{}, false
	}
	if threshold <= 0 {
		threshold = defaultWeightThreshold
	}

	var best plannedChoice
	bestFound := false
	limit := 1 << len(variants)
	for mask := 1; mask < limit; mask++ {
		candidate := plannedChoice{}
		for index, variant := range variants {
			if mask&(1<<index) == 0 {
				continue
			}
			candidate.variants = append(candidate.variants, variant)
			candidate.labels = append(candidate.labels, variant.label)
			candidate.streams = append(candidate.streams, variant.stream)
			candidate.cost += variant.cost
			candidate.weight += variant.weight
			candidate.order = append(candidate.order, variant.index)
		}

		if candidate.weight < threshold {
			continue
		}
		if !bestFound || candidate.betterThan(best) {
			best = candidate
			bestFound = true
		}
	}

	return best, bestFound
}

func (c plannedChoice) betterThan(other plannedChoice) bool {
	if c.cost != other.cost {
		return c.cost < other.cost
	}
	if c.weight != other.weight {
		return c.weight > other.weight
	}
	if len(c.variants) != len(other.variants) {
		return len(c.variants) < len(other.variants)
	}
	for index := 0; index < len(c.order) && index < len(other.order); index++ {
		if c.order[index] != other.order[index] {
			return c.order[index] < other.order[index]
		}
	}
	return len(c.order) < len(other.order)
}

func (c plannedChoice) describe() string {
	return fmt.Sprintf("%s | cost=%.2f weight=%.2f", strings.Join(c.labels, "+"), c.cost, c.weight)
}

func parsePlannedVariants(op ir.Operation, producerByStream map[string]string, operationsByID map[string]ir.Operation) ([]plannedVariant, error) {
	rawVariants, ok := op.Config["variants"]
	if !ok {
		return nil, fmt.Errorf("selector %q requires config.variants", op.ID)
	}
	items, ok := rawVariants.([]any)
	if !ok {
		return nil, fmt.Errorf("selector %q expects config.variants to be a list", op.ID)
	}

	variants := make([]plannedVariant, 0, len(items))
	for index, item := range items {
		raw, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("selector %q variant[%d] must be an object", op.ID, index)
		}

		entry := cloneMap(raw)
		streamID := stringConfig(entry, "stream", "")
		if streamID == "" {
			return nil, fmt.Errorf("selector %q variant[%d] requires stream", op.ID, index)
		}

		cost, hasCost := floatValue(entry["cost"])
		weight, hasWeight := floatValue(entry["weight"])
		if producerID, ok := producerByStream[streamID]; ok {
			if producer, exists := operationsByID[producerID]; exists {
				if !hasCost && producer.Domain.Cost != nil {
					cost = *producer.Domain.Cost
					hasCost = true
					entry["cost"] = cost
				}
				if !hasWeight && producer.Domain.Weight != nil {
					weight = *producer.Domain.Weight
					hasWeight = true
					entry["weight"] = weight
				}
			}
		}
		if !hasCost {
			cost = 1.0
			entry["cost"] = cost
		}
		if !hasWeight || weight <= 0 {
			weight = 1.0
			entry["weight"] = weight
		}

		variants = append(variants, plannedVariant{
			label:  stringConfig(entry, "label", streamID),
			stream: streamID,
			kind:   stringConfig(entry, "kind", "bool"),
			cost:   cost,
			weight: weight,
			index:  index,
			raw:    entry,
		})
	}

	return variants, nil
}

func pruneToRequiredSubgraph(doc *ir.Document) *ir.Document {
	if doc == nil {
		return nil
	}

	operationsByID := buildOperationsByID(doc)
	producerByStream := buildProducerByStream(doc)
	streamsByID := buildStreamsByID(doc)

	requiredStreams := make(map[string]struct{})
	queue := make([]string, 0)
	for _, op := range doc.Operations {
		if op.Kind != ir.OperationKindSink {
			continue
		}
		for _, input := range op.Inputs {
			if _, exists := requiredStreams[input.Stream]; exists {
				continue
			}
			requiredStreams[input.Stream] = struct{}{}
			queue = append(queue, input.Stream)
		}
	}

	requiredOps := make(map[string]struct{})
	for len(queue) > 0 {
		streamID := queue[0]
		queue = queue[1:]

		producerID, ok := producerByStream[streamID]
		if !ok {
			continue
		}
		if _, exists := requiredOps[producerID]; exists {
			continue
		}
		requiredOps[producerID] = struct{}{}

		op := operationsByID[producerID]
		for _, output := range op.Outputs {
			requiredStreams[output.Stream] = struct{}{}
		}
		for _, input := range op.Inputs {
			if _, exists := requiredStreams[input.Stream]; exists {
				continue
			}
			requiredStreams[input.Stream] = struct{}{}
			queue = append(queue, input.Stream)
		}
	}

	pruned := &ir.Document{
		Operations: make([]ir.Operation, 0, len(doc.Operations)),
		Streams:    make([]ir.Stream, 0, len(doc.Streams)),
	}

	for _, op := range doc.Operations {
		if op.Kind == ir.OperationKindSink {
			keep := false
			for _, input := range op.Inputs {
				if _, exists := requiredStreams[input.Stream]; exists {
					keep = true
					break
				}
			}
			if keep {
				pruned.Operations = append(pruned.Operations, op)
			}
			continue
		}
		if _, exists := requiredOps[op.ID]; exists {
			pruned.Operations = append(pruned.Operations, op)
		}
	}

	for _, stream := range doc.Streams {
		if _, exists := streamsByID[stream.ID]; !exists {
			continue
		}
		if _, exists := requiredStreams[stream.ID]; exists {
			pruned.Streams = append(pruned.Streams, stream)
		}
	}

	return pruned
}

func cloneDocument(doc *ir.Document) *ir.Document {
	cloned := &ir.Document{
		Operations: make([]ir.Operation, 0, len(doc.Operations)),
		Streams:    append([]ir.Stream(nil), doc.Streams...),
	}

	for _, op := range doc.Operations {
		cloned.Operations = append(cloned.Operations, ir.Operation{
			ID:      op.ID,
			Kind:    op.Kind,
			Mode:    op.Mode,
			Impl:    op.Impl,
			Inputs:  append([]ir.StreamRef(nil), op.Inputs...),
			Outputs: append([]ir.StreamRef(nil), op.Outputs...),
			Config:  cloneMap(op.Config),
			Domain:  op.Domain,
		})
	}

	return cloned
}

func buildProducerByStream(doc *ir.Document) map[string]string {
	result := make(map[string]string)
	for _, op := range doc.Operations {
		for _, output := range op.Outputs {
			result[output.Stream] = op.ID
		}
	}
	return result
}

func buildOperationsByID(doc *ir.Document) map[string]ir.Operation {
	result := make(map[string]ir.Operation, len(doc.Operations))
	for _, op := range doc.Operations {
		result[op.ID] = op
	}
	return result
}

func buildStreamsByID(doc *ir.Document) map[string]ir.Stream {
	result := make(map[string]ir.Stream, len(doc.Streams))
	for _, stream := range doc.Streams {
		result[stream.ID] = stream
	}
	return result
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}

	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = cloneValue(value)
	}
	return cloned
}

func cloneSlice(items []any) []any {
	if items == nil {
		return nil
	}
	cloned := make([]any, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneValue(item))
	}
	return cloned
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		return cloneSlice(typed)
	default:
		return typed
	}
}

func floatValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func stringConfig(config map[string]any, key string, fallback string) string {
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

func positiveFloatConfig(config map[string]any, key string, fallback float64) float64 {
	if config == nil {
		return fallback
	}
	value, ok := floatValue(config[key])
	if !ok || value <= 0 {
		return fallback
	}
	return value
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
