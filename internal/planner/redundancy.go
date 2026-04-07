package planner

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"LunaStreams/internal/ir"
)

const (
	SelectionModeSelectedAny         = "selected_any"
	defaultWeightThreshold   float64 = 1.0
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
	label        string
	stream       string
	kind         string
	keyWindow    time.Duration
	cost         float64
	weight       float64
	latencyMS    int
	index        int
	producerID   string
	operationIDs []string
	sourceInputs []string
	raw          map[string]any
}

type plannedChoice struct {
	variants  []plannedVariant
	labels    []string
	streams   []string
	cost      float64
	weight    float64
	latencyMS int
	order     []int
}

type planningContext struct {
	availableInputs map[string]struct{}
	minWeight       float64
	maxLatencyMS    int
	maxCost         float64
	hasMaxCost      bool
	primary         string
	secondary       string
}

func CompileRedundantChoices(doc *ir.Document) (*PlanResult, error) {
	if doc == nil {
		return nil, fmt.Errorf("ir document is nil")
	}

	cloned := cloneDocument(doc)
	producerByStream := buildProducerByStream(cloned)
	operationsByID := buildOperationsByID(cloned)
	operationProfiles := buildOperationProfiles(cloned.Task.OperationProfiles)
	selectorTasks := buildSelectorTasks(cloned.Task.Selectors)
	requiredOps := requiredOperationIDs(cloned, taskOutputStreams(cloned.Task.Outputs))
	context := buildPlanningContext(cloned.Task)

	var decisions []Decision
	for index := range cloned.Operations {
		op := cloned.Operations[index]
		if op.Impl != "selector.redundant_choice" {
			continue
		}
		if len(requiredOps) > 0 {
			if _, required := requiredOps[op.ID]; !required {
				continue
			}
		}

		if selectorTask, exists := selectorTasks[op.ID]; exists {
			op = applySelectorTask(op, selectorTask)
		}

		variants, err := parsePlannedVariants(op, producerByStream, operationsByID, operationProfiles)
		if err != nil {
			return nil, err
		}
		for variantIndex := range variants {
			variant := &variants[variantIndex]
			variant.producerID = producerByStream[variant.stream]
			variant.operationIDs, variant.sourceInputs = collectVariantClosure(cloned, variant.stream)
		}

		choice, ok := choosePlannedChoice(variants, operationsByID, operationProfiles, context, op.Config)
		if !ok {
			return nil, fmt.Errorf("selector %q has no feasible plan for task constraints", op.ID)
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
			if producerID := variant.producerID; producerID != "" {
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

	pruned := pruneToRequiredSubgraph(cloned, taskOutputStreams(cloned.Task.Outputs))
	return &PlanResult{
		Document:  pruned,
		Decisions: decisions,
	}, nil
}

func choosePlannedChoice(variants []plannedVariant, operationsByID map[string]ir.Operation, profiles map[string]ir.OperationProfile, context planningContext, selectorConfig map[string]any) (plannedChoice, bool) {
	if len(variants) == 0 {
		return plannedChoice{}, false
	}

	minWeight := context.minWeight
	if minWeight <= 0 {
		minWeight = positiveFloatConfig(selectorConfig, "selection_weight_threshold", defaultWeightThreshold)
	}
	maxLatencyMS := context.maxLatencyMS
	if maxLatencyMS <= 0 {
		maxLatencyMS = nonNegativeIntConfig(selectorConfig, "max_total_latency_ms", 0)
	}

	candidates := make([]plannedVariant, 0, len(variants))
	for _, variant := range variants {
		if !variantAvailable(variant, context.availableInputs) {
			continue
		}
		candidates = append(candidates, variant)
	}
	if len(candidates) == 0 {
		return plannedChoice{}, false
	}

	var best plannedChoice
	bestFound := false
	limit := 1 << len(candidates)
	for mask := 1; mask < limit; mask++ {
		candidate := plannedChoice{}
		operationIDs := make(map[string]struct{})
		producerOverrides := make(map[string]plannedVariant)
		for index, variant := range candidates {
			if mask&(1<<index) == 0 {
				continue
			}
			candidate.variants = append(candidate.variants, variant)
			candidate.labels = append(candidate.labels, variant.label)
			candidate.streams = append(candidate.streams, variant.stream)
			candidate.order = append(candidate.order, variant.index)
			if variant.producerID != "" {
				producerOverrides[variant.producerID] = variant
			}
			for _, opID := range variant.operationIDs {
				operationIDs[opID] = struct{}{}
			}
		}

		candidate.cost, candidate.weight, candidate.latencyMS = aggregateChoiceMetrics(operationIDs, producerOverrides, operationsByID, profiles)
		if candidate.weight < minWeight {
			continue
		}
		if maxLatencyMS > 0 && candidate.latencyMS > maxLatencyMS {
			continue
		}
		if context.hasMaxCost && candidate.cost > context.maxCost {
			continue
		}
		if !bestFound || candidate.betterThan(best, context) {
			best = candidate
			bestFound = true
		}
	}

	return best, bestFound
}

func (c plannedChoice) betterThan(other plannedChoice, context planningContext) bool {
	for _, objective := range []string{context.primary, context.secondary, "min_cost", "min_latency", "max_weight"} {
		switch objective {
		case "", "min_cost":
			if c.cost != other.cost {
				return c.cost < other.cost
			}
		case "min_latency":
			if c.latencyMS != other.latencyMS {
				return c.latencyMS < other.latencyMS
			}
		case "max_weight":
			if c.weight != other.weight {
				return c.weight > other.weight
			}
		}
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

func parsePlannedVariants(op ir.Operation, producerByStream map[string]string, operationsByID map[string]ir.Operation, profiles map[string]ir.OperationProfile) ([]plannedVariant, error) {
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
		latencyMS, hasLatency := intValue(entry["latency_ms"])
		if producerID, ok := producerByStream[streamID]; ok {
			if profile, exists := profiles[producerID]; exists {
				if !hasCost && profile.Cost != nil {
					cost = *profile.Cost
					hasCost = true
					entry["cost"] = cost
				}
				if !hasWeight && profile.Weight != nil {
					weight = *profile.Weight
					hasWeight = true
					entry["weight"] = weight
				}
				if !hasLatency && profile.LatencyMS != nil {
					latencyMS = *profile.LatencyMS
					hasLatency = true
					entry["latency_ms"] = latencyMS
				}
			}
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
		if !hasLatency || latencyMS < 0 {
			latencyMS = 0
			entry["latency_ms"] = latencyMS
		}

		variants = append(variants, plannedVariant{
			label:     stringConfig(entry, "label", streamID),
			stream:    streamID,
			kind:      stringConfig(entry, "kind", "bool"),
			cost:      cost,
			weight:    weight,
			latencyMS: latencyMS,
			index:     index,
			raw:       entry,
		})
	}

	return variants, nil
}

func pruneToRequiredSubgraph(doc *ir.Document, requiredOutputStreams []string) *ir.Document {
	if doc == nil {
		return nil
	}

	operationsByID := buildOperationsByID(doc)
	producerByStream := buildProducerByStream(doc)
	streamsByID := buildStreamsByID(doc)

	requiredStreams := make(map[string]struct{})
	requiredOutputSet := make(map[string]struct{}, len(requiredOutputStreams))
	queue := make([]string, 0)
	for _, streamID := range requiredOutputStreams {
		if streamID == "" {
			continue
		}
		requiredOutputSet[streamID] = struct{}{}
		if _, exists := requiredStreams[streamID]; exists {
			continue
		}
		requiredStreams[streamID] = struct{}{}
		queue = append(queue, streamID)
	}
	if len(requiredStreams) == 0 {
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
			if len(requiredOutputSet) > 0 {
				for _, input := range op.Inputs {
					if _, exists := requiredOutputSet[input.Stream]; exists {
						pruned.Operations = append(pruned.Operations, op)
						break
					}
				}
			} else {
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
		Operations:   make([]ir.Operation, 0, len(doc.Operations)),
		Streams:      append([]ir.Stream(nil), doc.Streams...),
		Task:         cloneTask(doc.Task),
		TaskVariants: cloneTaskVariants(doc.TaskVariants),
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

	cloned.Model = ir.ModelSpec{
		Operations: append([]ir.Operation(nil), cloned.Operations...),
		Streams:    append([]ir.Stream(nil), cloned.Streams...),
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

func cloneTask(task ir.TaskSpec) ir.TaskSpec {
	return ir.TaskSpec{
		ID:                task.ID,
		Inputs:            append([]ir.TaskStreamRef(nil), task.Inputs...),
		Outputs:           append([]ir.TaskStreamRef(nil), task.Outputs...),
		OperationProfiles: append([]ir.OperationProfile(nil), task.OperationProfiles...),
		Constraints:       task.Constraints,
		Objective:         task.Objective,
		Selectors:         append([]ir.SelectorTask(nil), task.Selectors...),
	}
}

func cloneTaskVariants(tasks []ir.TaskSpec) []ir.TaskSpec {
	result := make([]ir.TaskSpec, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, cloneTask(task))
	}
	return result
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

func intValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
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

func nonNegativeIntConfig(config map[string]any, key string, fallback int) int {
	if config == nil {
		return fallback
	}
	value, ok := intValue(config[key])
	if !ok || value < 0 {
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

func buildOperationProfiles(profiles []ir.OperationProfile) map[string]ir.OperationProfile {
	result := make(map[string]ir.OperationProfile, len(profiles))
	for _, profile := range profiles {
		if profile.Operation == "" {
			continue
		}
		result[profile.Operation] = profile
	}
	return result
}

func buildSelectorTasks(selectors []ir.SelectorTask) map[string]ir.SelectorTask {
	result := make(map[string]ir.SelectorTask, len(selectors))
	for _, selector := range selectors {
		if selector.ID == "" {
			continue
		}
		result[selector.ID] = selector
	}
	return result
}

func buildPlanningContext(task ir.TaskSpec) planningContext {
	context := planningContext{
		availableInputs: make(map[string]struct{}, len(task.Inputs)),
		minWeight:       defaultWeightThreshold,
		primary:         task.Objective.Primary,
		secondary:       task.Objective.Secondary,
	}
	for _, input := range task.Inputs {
		if input.Stream == "" {
			continue
		}
		context.availableInputs[input.Stream] = struct{}{}
	}
	if task.Constraints.MinTotalWeight != nil {
		context.minWeight = *task.Constraints.MinTotalWeight
	}
	if task.Constraints.MaxTotalLatencyMS != nil {
		context.maxLatencyMS = *task.Constraints.MaxTotalLatencyMS
	}
	if task.Constraints.MaxTotalCost != nil {
		context.maxCost = *task.Constraints.MaxTotalCost
		context.hasMaxCost = true
	}
	if context.primary == "" {
		context.primary = "min_cost"
	}
	if context.secondary == "" {
		context.secondary = "min_latency"
	}
	return context
}

func variantAvailable(variant plannedVariant, availableInputs map[string]struct{}) bool {
	if len(availableInputs) == 0 {
		return true
	}
	for _, input := range variant.sourceInputs {
		if _, exists := availableInputs[input]; !exists {
			return false
		}
	}
	return true
}

func aggregateChoiceMetrics(operationIDs map[string]struct{}, producerOverrides map[string]plannedVariant, operationsByID map[string]ir.Operation, profiles map[string]ir.OperationProfile) (float64, float64, int) {
	var totalCost float64
	var totalWeight float64
	totalLatency := 0

	for opID := range operationIDs {
		if override, exists := producerOverrides[opID]; exists {
			totalCost += override.cost
			totalWeight += override.weight
			totalLatency += override.latencyMS
			continue
		}
		op, exists := operationsByID[opID]
		if !exists {
			continue
		}
		cost, weight, latency := operationMetrics(op, profiles)
		totalCost += cost
		totalWeight += weight
		totalLatency += latency
	}

	return totalCost, totalWeight, totalLatency
}

func operationMetrics(op ir.Operation, profiles map[string]ir.OperationProfile) (float64, float64, int) {
	if profile, exists := profiles[op.ID]; exists {
		var cost float64
		var weight float64
		latency := 0
		if profile.Cost != nil {
			cost = *profile.Cost
		}
		if profile.Weight != nil {
			weight = *profile.Weight
		}
		if profile.LatencyMS != nil {
			latency = *profile.LatencyMS
		}
		return cost, weight, latency
	}

	var cost float64
	var weight float64
	if op.Domain.Cost != nil {
		cost = *op.Domain.Cost
	}
	if op.Domain.Weight != nil {
		weight = *op.Domain.Weight
	}
	return cost, weight, 0
}

func collectVariantClosure(doc *ir.Document, streamID string) ([]string, []string) {
	producerByStream := buildProducerByStream(doc)
	operationsByID := buildOperationsByID(doc)
	requiredOps := make(map[string]struct{})
	requiredInputs := make(map[string]struct{})

	var visitStream func(string)
	visitStream = func(currentStream string) {
		producerID, exists := producerByStream[currentStream]
		if !exists {
			return
		}
		op, exists := operationsByID[producerID]
		if !exists {
			return
		}
		requiredOps[producerID] = struct{}{}
		if op.Kind == ir.OperationKindSource {
			requiredInputs[currentStream] = struct{}{}
			return
		}
		for _, input := range op.Inputs {
			visitStream(input.Stream)
		}
	}

	visitStream(streamID)
	return sortedKeys(requiredOps), sortedKeys(requiredInputs)
}

func requiredOperationIDs(doc *ir.Document, requiredOutputStreams []string) map[string]struct{} {
	if doc == nil {
		return nil
	}
	producerByStream := buildProducerByStream(doc)
	operationsByID := buildOperationsByID(doc)
	requiredOps := make(map[string]struct{})
	queue := make([]string, 0, len(requiredOutputStreams))
	queue = append(queue, requiredOutputStreams...)

	if len(queue) == 0 {
		for _, op := range doc.Operations {
			if op.Kind != ir.OperationKindSink {
				continue
			}
			for _, input := range op.Inputs {
				queue = append(queue, input.Stream)
			}
		}
	}

	seenStreams := make(map[string]struct{}, len(queue))
	for len(queue) > 0 {
		streamID := queue[0]
		queue = queue[1:]
		if _, seen := seenStreams[streamID]; seen {
			continue
		}
		seenStreams[streamID] = struct{}{}

		producerID, exists := producerByStream[streamID]
		if !exists {
			continue
		}
		if _, seen := requiredOps[producerID]; seen {
			continue
		}
		requiredOps[producerID] = struct{}{}
		op := operationsByID[producerID]
		for _, input := range op.Inputs {
			queue = append(queue, input.Stream)
		}
	}

	return requiredOps
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func applySelectorTask(op ir.Operation, selector ir.SelectorTask) ir.Operation {
	config := cloneMap(op.Config)
	if config == nil {
		config = make(map[string]any)
	}

	if selector.SelectionMode != "" {
		config["selection_mode"] = selector.SelectionMode
	}
	if selector.DefaultChoice != "" {
		config["default_choice"] = selector.DefaultChoice
	}
	if selector.SelectionWeightThreshold != nil {
		config["selection_weight_threshold"] = *selector.SelectionWeightThreshold
	}
	if selector.MaxTotalLatencyMS != nil {
		config["max_total_latency_ms"] = *selector.MaxTotalLatencyMS
	}
	if len(selector.Variants) > 0 {
		inputs := make([]ir.StreamRef, 0, len(selector.Variants))
		rawVariants := make([]any, 0, len(selector.Variants))
		for _, variant := range selector.Variants {
			inputs = append(inputs, ir.StreamRef{Stream: variant.Stream})
			entry := map[string]any{
				"stream": variant.Stream,
			}
			if variant.Kind != "" {
				entry["kind"] = variant.Kind
			}
			if variant.Label != "" {
				entry["label"] = variant.Label
			}
			if variant.WindowMS != nil {
				entry["window_ms"] = *variant.WindowMS
			}
			if variant.Cost != nil {
				entry["cost"] = *variant.Cost
			}
			if variant.Weight != nil {
				entry["weight"] = *variant.Weight
			}
			if variant.LatencyMS != nil {
				entry["latency_ms"] = *variant.LatencyMS
			}
			rawVariants = append(rawVariants, entry)
		}
		op.Inputs = inputs
		config["variants"] = rawVariants
	}

	op.Config = config
	return op
}

func taskOutputStreams(outputs []ir.TaskStreamRef) []string {
	result := make([]string, 0, len(outputs))
	for _, output := range outputs {
		if output.Stream == "" {
			continue
		}
		result = append(result, output.Stream)
	}
	return result
}
