package ir

import (
	"fmt"
	"strings"
)

type ImplRegistry interface {
	Has(impl string) bool
}

func Validate(doc *Document, registry ImplRegistry) error {
	if doc == nil {
		return fmt.Errorf("ir document is nil")
	}

	var problems []string

	streamsByID := make(map[string]Stream, len(doc.Streams))
	for _, stream := range doc.Streams {
		if stream.ID == "" {
			problems = append(problems, "stream.id must not be empty")
			continue
		}
		if _, exists := streamsByID[stream.ID]; exists {
			problems = append(problems, fmt.Sprintf("duplicate stream.id %q", stream.ID))
			continue
		}
		streamsByID[stream.ID] = stream
	}

	operationsByID := make(map[string]Operation, len(doc.Operations))
	producers := make(map[string]int, len(doc.Streams))

	for _, op := range doc.Operations {
		if op.ID == "" {
			problems = append(problems, "operation.id must not be empty")
			continue
		}
		if _, exists := operationsByID[op.ID]; exists {
			problems = append(problems, fmt.Sprintf("duplicate operation.id %q", op.ID))
			continue
		}
		operationsByID[op.ID] = op

		if op.Impl == "" {
			problems = append(problems, fmt.Sprintf("operation %q has empty impl", op.ID))
		} else if registry != nil && !registry.Has(op.Impl) {
			problems = append(problems, fmt.Sprintf("operation %q uses unregistered impl %q", op.ID, op.Impl))
		}
		if op.Domain.Cost != nil && *op.Domain.Cost < 0 {
			problems = append(problems, fmt.Sprintf("operation %q has negative domain.cost", op.ID))
		}
		if op.Domain.Weight != nil && *op.Domain.Weight < 0 {
			problems = append(problems, fmt.Sprintf("operation %q has negative domain.weight", op.ID))
		}

		if op.Kind == OperationKindSource && len(op.Inputs) > 0 {
			problems = append(problems, fmt.Sprintf("source operation %q must not have inputs", op.ID))
		}
		if op.Kind == OperationKindSink && len(op.Outputs) > 0 {
			problems = append(problems, fmt.Sprintf("sink operation %q must not have outputs", op.ID))
		}
		if op.Impl == "selector.redundant_choice" {
			if len(op.Inputs) == 0 && len(selectorVariantStreams(op.Config)) == 0 {
				problems = append(problems, fmt.Sprintf("selector %q requires model inputs or config.variants", op.ID))
			}
			for _, streamID := range selectorVariantStreams(op.Config) {
				if _, exists := streamsByID[streamID]; !exists {
					problems = append(problems, fmt.Sprintf("selector %q variant references unknown stream %q", op.ID, streamID))
				}
			}
		}

		for _, input := range op.Inputs {
			if input.Stream == "" {
				problems = append(problems, fmt.Sprintf("operation %q has empty input stream reference", op.ID))
				continue
			}
			if _, exists := streamsByID[input.Stream]; !exists {
				problems = append(problems, fmt.Sprintf("operation %q references unknown input stream %q", op.ID, input.Stream))
			}
		}

		for _, output := range op.Outputs {
			if output.Stream == "" {
				problems = append(problems, fmt.Sprintf("operation %q has empty output stream reference", op.ID))
				continue
			}
			if _, exists := streamsByID[output.Stream]; !exists {
				problems = append(problems, fmt.Sprintf("operation %q references unknown output stream %q", op.ID, output.Stream))
				continue
			}
			producers[output.Stream]++
		}
	}

	for streamID := range streamsByID {
		switch producers[streamID] {
		case 0:
			problems = append(problems, fmt.Sprintf("stream %q has no producer", streamID))
		case 1:
		default:
			problems = append(problems, fmt.Sprintf("stream %q has %d producers, expected exactly 1", streamID, producers[streamID]))
		}
	}

	problems = append(problems, validateTaskSpec(doc, doc.Task, operationsByID, streamsByID, producers, "task")...)
	seenTaskVariantIDs := make(map[string]struct{}, len(doc.TaskVariants))
	for index, taskVariant := range doc.TaskVariants {
		prefix := fmt.Sprintf("task_variants[%d]", index)
		if taskVariant.ID != "" {
			if _, exists := seenTaskVariantIDs[taskVariant.ID]; exists {
				problems = append(problems, fmt.Sprintf("duplicate task_variants id %q", taskVariant.ID))
			}
			seenTaskVariantIDs[taskVariant.ID] = struct{}{}
		}
		problems = append(problems, validateTaskSpec(doc, taskVariant, operationsByID, streamsByID, producers, prefix)...)
	}

	if len(problems) > 0 {
		return fmt.Errorf("ir validation failed:\n- %s", strings.Join(problems, "\n- "))
	}

	return nil
}

func validateTaskSpec(doc *Document, task TaskSpec, operationsByID map[string]Operation, streamsByID map[string]Stream, producers map[string]int, prefix string) []string {
	var problems []string

	if task.ID == "" && prefix != "task" {
		problems = append(problems, fmt.Sprintf("%s.id must not be empty", prefix))
	}

	for _, input := range task.Inputs {
		if input.Stream == "" {
			problems = append(problems, fmt.Sprintf("%s.inputs[].stream must not be empty", prefix))
			continue
		}
		stream, exists := streamsByID[input.Stream]
		if !exists {
			problems = append(problems, fmt.Sprintf("%s input references unknown stream %q", prefix, input.Stream))
			continue
		}
		if producers[stream.ID] != 1 {
			continue
		}
		producerID := producerForStream(doc, input.Stream)
		if producerID == "" {
			continue
		}
		if producer := operationsByID[producerID]; producer.Kind != OperationKindSource {
			problems = append(problems, fmt.Sprintf("%s input stream %q must be produced by source operation, got %q", prefix, input.Stream, producerID))
		}
	}

	for _, output := range task.Outputs {
		if output.Stream == "" {
			problems = append(problems, fmt.Sprintf("%s.outputs[].stream must not be empty", prefix))
			continue
		}
		if _, exists := streamsByID[output.Stream]; !exists {
			problems = append(problems, fmt.Sprintf("%s output references unknown stream %q", prefix, output.Stream))
		}
	}

	seenProfiles := make(map[string]struct{}, len(task.OperationProfiles))
	for _, profile := range task.OperationProfiles {
		if profile.Operation == "" {
			problems = append(problems, fmt.Sprintf("%s.operation_profiles[].operation must not be empty", prefix))
			continue
		}
		if _, exists := seenProfiles[profile.Operation]; exists {
			problems = append(problems, fmt.Sprintf("duplicate %s operation_profile for %q", prefix, profile.Operation))
			continue
		}
		seenProfiles[profile.Operation] = struct{}{}
		if _, exists := operationsByID[profile.Operation]; !exists {
			problems = append(problems, fmt.Sprintf("%s operation_profile references unknown operation %q", prefix, profile.Operation))
		}
		if profile.Cost != nil && *profile.Cost < 0 {
			problems = append(problems, fmt.Sprintf("%s operation_profile %q has negative cost", prefix, profile.Operation))
		}
		if profile.Weight != nil && *profile.Weight < 0 {
			problems = append(problems, fmt.Sprintf("%s operation_profile %q has negative weight", prefix, profile.Operation))
		}
		if profile.LatencyMS != nil && *profile.LatencyMS < 0 {
			problems = append(problems, fmt.Sprintf("%s operation_profile %q has negative latency_ms", prefix, profile.Operation))
		}
	}

	if task.Constraints.MinTotalWeight != nil && *task.Constraints.MinTotalWeight < 0 {
		problems = append(problems, fmt.Sprintf("%s.constraints.min_total_weight has negative value", prefix))
	}
	if task.Constraints.MaxTotalLatencyMS != nil && *task.Constraints.MaxTotalLatencyMS < 0 {
		problems = append(problems, fmt.Sprintf("%s.constraints.max_total_latency_ms has negative value", prefix))
	}
	if task.Constraints.MaxTotalCost != nil && *task.Constraints.MaxTotalCost < 0 {
		problems = append(problems, fmt.Sprintf("%s.constraints.max_total_cost has negative value", prefix))
	}
	if !isSupportedObjective(task.Objective.Primary) {
		problems = append(problems, fmt.Sprintf("%s.objective.primary uses unsupported value %q", prefix, task.Objective.Primary))
	}
	if task.Objective.Secondary != "" && !isSupportedObjective(task.Objective.Secondary) {
		problems = append(problems, fmt.Sprintf("%s.objective.secondary uses unsupported value %q", prefix, task.Objective.Secondary))
	}

	for _, selector := range task.Selectors {
		op, exists := operationsByID[selector.ID]
		if !exists {
			problems = append(problems, fmt.Sprintf("%s selector %q references unknown operation", prefix, selector.ID))
			continue
		}
		if op.Impl != "selector.redundant_choice" {
			problems = append(problems, fmt.Sprintf("%s selector %q must reference selector.redundant_choice", prefix, selector.ID))
		}
		if selector.SelectionWeightThreshold != nil && *selector.SelectionWeightThreshold < 0 {
			problems = append(problems, fmt.Sprintf("%s selector %q has negative selection_weight_threshold", prefix, selector.ID))
		}
		if selector.MaxTotalLatencyMS != nil && *selector.MaxTotalLatencyMS < 0 {
			problems = append(problems, fmt.Sprintf("%s selector %q has negative max_total_latency_ms", prefix, selector.ID))
		}
		for index, variant := range selector.Variants {
			if variant.Stream == "" {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] requires stream", prefix, selector.ID, index))
				continue
			}
			if _, exists := streamsByID[variant.Stream]; !exists {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] references unknown stream %q", prefix, selector.ID, index, variant.Stream))
			}
			if variant.Cost != nil && *variant.Cost < 0 {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] has negative cost", prefix, selector.ID, index))
			}
			if variant.Weight != nil && *variant.Weight < 0 {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] has negative weight", prefix, selector.ID, index))
			}
			if variant.LatencyMS != nil && *variant.LatencyMS < 0 {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] has negative latency_ms", prefix, selector.ID, index))
			}
			if variant.WindowMS != nil && *variant.WindowMS < 0 {
				problems = append(problems, fmt.Sprintf("%s selector %q variant[%d] has negative window_ms", prefix, selector.ID, index))
			}
		}
	}

	return problems
}

func producerForStream(doc *Document, streamID string) string {
	if doc == nil {
		return ""
	}
	for _, op := range doc.Operations {
		for _, output := range op.Outputs {
			if output.Stream == streamID {
				return op.ID
			}
		}
	}
	return ""
}

func selectorVariantStreams(config map[string]any) []string {
	raw, ok := config["variants"]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		streamID, _ := entry["stream"].(string)
		if streamID == "" {
			continue
		}
		result = append(result, streamID)
	}
	return result
}

func isSupportedObjective(value string) bool {
	switch value {
	case "", "min_cost", "min_latency", "max_weight":
		return true
	default:
		return false
	}
}
