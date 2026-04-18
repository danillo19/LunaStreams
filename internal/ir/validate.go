package ir

import (
	"fmt"
	"strings"
)

// ImplRegistry проверяет, может ли runtime создать реализацию для операции.
// Учитывается не только Impl-идентификатор, но и RuntimeSpec (inproc_go,
// subprocess, docker, grpc, ...). Это позволяет декларативно смешивать
// операции, реализованные на разных стэках: Go, Python, внешние сервисы и т.д.
type ImplRegistry interface {
	Supports(op Operation) bool
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
		} else if registry != nil && !registry.Supports(op) {
			problems = append(problems, fmt.Sprintf("operation %q uses unsupported impl %q (runtime kind=%q)", op.ID, op.Impl, op.Runtime.Kind))
		}
		problems = append(problems, validateRuntimeSpec(op)...)
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
		if input.Sink != "" {
			problems = append(problems, fmt.Sprintf("%s.inputs[] must not contain sink references (use stream only)", prefix))
			continue
		}
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

	seenOutputStreams := make(map[string]struct{}, len(task.Outputs))
	seenOutputSinks := make(map[string]struct{}, len(task.Outputs))
	for _, output := range task.Outputs {
		if output.Stream != "" && output.Sink != "" {
			problems = append(problems, fmt.Sprintf("%s.outputs[] must set exactly one of {stream, sink}, got both (%q, %q)", prefix, output.Stream, output.Sink))
			continue
		}
		switch {
		case output.Stream != "":
			if _, dup := seenOutputStreams[output.Stream]; dup {
				problems = append(problems, fmt.Sprintf("duplicate %s.outputs stream %q", prefix, output.Stream))
				continue
			}
			seenOutputStreams[output.Stream] = struct{}{}
			if _, exists := streamsByID[output.Stream]; !exists {
				problems = append(problems, fmt.Sprintf("%s output references unknown stream %q", prefix, output.Stream))
			}
		case output.Sink != "":
			if _, dup := seenOutputSinks[output.Sink]; dup {
				problems = append(problems, fmt.Sprintf("duplicate %s.outputs sink %q", prefix, output.Sink))
				continue
			}
			seenOutputSinks[output.Sink] = struct{}{}
			op, exists := operationsByID[output.Sink]
			if !exists {
				problems = append(problems, fmt.Sprintf("%s output references unknown sink operation %q", prefix, output.Sink))
				continue
			}
			if op.Kind != OperationKindSink {
				problems = append(problems, fmt.Sprintf("%s output sink %q must reference a sink operation (got kind=%q)", prefix, output.Sink, op.Kind))
			}
		default:
			problems = append(problems, fmt.Sprintf("%s.outputs[] must set either stream or sink", prefix))
		}
	}

	seenConfigs := make(map[string]struct{}, len(task.OperationConfigs))
	for _, entry := range task.OperationConfigs {
		if entry.Operation == "" {
			problems = append(problems, fmt.Sprintf("%s.operation_configs[].operation must not be empty", prefix))
			continue
		}
		if _, exists := seenConfigs[entry.Operation]; exists {
			problems = append(problems, fmt.Sprintf("duplicate %s operation_config for %q", prefix, entry.Operation))
			continue
		}
		seenConfigs[entry.Operation] = struct{}{}
		if _, exists := operationsByID[entry.Operation]; !exists {
			problems = append(problems, fmt.Sprintf("%s operation_config references unknown operation %q", prefix, entry.Operation))
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

// validateRuntimeSpec проверяет минимальные инварианты runtime-секции.
// Каждый kind нуждается в своём минимальном наборе полей: subprocess/docker
// — команда, grpc/http — endpoint. Сами kind-ы регистрируются в runtime
// registry отдельно, здесь мы только ловим явные опечатки в IR.
func validateRuntimeSpec(op Operation) []string {
	var problems []string
	runtime := op.Runtime
	switch runtime.Kind {
	case "", "inproc_go":
		if len(runtime.Command) > 0 || runtime.Image != "" || runtime.Endpoint != "" {
			problems = append(problems, fmt.Sprintf("operation %q uses inproc_go runtime but specifies command/image/endpoint", op.ID))
		}
	case "subprocess":
		if len(runtime.Command) == 0 {
			problems = append(problems, fmt.Sprintf("operation %q (runtime=subprocess) requires runtime.command", op.ID))
		}
	case "docker":
		if runtime.Image == "" {
			problems = append(problems, fmt.Sprintf("operation %q (runtime=docker) requires runtime.image", op.ID))
		}
	case "grpc", "http":
		if runtime.Endpoint == "" {
			problems = append(problems, fmt.Sprintf("operation %q (runtime=%s) requires runtime.endpoint", op.ID, runtime.Kind))
		}
	}
	return problems
}

func isSupportedObjective(value string) bool {
	switch value {
	case "", "min_cost", "min_latency", "max_weight":
		return true
	default:
		return false
	}
}
