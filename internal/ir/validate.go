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

		if op.Kind == OperationKindSource && len(op.Inputs) > 0 {
			problems = append(problems, fmt.Sprintf("source operation %q must not have inputs", op.ID))
		}
		if op.Kind == OperationKindSink && len(op.Outputs) > 0 {
			problems = append(problems, fmt.Sprintf("sink operation %q must not have outputs", op.ID))
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

	if len(problems) > 0 {
		return fmt.Errorf("ir validation failed:\n- %s", strings.Join(problems, "\n- "))
	}

	return nil
}
