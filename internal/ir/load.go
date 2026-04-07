package ir

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ir file: %w", err)
	}

	var doc Document
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("decode ir yaml: %w", err)
	}

	normalizeDocument(&doc)

	return &doc, nil
}

func normalizeDocument(doc *Document) {
	if doc == nil {
		return
	}

	if len(doc.Model.Operations) > 0 || len(doc.Model.Streams) > 0 {
		doc.Operations = append([]Operation(nil), doc.Model.Operations...)
		doc.Streams = append([]Stream(nil), doc.Model.Streams...)
		return
	}

	doc.Model = ModelSpec{
		Operations: append([]Operation(nil), doc.Operations...),
		Streams:    append([]Stream(nil), doc.Streams...),
	}
}

func ResolveTask(doc *Document, taskID string) error {
	if doc == nil {
		return fmt.Errorf("ir document is nil")
	}

	if taskID == "" {
		if doc.Task.ID == "" && len(doc.TaskVariants) == 1 {
			doc.Task = cloneTaskSpec(doc.TaskVariants[0])
		}
		return nil
	}

	if doc.Task.ID == taskID {
		doc.Task = cloneTaskSpec(doc.Task)
		return nil
	}

	for _, variant := range doc.TaskVariants {
		if variant.ID != taskID {
			continue
		}
		doc.Task = cloneTaskSpec(variant)
		return nil
	}

	return fmt.Errorf("task %q not found", taskID)
}

func cloneTaskSpec(task TaskSpec) TaskSpec {
	return TaskSpec{
		ID:                task.ID,
		Inputs:            append([]TaskStreamRef(nil), task.Inputs...),
		Outputs:           append([]TaskStreamRef(nil), task.Outputs...),
		OperationProfiles: append([]OperationProfile(nil), task.OperationProfiles...),
		Constraints:       task.Constraints,
		Objective:         task.Objective,
		Selectors:         append([]SelectorTask(nil), task.Selectors...),
	}
}
