package ir

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// Load читает IR из файла или директории.
//
// Если path указывает на директорию, все *.yaml / *.yml файлы внутри
// читаются и сливаются в один Document. Это позволяет разделять описание
// примера на несколько файлов (например, presence.yaml — вычислительная
// модель, task.yaml — постановки задачи) без дублирования содержимого.
//
// Если path указывает на файл, поведение совместимо с прошлым MVP: читается
// и парсится один документ.
func Load(path string) (*Document, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat ir path %q: %w", path, err)
	}

	if info.IsDir() {
		return loadDirectory(path)
	}
	return loadFile(path)
}

func loadFile(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ir file: %w", err)
	}

	var doc Document
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("decode ir yaml %q: %w", path, err)
	}

	normalizeDocument(&doc)
	return &doc, nil
}

func loadDirectory(dir string) (*Document, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read ir directory %q: %w", dir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := filepath.Ext(name)
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no yaml files found in %q", dir)
	}
	sort.Strings(files)

	merged := &Document{}
	for _, file := range files {
		part, err := loadFile(file)
		if err != nil {
			return nil, err
		}
		mergeDocuments(merged, part)
	}
	normalizeDocument(merged)

	return merged, nil
}

// mergeDocuments объединяет src в dst.
// Стратегия — безусловная склейка списков (operations, streams,
// task_variants) и замена task, если она явно задана в src.
// Такая склейка удобна для разбиения примера на presence.yaml + task.yaml:
// ни один файл не перезаписывает полностью содержимое другого.
func mergeDocuments(dst, src *Document) {
	if src == nil {
		return
	}

	dst.Model.Operations = append(dst.Model.Operations, src.Model.Operations...)
	dst.Model.Streams = append(dst.Model.Streams, src.Model.Streams...)
	dst.Operations = append(dst.Operations, src.Operations...)
	dst.Streams = append(dst.Streams, src.Streams...)
	dst.TaskVariants = append(dst.TaskVariants, src.TaskVariants...)

	if src.Task.ID != "" || len(src.Task.Inputs) > 0 || len(src.Task.Outputs) > 0 {
		dst.Task = src.Task
	}
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
