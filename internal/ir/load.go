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
// читаются и сливаются в один Document. Типичный пример:
//   - model.yaml — вычислительная модель (model.operations / streams);
//   - task_<id>.yaml — одна постановка задачи в корневом блоке task: …
//     (добавляется в task_variants по id);
//   - либо произвольные файлы с task_variants: [] (несколько вариантов
//     в одном файле) — тоже поддерживается.
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
// Стратегия — склейка списков (operations, streams, task_variants).
// Файл только с task: … (без model и без task_variants) добавляет одну
// запись в task_variants и не трогает dst.Task — удобно для task_*.yaml.
// Во всех остальных случаях непустой task в src заменяет dst.Task
// (например, единый документ с model + task по умолчанию).
func mergeDocuments(dst, src *Document) {
	if src == nil {
		return
	}

	dst.Model.Operations = append(dst.Model.Operations, src.Model.Operations...)
	dst.Model.Streams = append(dst.Model.Streams, src.Model.Streams...)
	dst.Operations = append(dst.Operations, src.Operations...)
	dst.Streams = append(dst.Streams, src.Streams...)
	dst.TaskVariants = append(dst.TaskVariants, src.TaskVariants...)

	if isTaskOnlyYAMLFragment(src) {
		if src.Task.ID != "" {
			dst.TaskVariants = append(dst.TaskVariants, cloneTaskSpec(src.Task))
		}
		return
	}

	if src.Task.ID != "" || len(src.Task.Inputs) > 0 || len(src.Task.Outputs) > 0 {
		dst.Task = src.Task
	}
}

// isTaskOnlyYAMLFragment — в src нет model/streams/operations и нет
// списка task_variants; зато задана одна постановка в task (корневой ключ).
func isTaskOnlyYAMLFragment(src *Document) bool {
	if src == nil {
		return false
	}
	if len(src.Model.Operations) > 0 || len(src.Model.Streams) > 0 {
		return false
	}
	if len(src.Operations) > 0 || len(src.Streams) > 0 {
		return false
	}
	if len(src.TaskVariants) > 0 {
		return false
	}
	return src.Task.ID != "" || len(src.Task.Inputs) > 0 || len(src.Task.Outputs) > 0
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

// ResolveTask выбирает из doc одну постановку и применяет её к doc.Operations:
// помимо наполнения doc.Task, он переносит task.operation_configs в соответствующие
// op.Config. Это позволяет держать model.yaml абстрактным (без cost/threshold/port),
// а конкретные значения задавать per-task.
//
// Слияние — shallow: каждый ключ из task.operation_configs.config перезаписывает
// одноимённый ключ в op.Config; отсутствующие ключи модели сохраняются. doc.Model
// не меняется и остаётся исходным "чертежом" на случай повторной загрузки.
func ResolveTask(doc *Document, taskID string) error {
	if doc == nil {
		return fmt.Errorf("ir document is nil")
	}

	switch {
	case taskID == "":
		if doc.Task.ID == "" && len(doc.TaskVariants) == 1 {
			doc.Task = cloneTaskSpec(doc.TaskVariants[0])
		} else {
			doc.Task = cloneTaskSpec(doc.Task)
		}
	case doc.Task.ID == taskID:
		doc.Task = cloneTaskSpec(doc.Task)
	default:
		var matched bool
		for _, variant := range doc.TaskVariants {
			if variant.ID != taskID {
				continue
			}
			doc.Task = cloneTaskSpec(variant)
			matched = true
			break
		}
		if !matched {
			return fmt.Errorf("task %q not found", taskID)
		}
	}

	applyOperationConfigs(doc)
	return nil
}

// applyOperationConfigs переносит task.operation_configs в doc.Operations.
// Операции, отсутствующие в модели, игнорируются здесь и будут пойманы валидатором.
func applyOperationConfigs(doc *Document) {
	if doc == nil || len(doc.Task.OperationConfigs) == 0 {
		return
	}

	overrides := make(map[string]map[string]any, len(doc.Task.OperationConfigs))
	for _, entry := range doc.Task.OperationConfigs {
		if entry.Operation == "" {
			continue
		}
		overrides[entry.Operation] = entry.Config
	}

	for index := range doc.Operations {
		op := &doc.Operations[index]
		patch, ok := overrides[op.ID]
		if !ok || len(patch) == 0 {
			continue
		}
		if op.Config == nil {
			op.Config = make(map[string]any, len(patch))
		}
		for key, value := range patch {
			op.Config[key] = value
		}
	}
}

func cloneTaskSpec(task TaskSpec) TaskSpec {
	return TaskSpec{
		ID:                task.ID,
		Inputs:            append([]TaskStreamRef(nil), task.Inputs...),
		Outputs:           append([]TaskStreamRef(nil), task.Outputs...),
		OperationProfiles: append([]OperationProfile(nil), task.OperationProfiles...),
		OperationConfigs:  cloneOperationConfigs(task.OperationConfigs),
		Constraints:       task.Constraints,
		Objective:         task.Objective,
		Selectors:         append([]SelectorTask(nil), task.Selectors...),
	}
}

func cloneOperationConfigs(configs []OperationConfig) []OperationConfig {
	if len(configs) == 0 {
		return nil
	}
	result := make([]OperationConfig, 0, len(configs))
	for _, entry := range configs {
		clone := OperationConfig{Operation: entry.Operation}
		if len(entry.Config) > 0 {
			clone.Config = make(map[string]any, len(entry.Config))
			for key, value := range entry.Config {
				clone.Config[key] = value
			}
		}
		result = append(result, clone)
	}
	return result
}
