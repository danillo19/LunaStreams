package examples_test

import (
	"testing"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
	"LunaStreams/internal/planner"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/ops"
)

const presenceExampleDir = "presence"

func TestPresenceExampleLoadsFromDirectoryWithModelAndTask(t *testing.T) {
	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}

	if len(doc.Model.Operations) == 0 {
		t.Fatalf("model.operations is empty; model file was not merged")
	}
	if len(doc.TaskVariants) == 0 {
		t.Fatalf("task_variants is empty; task file was not merged")
	}

	variantIDs := make(map[string]struct{}, len(doc.TaskVariants))
	for _, variant := range doc.TaskVariants {
		variantIDs[variant.ID] = struct{}{}
	}
	for _, expected := range []string{"low_cost_interactive", "camera_vision_accurate", "audio_only_fast"} {
		if _, ok := variantIDs[expected]; !ok {
			t.Fatalf("expected task_variant %q to be merged from task_*.yaml; got %v", expected, variantIDs)
		}
	}
}

func TestPresenceLowCostInteractiveValidatesAndBuilds(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}
	if err := ir.ResolveTask(doc, "low_cost_interactive"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}
	doc = planResult.Document

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate(planned) error = %v", err)
	}

	dependencyGraph, err := graph.Build(doc)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if hasOperation(doc, "loud_audio_presence") {
		t.Fatalf("planned document still contains loud_audio_presence")
	}
	if !hasOperation(doc, "soft_audio_presence") || !hasOperation(doc, "keyboard_source") {
		t.Fatalf("planned document lost selected low-cost operations")
	}
	if hasOperation(doc, "keyboard_sink") {
		t.Fatalf("planned document still contains keyboard_sink, which is not part of task.outputs")
	}

	_ = dependencyGraph
}

func TestPresenceCameraVisionTaskRoutesThroughPythonFaceOp(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}
	if err := ir.ResolveTask(doc, "camera_vision_accurate"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}
	doc = planResult.Document

	if !hasOperation(doc, "face_presence") {
		t.Fatalf("planned document missing python-backed face_presence op")
	}
	if !hasOperation(doc, "camera_source") {
		t.Fatalf("planned document missing camera_source (required by face_presence)")
	}
	if hasOperation(doc, "loud_audio_presence") || hasOperation(doc, "soft_audio_presence") || hasOperation(doc, "keyboard_source") {
		t.Fatalf("planned document contains audio/keyboard ops that are not required for camera_vision_accurate")
	}

	face := findOperation(doc, "face_presence")
	if face.Runtime.Kind != "subprocess" {
		t.Fatalf("face_presence runtime.kind = %q, want subprocess", face.Runtime.Kind)
	}
	if len(face.Runtime.Command) < 2 || face.Runtime.Command[0] != "sh" {
		t.Fatalf("face_presence runtime.command = %v, want sh + launcher script", face.Runtime.Command)
	}
}

func TestPresenceFrontendTaskKeepsCameraAudioAndWebSink(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}
	if err := ir.ResolveTask(doc, "frontend_full"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}
	doc = planResult.Document

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate(planned) error = %v", err)
	}

	for _, id := range []string{"camera_source", "microphone_source", "face_presence", "frontend_sink"} {
		if !hasOperation(doc, id) {
			t.Fatalf("planned document for frontend_full missing %q", id)
		}
	}
	// frontend_full не объявляет console-sinks в task.outputs[].sink,
	// значит planner должен их отбросить — иначе opt-in теряет смысл.
	for _, id := range []string{"presence_sink", "strategy_sink"} {
		if hasOperation(doc, id) {
			t.Fatalf("frontend_full must not keep console sink %q (not listed in task.outputs[].sink)", id)
		}
	}

	sink := findOperation(doc, "frontend_sink")
	streams := make(map[string]struct{}, len(sink.Inputs))
	for _, input := range sink.Inputs {
		streams[input.Stream] = struct{}{}
	}
	for _, expected := range []string{"camera_frame", "audio_chunk", "presence_decision", "presence_strategy"} {
		if _, ok := streams[expected]; !ok {
			t.Fatalf("frontend_sink lost expected input %q; got %v", expected, streams)
		}
	}
}

// Задачи без frontend_sink в task.outputs[].sink не должны запускать
// HTTP-сервер. Проверяем, что в плане остаются только явно указанные
// sink-и и что camera_source не подтягивается через inputs чужого sink-а.
func TestPresenceAudioOnlyFastExcludesFrontendSink(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}
	if err := ir.ResolveTask(doc, "audio_only_fast"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}
	doc = planResult.Document

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate(planned) error = %v", err)
	}

	if hasOperation(doc, "frontend_sink") {
		t.Fatalf("audio_only_fast must not keep frontend_sink (not listed in task.outputs[].sink)")
	}
	if hasOperation(doc, "camera_source") {
		t.Fatalf("audio_only_fast should not require camera_source")
	}
	for _, id := range []string{"presence_sink", "strategy_sink"} {
		if !hasOperation(doc, id) {
			t.Fatalf("audio_only_fast must keep sink %q listed in task.outputs[].sink", id)
		}
	}
}

func TestPresenceHighConfidenceTaskChoosesDifferentPlan(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}
	if err := ir.ResolveTask(doc, "high_confidence_monitoring"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	if err := ir.Validate(doc, registry); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}
	doc = planResult.Document

	if !hasOperation(doc, "loud_audio_presence") {
		t.Fatalf("planned document lost loud_audio_presence")
	}
	if hasOperation(doc, "soft_audio_presence") || hasOperation(doc, "keyboard_source") {
		t.Fatalf("planned document retained low-confidence alternatives")
	}
}

func TestPresenceTaskAppliesOperationConfigsToModel(t *testing.T) {
	doc, err := ir.Load(presenceExampleDir)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", presenceExampleDir, err)
	}

	for _, op := range doc.Model.Operations {
		if len(op.Config) != 0 {
			t.Fatalf("model operation %q still carries config %v; configs must live in tasks only", op.ID, op.Config)
		}
	}

	if err := ir.ResolveTask(doc, "audio_only_fast"); err != nil {
		t.Fatalf("ResolveTask() error = %v", err)
	}

	soft := findOperation(doc, "soft_audio_presence")
	if soft.ID == "" {
		t.Fatalf("soft_audio_presence not found in resolved doc")
	}
	threshold, ok := soft.Config["threshold"].(float64)
	if !ok {
		t.Fatalf("soft_audio_presence threshold type = %T, want float64", soft.Config["threshold"])
	}
	if threshold != 0.04 {
		t.Fatalf("soft_audio_presence threshold = %v, want 0.04 (from task)", threshold)
	}

	selector := findOperation(doc, "presence_choice_selector")
	if selector.ID == "" {
		t.Fatalf("presence_choice_selector not found in resolved doc")
	}
	variants, ok := selector.Config["variants"].([]any)
	if !ok {
		t.Fatalf("selector variants type = %T, want []any", selector.Config["variants"])
	}
	if len(variants) != 2 {
		t.Fatalf("audio_only_fast selector variants len = %d, want 2 (loud+soft from task)", len(variants))
	}

	for _, op := range doc.Model.Operations {
		if len(op.Config) != 0 {
			t.Fatalf("model operation %q was mutated after ResolveTask; model must remain abstract", op.ID)
		}
	}
}

func hasOperation(doc *ir.Document, id string) bool {
	for _, op := range doc.Operations {
		if op.ID == id {
			return true
		}
	}
	return false
}

func findOperation(doc *ir.Document, id string) ir.Operation {
	for _, op := range doc.Operations {
		if op.ID == id {
			return op
		}
	}
	return ir.Operation{}
}
