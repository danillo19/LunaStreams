package examples_test

import (
	"testing"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
	"LunaStreams/internal/planner"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/internal/runtime/ops"
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
			t.Fatalf("expected task_variant %q to be merged from task.yaml; got %v", expected, variantIDs)
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
	if len(face.Runtime.Command) < 2 || face.Runtime.Command[0] != "python3" {
		t.Fatalf("face_presence runtime.command = %v, want python3 + script", face.Runtime.Command)
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

	sink := findOperation(doc, "frontend_sink")
	streams := make(map[string]struct{}, len(sink.Inputs))
	for _, input := range sink.Inputs {
		streams[input.Stream] = struct{}{}
	}
	for _, expected := range []string{"camera_frame", "audio_chunk", "presence_decision"} {
		if _, ok := streams[expected]; !ok {
			t.Fatalf("frontend_sink lost expected input %q; got %v", expected, streams)
		}
	}
}

func TestPresenceFrontendSinkDropsMissingInputsOnReducedPlan(t *testing.T) {
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

	if hasOperation(doc, "camera_source") {
		t.Fatalf("audio_only_fast should not require camera_source")
	}

	sink := findOperation(doc, "frontend_sink")
	for _, input := range sink.Inputs {
		if input.Stream == "camera_frame" {
			t.Fatalf("frontend_sink still references pruned stream %q", input.Stream)
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
