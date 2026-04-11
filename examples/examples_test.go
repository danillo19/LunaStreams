package examples_test

import (
	"testing"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
	"LunaStreams/internal/planner"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/internal/runtime/ops"
)

func TestPresenceRedundantV2ValidatesAndBuilds(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load("presence_redundant_v2.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
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

func TestPresenceRedundantV2CameraVisionTaskRoutesThroughPythonFaceOp(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load("presence_redundant_v2.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
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

func TestPresenceRedundantV2HighConfidenceTaskChoosesDifferentPlan(t *testing.T) {
	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		t.Fatalf("RegisterAll() error = %v", err)
	}

	doc, err := ir.Load("presence_redundant_v2.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
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
