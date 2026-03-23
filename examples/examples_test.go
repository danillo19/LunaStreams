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

	if _, err := rt.NewEngine(doc, dependencyGraph, registry, rt.NewBeautifulLogger(false)); err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	if hasOperation(doc, "loud_audio_presence") {
		t.Fatalf("planned document still contains loud_audio_presence")
	}
	if !hasOperation(doc, "soft_audio_presence") || !hasOperation(doc, "keyboard_source") {
		t.Fatalf("planned document lost selected low-cost operations")
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
