package examples_test

import (
	"testing"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
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

	dependencyGraph, err := graph.Build(doc)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if _, err := rt.NewEngine(doc, dependencyGraph, registry, rt.NewBeautifulLogger(false)); err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
}
