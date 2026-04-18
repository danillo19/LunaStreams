package runtime

import (
	"context"
	"testing"
	"time"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
)

func TestAlwaysOnTransformRunsWithoutSelectorKind(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("test.copy", func(spec ir.Operation) (Operator, error) {
		return copyBoolOperator{
			input:  spec.Inputs[0].Stream,
			output: spec.Outputs[0].Stream,
		}, nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	doc := &ir.Document{
		Operations: []ir.Operation{
			{
				ID:   "presence_aggregator",
				Kind: ir.OperationKindTransform,
				Mode: ir.OperationModeAlwaysOn,
				Impl: "test.copy",
				Inputs: []ir.StreamRef{
					{Stream: "presence_input"},
				},
				Outputs: []ir.StreamRef{
					{Stream: "presence_output"},
				},
				Config: map[string]any{
					"tick_ms": 5,
				},
			},
		},
		Streams: []ir.Stream{
			{ID: "presence_input", Type: ir.StreamTypeBool},
			{ID: "presence_output", Type: ir.StreamTypeBool},
		},
	}

	dependencyGraph, err := graph.Build(doc)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	engine, err := NewEngine(doc, dependencyGraph, registry, nil)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	engine.store.Set("presence_input", true)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := engine.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		value, _, ok := engine.store.Get("presence_output")
		if ok {
			got, castOK := value.(bool)
			if !castOK {
				t.Fatalf("presence_output type = %T, want bool", value)
			}
			if !got {
				t.Fatalf("presence_output = %v, want true", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("presence_output was not produced by always_on transform")
}

type copyBoolOperator struct {
	input  string
	output string
}

func (o copyBoolOperator) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	value, _ := inputs[o.input].(bool)
	return map[string]any{
		o.output: value,
	}, nil
}
