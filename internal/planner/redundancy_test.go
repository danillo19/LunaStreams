package planner

import (
	"testing"

	"LunaStreams/internal/ir"
)

func TestCompileRedundantChoicesPrunesExpensiveBranch(t *testing.T) {
	doc := &ir.Document{
		Operations: []ir.Operation{
			{
				ID:   "microphone_source",
				Kind: ir.OperationKindSource,
				Mode: ir.OperationModeAlwaysOn,
				Impl: "microphone.capture",
				Outputs: []ir.StreamRef{
					{Stream: "audio_chunk"},
				},
			},
			{
				ID:   "loud_audio_presence",
				Kind: ir.OperationKindTransform,
				Mode: ir.OperationModeTaskPerEvent,
				Impl: "audio.volume_presence",
				Inputs: []ir.StreamRef{
					{Stream: "audio_chunk"},
				},
				Outputs: []ir.StreamRef{
					{Stream: "loud_audio_presence"},
				},
				Domain: ir.OperationDomain{
					Cost:   floatPtr(7),
					Weight: floatPtr(0.95),
				},
			},
			{
				ID:   "soft_audio_presence",
				Kind: ir.OperationKindTransform,
				Mode: ir.OperationModeTaskPerEvent,
				Impl: "audio.volume_presence",
				Inputs: []ir.StreamRef{
					{Stream: "audio_chunk"},
				},
				Outputs: []ir.StreamRef{
					{Stream: "soft_audio_presence"},
				},
				Domain: ir.OperationDomain{
					Cost:   floatPtr(1),
					Weight: floatPtr(0.45),
				},
			},
			{
				ID:   "keyboard_source",
				Kind: ir.OperationKindSource,
				Mode: ir.OperationModeAlwaysOn,
				Impl: "keyboard.read",
				Outputs: []ir.StreamRef{
					{Stream: "pressed_key"},
				},
				Domain: ir.OperationDomain{
					Cost:   floatPtr(2),
					Weight: floatPtr(0.40),
				},
			},
			{
				ID:   "presence_choice_selector",
				Kind: ir.OperationKindSelector,
				Mode: ir.OperationModeAlwaysOn,
				Impl: "selector.redundant_choice",
				Inputs: []ir.StreamRef{
					{Stream: "loud_audio_presence"},
					{Stream: "soft_audio_presence"},
					{Stream: "pressed_key"},
				},
				Outputs: []ir.StreamRef{
					{Stream: "presence_decision"},
					{Stream: "presence_strategy"},
				},
				Config: map[string]any{
					"selection_mode":             SelectionModeCompileTimeMin,
					"default_choice":             "plan_inactive",
					"selection_weight_threshold": 0.80,
					"variants": []any{
						map[string]any{"stream": "loud_audio_presence", "kind": "bool", "label": "loud_audio"},
						map[string]any{"stream": "soft_audio_presence", "kind": "bool", "label": "soft_audio"},
						map[string]any{"stream": "pressed_key", "kind": "recent_key", "label": "recent_keyboard"},
					},
				},
			},
			{
				ID:   "presence_sink",
				Kind: ir.OperationKindSink,
				Mode: ir.OperationModeTaskPerEvent,
				Impl: "output.console",
				Inputs: []ir.StreamRef{
					{Stream: "presence_decision"},
				},
			},
			{
				ID:   "strategy_sink",
				Kind: ir.OperationKindSink,
				Mode: ir.OperationModeTaskPerEvent,
				Impl: "output.console",
				Inputs: []ir.StreamRef{
					{Stream: "presence_strategy"},
				},
			},
		},
		Streams: []ir.Stream{
			{ID: "audio_chunk", Type: ir.StreamTypeAudioChunk},
			{ID: "loud_audio_presence", Type: ir.StreamTypeBool},
			{ID: "soft_audio_presence", Type: ir.StreamTypeBool},
			{ID: "pressed_key", Type: ir.StreamTypeText},
			{ID: "presence_decision", Type: ir.StreamTypeBool},
			{ID: "presence_strategy", Type: ir.StreamTypeText},
		},
	}

	result, err := CompileRedundantChoices(doc)
	if err != nil {
		t.Fatalf("CompileRedundantChoices() error = %v", err)
	}

	if len(result.Decisions) != 1 {
		t.Fatalf("decisions = %d, want 1", len(result.Decisions))
	}
	if got := result.Decisions[0].Strategy; got != "soft_audio+recent_keyboard | cost=3.00 weight=0.85" {
		t.Fatalf("strategy = %q", got)
	}
	if hasOperation(result.Document, "loud_audio_presence") {
		t.Fatalf("planned document still contains loud_audio_presence")
	}
	if !hasOperation(result.Document, "soft_audio_presence") || !hasOperation(result.Document, "keyboard_source") {
		t.Fatalf("planned document lost selected operations")
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

func floatPtr(value float64) *float64 {
	return &value
}
