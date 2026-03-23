package ops

import (
	"context"
	"testing"
	"time"

	"LunaStreams/internal/ir"
)

func TestRedundantChoiceSelectorRuntimeBundleChoosesCheapestSetMeetingWeightThreshold(t *testing.T) {
	operator, err := newRedundantChoiceSelector(ir.Operation{
		ID: "presence_choice_selector",
		Outputs: []ir.StreamRef{
			{Stream: "presence_decision"},
			{Stream: "presence_strategy"},
		},
		Config: map[string]any{
			"selection_mode":             "runtime_bundle",
			"default_choice":             "threshold_unmet",
			"selection_weight_threshold": 0.8,
			"variants": []any{
				map[string]any{
					"stream": "loud_audio_presence",
					"label":  "loud_audio",
					"cost":   7.0,
					"weight": 0.95,
				},
				map[string]any{
					"stream": "soft_audio_presence",
					"label":  "soft_audio",
					"cost":   1.0,
					"weight": 0.45,
				},
				map[string]any{
					"stream": "pressed_key",
					"label":  "recent_keyboard",
					"kind":   "recent_key",
					"cost":   2.0,
					"weight": 0.40,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("newRedundantChoiceSelector() error = %v", err)
	}

	outputs, err := operator.Run(context.Background(), map[string]any{
		"loud_audio_presence": true,
		"soft_audio_presence": true,
		"pressed_key":         KeyEvent{Key: "a", At: time.Now()},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := outputs["presence_decision"]; got != true {
		t.Fatalf("presence_decision = %v, want true", got)
	}
	if got := outputs["presence_strategy"]; got != "soft_audio+recent_keyboard | cost=3.00 weight=0.85" {
		t.Fatalf("presence_strategy = %v, want weighted cheap bundle", got)
	}
}

func TestRedundantChoiceSelectorRuntimeBundleReturnsDefaultWhenThresholdNotMet(t *testing.T) {
	operator, err := newRedundantChoiceSelector(ir.Operation{
		ID: "presence_choice_selector",
		Outputs: []ir.StreamRef{
			{Stream: "presence_decision"},
			{Stream: "presence_strategy"},
		},
		Config: map[string]any{
			"selection_mode":             "runtime_bundle",
			"default_choice":             "threshold_unmet",
			"selection_weight_threshold": 0.8,
			"variants": []any{
				map[string]any{
					"stream": "soft_audio_presence",
					"label":  "soft_audio",
					"cost":   1.0,
					"weight": 0.45,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("newRedundantChoiceSelector() error = %v", err)
	}

	outputs, err := operator.Run(context.Background(), map[string]any{
		"soft_audio_presence": true,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := outputs["presence_decision"]; got != false {
		t.Fatalf("presence_decision = %v, want false", got)
	}
	if got := outputs["presence_strategy"]; got != "threshold_unmet" {
		t.Fatalf("presence_strategy = %v, want threshold_unmet", got)
	}
}

func TestRedundantChoiceSelectorSelectedAnyUsesPlannedStrategy(t *testing.T) {
	operator, err := newRedundantChoiceSelector(ir.Operation{
		ID: "presence_choice_selector",
		Outputs: []ir.StreamRef{
			{Stream: "presence_decision"},
			{Stream: "presence_strategy"},
		},
		Config: map[string]any{
			"selection_mode":   "selected_any",
			"default_choice":   "plan_inactive",
			"planned_strategy": "soft_audio+recent_keyboard | cost=3.00 weight=0.85",
			"variants": []any{
				map[string]any{
					"stream": "soft_audio_presence",
					"kind":   "bool",
					"label":  "soft_audio",
				},
				map[string]any{
					"stream":    "pressed_key",
					"kind":      "recent_key",
					"label":     "recent_keyboard",
					"window_ms": 250,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("newRedundantChoiceSelector() error = %v", err)
	}

	freshOutputs, err := operator.Run(context.Background(), map[string]any{
		"soft_audio_presence": true,
	})
	if err != nil {
		t.Fatalf("Run() with selected signal error = %v", err)
	}
	if got := freshOutputs["presence_decision"]; got != true {
		t.Fatalf("selected presence_decision = %v, want true", got)
	}
	if got := freshOutputs["presence_strategy"]; got != "soft_audio+recent_keyboard | cost=3.00 weight=0.85" {
		t.Fatalf("selected presence_strategy = %v, want planned strategy", got)
	}

	staleOutputs, err := operator.Run(context.Background(), map[string]any{
		"pressed_key": KeyEvent{Key: "a", At: time.Now().Add(-500 * time.Millisecond)},
	})
	if err != nil {
		t.Fatalf("Run() with inactive selected plan error = %v", err)
	}
	if got := staleOutputs["presence_decision"]; got != false {
		t.Fatalf("inactive presence_decision = %v, want false", got)
	}
	if got := staleOutputs["presence_strategy"]; got != "plan_inactive" {
		t.Fatalf("inactive presence_strategy = %v, want plan_inactive", got)
	}
}
