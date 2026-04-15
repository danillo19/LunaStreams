package ops

import (
	"context"
	"fmt"
	"time"

	"LunaStreams/internal/ir"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/internal/runtime/drivers"
)

type Frame struct {
	Seq int
}

type AudioChunk struct {
	Seq     int
	Samples []int16
	RMS     float64
}

func RegisterAll(registry *rt.Registry) error {
	registrations := []struct {
		impl    string
		factory rt.Factory
	}{
		{impl: "webcam.read", factory: newWebcamSource},
		{impl: "microphone.read", factory: newMicrophoneSource},
		{impl: "cv.face_presence", factory: newFacePresence},
		{impl: "cv.motion_presence", factory: newMotionPresence},
		{impl: "audio.voice_presence", factory: newVoicePresence},
		{impl: "microphone.capture", factory: newRealtimeMicrophoneSource},
		{impl: "audio.volume_presence", factory: newVolumePresence},
		{impl: "keyboard.read", factory: newKeyboardSource},
		{impl: "selector.audio_or_recent_key", factory: newAudioOrRecentKeySelector},
		{impl: "selector.redundant_choice", factory: newRedundantChoiceSelector},
		{impl: "selector.priority_failover", factory: newPrioritySelector},
		{impl: "output.console", factory: newConsoleSink},
		{impl: "frontend.web", factory: newFrontendSink},
	}

	for _, registration := range registrations {
		if err := registry.Register(registration.impl, registration.factory); err != nil {
			return err
		}
	}

	if err := registry.RegisterDriver("subprocess", drivers.NewSubprocessDriver(rt.DefaultLogger())); err != nil {
		return err
	}

	return nil
}

func (f Frame) String() string {
	return fmt.Sprintf("frame(seq=%d)", f.Seq)
}

func (c AudioChunk) String() string {
	if len(c.Samples) == 0 {
		return fmt.Sprintf("audio(seq=%d)", c.Seq)
	}
	return fmt.Sprintf("audio(seq=%d samples=%d rms=%.4f)", c.Seq, len(c.Samples), c.RMS)
}

type webcamSource struct {
	seq    int
	output string
}

func newWebcamSource(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &webcamSource{output: output}, nil
}

func (s *webcamSource) Run(ctx context.Context, _ map[string]any) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(66 * time.Millisecond):
	}

	s.seq++
	return map[string]any{
		s.output: Frame{Seq: s.seq},
	}, nil
}

type microphoneSource struct {
	seq    int
	output string
}

func newMicrophoneSource(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &microphoneSource{output: output}, nil
}

func (s *microphoneSource) Run(ctx context.Context, _ map[string]any) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}

	s.seq++
	return map[string]any{
		s.output: AudioChunk{Seq: s.seq},
	}, nil
}

type facePresence struct {
	input  string
	output string
}

func newFacePresence(spec ir.Operation) (rt.Operator, error) {
	input, err := singleInput(spec)
	if err != nil {
		return nil, err
	}
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &facePresence{input: input, output: output}, nil
}

func (o *facePresence) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	frame, ok := inputs[o.input].(Frame)
	if !ok {
		return nil, fmt.Errorf("expected Frame on %q", o.input)
	}

	return map[string]any{
		o.output: frame.Seq%10 == 0,
	}, nil
}

type motionPresence struct {
	input  string
	output string
}

func newMotionPresence(spec ir.Operation) (rt.Operator, error) {
	input, err := singleInput(spec)
	if err != nil {
		return nil, err
	}
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &motionPresence{input: input, output: output}, nil
}

func (o *motionPresence) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	frame, ok := inputs[o.input].(Frame)
	if !ok {
		return nil, fmt.Errorf("expected Frame on %q", o.input)
	}

	return map[string]any{
		o.output: frame.Seq%5 == 0,
	}, nil
}

type voicePresence struct {
	input  string
	output string
}

func newVoicePresence(spec ir.Operation) (rt.Operator, error) {
	input, err := singleInput(spec)
	if err != nil {
		return nil, err
	}
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &voicePresence{input: input, output: output}, nil
}

func (o *voicePresence) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	chunk, ok := inputs[o.input].(AudioChunk)
	if !ok {
		return nil, fmt.Errorf("expected AudioChunk on %q", o.input)
	}

	return map[string]any{
		o.output: chunk.Seq%7 == 0,
	}, nil
}

type prioritySelector struct {
	output   string
	priority []string
}

func newPrioritySelector(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	priority := parsePriority(spec.Config)
	if len(priority) == 0 {
		for _, input := range spec.Inputs {
			priority = append(priority, input.Stream)
		}
	}

	return &prioritySelector{
		output:   output,
		priority: priority,
	}, nil
}

func (o *prioritySelector) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	decision := false
	for _, streamID := range o.priority {
		value, _ := inputs[streamID].(bool)
		if value {
			decision = true
			break
		}
	}

	return map[string]any{
		o.output: decision,
	}, nil
}

type consoleSink struct {
	input string
}

func newConsoleSink(spec ir.Operation) (rt.Operator, error) {
	input, err := singleInput(spec)
	if err != nil {
		return nil, err
	}

	return &consoleSink{input: input}, nil
}

func (s *consoleSink) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	value := inputs[s.input]
	rt.DefaultLogger().Result(s.input, value)
	return nil, nil
}

func singleInput(spec ir.Operation) (string, error) {
	if len(spec.Inputs) != 1 {
		return "", fmt.Errorf("operation %q expects exactly one input", spec.ID)
	}
	return spec.Inputs[0].Stream, nil
}

func singleOutput(spec ir.Operation) (string, error) {
	if len(spec.Outputs) != 1 {
		return "", fmt.Errorf("operation %q expects exactly one output", spec.ID)
	}
	return spec.Outputs[0].Stream, nil
}

func parsePriority(config map[string]any) []string {
	if config == nil {
		return nil
	}

	raw, ok := config["priority"]
	if !ok {
		return nil
	}

	items, ok := raw.([]any)
	if !ok {
		return nil
	}

	priority := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if ok && value != "" {
			priority = append(priority, value)
		}
	}

	return priority
}
