package ops

// #include <stdlib.h>
import "C"

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	goRuntime "runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"LunaStreams/internal/ir"
	rt "LunaStreams/internal/runtime"
	"github.com/gen2brain/malgo"
)

type KeyEvent struct {
	Seq int
	Key string
	At  time.Time
}

func (e KeyEvent) String() string {
	return fmt.Sprintf("key(seq=%d value=%q)", e.Seq, e.Key)
}

type realtimeMicrophoneSource struct {
	opID   string
	output string
	chunks chan AudioChunk

	ctx         *malgo.AllocatedContext
	device      *malgo.Device
	deviceIDPtr unsafe.Pointer

	closeOnce sync.Once
}

func newRealtimeMicrophoneSource(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	source := &realtimeMicrophoneSource{
		opID:   spec.ID,
		output: output,
		chunks: make(chan AudioChunk, 32),
	}

	if err := source.init(spec.Config); err != nil {
		_ = source.Close()
		return nil, err
	}

	return source, nil
}

func (s *realtimeMicrophoneSource) init(config map[string]any) error {
	allocatedContext, err := malgo.InitContext(preferredBackends(), malgo.ContextConfig{}, func(message string) {
		message = strings.TrimSpace(message)
		if message != "" {
			rt.DefaultLogger().Debug(s.opID, "malgo: %s", message)
		}
	})
	if err != nil {
		return fmt.Errorf("init malgo context: %w", err)
	}
	s.ctx = allocatedContext

	sampleRate := uint32(intConfig(config, "sample_rate", 16000))
	channels := uint32(intConfig(config, "channels", 1))
	periodFrames := uint32(intConfig(config, "frames_per_chunk", 1024))
	deviceNameFilter := strings.ToLower(strings.TrimSpace(stringConfig(config, "device_name_contains", "")))

	if err := s.configureCaptureDevice(deviceNameFilter); err != nil {
		return err
	}

	deviceConfig := malgo.DefaultDeviceConfig(malgo.Capture)
	deviceConfig.Capture.Format = malgo.FormatS16
	deviceConfig.Capture.Channels = channels
	if s.deviceIDPtr != nil {
		deviceConfig.Capture.DeviceID = s.deviceIDPtr
	}
	deviceConfig.SampleRate = sampleRate
	deviceConfig.PeriodSizeInFrames = periodFrames
	deviceConfig.Alsa.NoMMap = 1

	var seq int
	callbacks := malgo.DeviceCallbacks{
		Data: func(_, input []byte, _ uint32) {
			if len(input) == 0 {
				return
			}

			chunk := decodeAudioChunk(input, &seq)
			select {
			case s.chunks <- chunk:
			default:
			}
		},
		Stop: func() {
			rt.DefaultLogger().Info(s.opID, "microphone device stopped")
		},
	}

	device, err := malgo.InitDevice(s.ctx.Context, deviceConfig, callbacks)
	if err != nil {
		return fmt.Errorf("init microphone device: %w", err)
	}
	s.device = device

	if err := s.device.Start(); err != nil {
		return fmt.Errorf("start microphone device: %w", err)
	}

	rt.DefaultLogger().Info(s.opID, "microphone capture active sample_rate=%d channels=%d frames_per_chunk=%d", sampleRate, channels, periodFrames)
	return nil
}

func (s *realtimeMicrophoneSource) configureCaptureDevice(deviceNameFilter string) error {
	devices, err := s.ctx.Devices(malgo.Capture)
	if err != nil {
		return fmt.Errorf("list capture devices: %w", err)
	}

	if len(devices) == 0 {
		return fmt.Errorf("no capture devices found")
	}

	names := make([]string, 0, len(devices))
	for _, device := range devices {
		label := device.Name()
		if device.IsDefault != 0 {
			label += " [default]"
		}
		names = append(names, label)
	}
	rt.DefaultLogger().Info(s.opID, "capture devices: %s", strings.Join(names, ", "))

	if deviceNameFilter == "" {
		return nil
	}

	for _, device := range devices {
		if !strings.Contains(strings.ToLower(device.Name()), deviceNameFilter) {
			continue
		}

		s.deviceIDPtr = device.ID.Pointer()
		rt.DefaultLogger().Info(s.opID, "selected capture device %q by filter %q", device.Name(), deviceNameFilter)
		return nil
	}

	return fmt.Errorf("no capture device matched %q", deviceNameFilter)
}

func (s *realtimeMicrophoneSource) Run(ctx context.Context, _ map[string]any) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case chunk := <-s.chunks:
		return map[string]any{
			s.output: chunk,
		}, nil
	}
}

func (s *realtimeMicrophoneSource) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		if s.device != nil {
			s.device.Uninit()
			s.device = nil
		}
		if s.deviceIDPtr != nil {
			C.free(s.deviceIDPtr)
			s.deviceIDPtr = nil
		}
		if s.ctx != nil {
			if err := s.ctx.Uninit(); err != nil {
				closeErr = err
			}
			s.ctx.Free()
			s.ctx = nil
		}
	})

	return closeErr
}

type volumePresence struct {
	input     string
	output    string
	threshold float64
}

type audioOrRecentKeySelector struct {
	output      string
	audioStream string
	keyStream   string
	keyWindow   time.Duration
}

type redundantChoiceSelector struct {
	decisionOutput        string
	choiceOutput          string
	defaultChoice         string
	selectionWeightTarget float64
	variants              []choiceVariant
}

type choiceVariant struct {
	label     string
	stream    string
	kind      string
	keyWindow time.Duration
	cost      float64
	weight    float64
	index     int
}

type selectedChoice struct {
	labels []string
	cost   float64
	weight float64
	count  int
	order  []int
}

func newVolumePresence(spec ir.Operation) (rt.Operator, error) {
	input, err := singleInput(spec)
	if err != nil {
		return nil, err
	}
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	return &volumePresence{
		input:     input,
		output:    output,
		threshold: floatConfig(spec.Config, "threshold", 0.08),
	}, nil
}

func (o *volumePresence) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	chunk, ok := inputs[o.input].(AudioChunk)
	if !ok {
		return nil, fmt.Errorf("expected AudioChunk on %q", o.input)
	}

	return map[string]any{
		o.output: chunk.RMS >= o.threshold,
	}, nil
}

func newAudioOrRecentKeySelector(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	audioStream := stringConfig(spec.Config, "audio_stream", "")
	keyStream := stringConfig(spec.Config, "key_stream", "")
	if audioStream == "" || keyStream == "" {
		return nil, fmt.Errorf("selector %q requires config.audio_stream and config.key_stream", spec.ID)
	}

	windowMS := intConfig(spec.Config, "keyboard_window_ms", 1500)
	if windowMS <= 0 {
		windowMS = 1500
	}

	return &audioOrRecentKeySelector{
		output:      output,
		audioStream: audioStream,
		keyStream:   keyStream,
		keyWindow:   time.Duration(windowMS) * time.Millisecond,
	}, nil
}

func (o *audioOrRecentKeySelector) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	audioPresence, _ := inputs[o.audioStream].(bool)

	keyPresence := false
	if event, ok := inputs[o.keyStream].(KeyEvent); ok && !event.At.IsZero() {
		keyPresence = time.Since(event.At) <= o.keyWindow
	}

	return map[string]any{
		o.output: audioPresence || keyPresence,
	}, nil
}

func newRedundantChoiceSelector(spec ir.Operation) (rt.Operator, error) {
	decisionOutput, choiceOutput, err := dualOutputs(spec)
	if err != nil {
		return nil, err
	}

	variants, err := parseChoiceVariants(spec)
	if err != nil {
		return nil, err
	}
	if len(variants) == 0 {
		return nil, fmt.Errorf("selector %q requires at least one config.variants item", spec.ID)
	}

	return &redundantChoiceSelector{
		decisionOutput:        decisionOutput,
		choiceOutput:          choiceOutput,
		defaultChoice:         stringConfig(spec.Config, "default_choice", "threshold_unmet"),
		selectionWeightTarget: positiveFloatConfig(spec.Config, "selection_weight_threshold", 1.0),
		variants:              variants,
	}, nil
}

func (o *redundantChoiceSelector) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	active := make([]choiceVariant, 0, len(o.variants))
	for _, variant := range o.variants {
		if variant.matches(inputs) {
			active = append(active, variant)
		}
	}

	best, ok := chooseWeightedVariantSet(active, o.selectionWeightTarget)
	if !ok {
		return map[string]any{
			o.decisionOutput: false,
			o.choiceOutput:   o.defaultChoice,
		}, nil
	}

	return map[string]any{
		o.decisionOutput: true,
		o.choiceOutput:   best.describe(),
	}, nil
}

type keyboardSource struct {
	opID   string
	output string
	events chan KeyEvent
	done   chan struct{}

	closeOnce sync.Once
}

func newKeyboardSource(spec ir.Operation) (rt.Operator, error) {
	output, err := singleOutput(spec)
	if err != nil {
		return nil, err
	}

	source := &keyboardSource{
		opID:   spec.ID,
		output: output,
		events: make(chan KeyEvent, 32),
		done:   make(chan struct{}),
	}

	go source.readLoop()
	rt.DefaultLogger().Info(spec.ID, "keyboard input active; type keys and press Enter")

	return source, nil
}

func (s *keyboardSource) readLoop() {
	reader := bufio.NewReader(os.Stdin)
	seq := 0

	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
			}

			rt.DefaultLogger().Error(s.opID, "keyboard read failed: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if r == '\n' || r == '\r' {
			continue
		}

		seq++
		event := KeyEvent{
			Seq: seq,
			Key: string(r),
			At:  time.Now(),
		}

		select {
		case <-s.done:
			return
		case s.events <- event:
		}
	}
}

func (s *keyboardSource) Run(ctx context.Context, _ map[string]any) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case event := <-s.events:
		return map[string]any{
			s.output: event,
		}, nil
	}
}

func (s *keyboardSource) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
	})
	return nil
}

func decodeAudioChunk(input []byte, seq *int) AudioChunk {
	samples := make([]int16, 0, len(input)/2)
	var sumSquares float64

	for i := 0; i+1 < len(input); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(input[i : i+2]))
		samples = append(samples, sample)

		normalized := float64(sample) / 32768.0
		sumSquares += normalized * normalized
	}

	*seq = *seq + 1

	rms := 0.0
	if len(samples) > 0 {
		rms = math.Sqrt(sumSquares / float64(len(samples)))
	}

	return AudioChunk{
		Seq:     *seq,
		Samples: samples,
		RMS:     rms,
	}
}

func intConfig(config map[string]any, key string, fallback int) int {
	if config == nil {
		return fallback
	}

	raw, ok := config[key]
	if !ok {
		return fallback
	}

	switch value := raw.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return fallback
	}
}

func floatConfig(config map[string]any, key string, fallback float64) float64 {
	if config == nil {
		return fallback
	}

	raw, ok := config[key]
	if !ok {
		return fallback
	}

	switch value := raw.(type) {
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case float64:
		return value
	default:
		return fallback
	}
}

func positiveFloatConfig(config map[string]any, key string, fallback float64) float64 {
	value := floatConfig(config, key, fallback)
	if value > 0 {
		return value
	}
	return fallback
}

func nonNegativeFloatConfig(config map[string]any, key string, fallback float64) float64 {
	value := floatConfig(config, key, fallback)
	if value >= 0 {
		return value
	}
	return fallback
}

func stringConfig(config map[string]any, key string, fallback string) string {
	if config == nil {
		return fallback
	}

	raw, ok := config[key]
	if !ok {
		return fallback
	}

	value, ok := raw.(string)
	if !ok {
		return fallback
	}

	return value
}

func dualOutputs(spec ir.Operation) (string, string, error) {
	if len(spec.Outputs) != 2 {
		return "", "", fmt.Errorf("operation %q expects exactly two outputs", spec.ID)
	}

	return spec.Outputs[0].Stream, spec.Outputs[1].Stream, nil
}

func parseChoiceVariants(spec ir.Operation) ([]choiceVariant, error) {
	if spec.Config == nil {
		return nil, nil
	}

	raw, ok := spec.Config["variants"]
	if !ok {
		return nil, nil
	}

	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("selector %q expects config.variants to be a list", spec.ID)
	}

	variants := make([]choiceVariant, 0, len(items))
	for index, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("selector %q variant[%d] must be an object", spec.ID, index)
		}

		stream := stringConfig(entry, "stream", "")
		if stream == "" {
			return nil, fmt.Errorf("selector %q variant[%d] requires stream", spec.ID, index)
		}

		variant := choiceVariant{
			label:  stringConfig(entry, "label", stream),
			stream: stream,
			kind:   stringConfig(entry, "kind", "bool"),
			cost:   nonNegativeFloatConfig(entry, "cost", 1.0),
			weight: positiveFloatConfig(entry, "weight", 1.0),
			index:  index,
		}

		switch variant.kind {
		case "bool":
		case "recent_key":
			windowMS := intConfig(entry, "window_ms", 1500)
			if windowMS <= 0 {
				windowMS = 1500
			}
			variant.keyWindow = time.Duration(windowMS) * time.Millisecond
		default:
			return nil, fmt.Errorf("selector %q variant[%d] uses unsupported kind %q", spec.ID, index, variant.kind)
		}

		variants = append(variants, variant)
	}

	return variants, nil
}

func (v choiceVariant) matches(inputs map[string]any) bool {
	switch v.kind {
	case "bool":
		value, _ := inputs[v.stream].(bool)
		return value
	case "recent_key":
		event, ok := inputs[v.stream].(KeyEvent)
		return ok && !event.At.IsZero() && time.Since(event.At) <= v.keyWindow
	default:
		return false
	}
}

func chooseWeightedVariantSet(active []choiceVariant, threshold float64) (selectedChoice, bool) {
	if len(active) == 0 {
		return selectedChoice{}, false
	}

	if threshold <= 0 {
		threshold = 1.0
	}

	var best selectedChoice
	bestFound := false
	limit := 1 << len(active)
	for mask := 1; mask < limit; mask++ {
		candidate := selectedChoice{}
		for index, variant := range active {
			if mask&(1<<index) == 0 {
				continue
			}

			candidate.labels = append(candidate.labels, variant.label)
			candidate.cost += variant.cost
			candidate.weight += variant.weight
			candidate.count++
			candidate.order = append(candidate.order, variant.index)
		}

		if candidate.weight < threshold {
			continue
		}

		if !bestFound || candidate.betterThan(best) {
			best = candidate
			bestFound = true
		}
	}

	return best, bestFound
}

func (c selectedChoice) betterThan(other selectedChoice) bool {
	if c.cost != other.cost {
		return c.cost < other.cost
	}
	if c.weight != other.weight {
		return c.weight > other.weight
	}
	if c.count != other.count {
		return c.count < other.count
	}

	for index := 0; index < len(c.order) && index < len(other.order); index++ {
		if c.order[index] != other.order[index] {
			return c.order[index] < other.order[index]
		}
	}

	return len(c.order) < len(other.order)
}

func (c selectedChoice) describe() string {
	return fmt.Sprintf("%s | cost=%.2f weight=%.2f", strings.Join(c.labels, "+"), c.cost, c.weight)
}

func preferredBackends() []malgo.Backend {
	switch goRuntime.GOOS {
	case "darwin":
		return []malgo.Backend{malgo.BackendCoreaudio}
	case "linux":
		return []malgo.Backend{malgo.BackendPulseaudio, malgo.BackendAlsa}
	case "windows":
		return []malgo.Backend{malgo.BackendWasapi}
	default:
		return nil
	}
}
