package inprocgo

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"LunaStreams/internal/ir"
	rt "LunaStreams/internal/runtime"
)

// frontendIndexHTML зашивается в бинарь, чтобы sink мог отдавать UI без
// зависимости от файловой системы пользователя.
//
//go:embed web/index.html
var frontendIndexHTML []byte

// frontendSink — sink-оператор `frontend.web`, который поднимает небольшой
// HTTP-сервер и отдаёт состояние pipeline в браузер:
//   - GET  /        — статическая HTML-страница с камерой, audio-метром и
//     индикатором presence;
//   - GET  /events  — поток Server-Sent Events с JSON snapshot-ами вида
//     {camera, audio, presence, strategy}.
//
// Сервер поднимается один раз при создании оператора и живёт до Close().
// Run() вызывается движком на каждое событие любого из входных streams —
// на этот момент sink публикует актуальный snapshot всем подписчикам SSE.
//
// Такая реализация специально сделана без внешних зависимостей, чтобы
// показать, что frontend — это обычная polyglot-операция: её легко
// заменить на Node.js/Python subprocess (kind=subprocess) или на вынесенный
// сервис через kind=http без изменений ядра runtime.
type frontendSink struct {
	logger *rt.BeautifulLogger

	cameraStream   string
	audioStream    string
	presenceStream string
	strategyStream string

	server   *http.Server
	listener net.Listener

	mu       sync.RWMutex
	latest   snapshot
	clients  map[chan []byte]struct{}
	closed   chan struct{}
	closeOne sync.Once
}

// snapshot — то, что мы пушим в браузер. Всё опционально, чтобы один и
// тот же UI работал и на урезанных task-вариантах.
type snapshot struct {
	Camera   map[string]any `json:"camera,omitempty"`
	Audio    *audioSnapshot `json:"audio,omitempty"`
	Presence *bool          `json:"presence,omitempty"`
	Strategy *string        `json:"strategy,omitempty"`
}

type audioSnapshot struct {
	RMS float64 `json:"rms"`
	Seq int     `json:"seq"`
}

func newFrontendSink(spec ir.Operation) (rt.Operator, error) {
	logger := rt.DefaultLogger()

	port := intConfig(spec.Config, "port", 8080)
	bind := stringConfig(spec.Config, "bind", "127.0.0.1")

	sink := &frontendSink{
		logger:         logger,
		cameraStream:   stringConfig(spec.Config, "camera_stream", "camera_frame"),
		audioStream:    stringConfig(spec.Config, "audio_stream", "audio_chunk"),
		presenceStream: stringConfig(spec.Config, "presence_stream", "presence_decision"),
		strategyStream: stringConfig(spec.Config, "strategy_stream", "presence_strategy"),
		clients:        make(map[chan []byte]struct{}),
		closed:         make(chan struct{}),
	}

	addr := fmt.Sprintf("%s:%d", bind, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("frontend sink %q: listen %s: %w", spec.ID, addr, err)
	}
	sink.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("/", sink.handleIndex)
	mux.HandleFunc("/events", sink.handleEvents)
	mux.HandleFunc("/snapshot", sink.handleSnapshot)

	sink.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // SSE — долгие ответы
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := sink.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(spec.ID, "frontend http serve failed: %v", err)
		}
	}()

	logger.Info(spec.ID, "frontend ready on http://%s (camera=%s audio=%s presence=%s)",
		listener.Addr(), sink.cameraStream, sink.audioStream, sink.presenceStream)

	return sink, nil
}

// Run публикует актуальный snapshot всем SSE-подписчикам. inputs уже
// содержит последние значения из store, поэтому sink всегда шлёт
// согласованный срез, а не только ту stream, которая триггернула событие.
func (s *frontendSink) Run(_ context.Context, inputs map[string]any) (map[string]any, error) {
	snap := s.buildSnapshot(inputs)
	s.mu.Lock()
	s.latest = snap
	clients := make([]chan []byte, 0, len(s.clients))
	for ch := range s.clients {
		clients = append(clients, ch)
	}
	s.mu.Unlock()

	payload, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	for _, ch := range clients {
		select {
		case ch <- payload:
		default:
			// медленный клиент — дропаем фрейм, а не блокируем sink.
		}
	}

	return nil, nil
}

func (s *frontendSink) buildSnapshot(inputs map[string]any) snapshot {
	out := snapshot{}

	if value, ok := inputs[s.cameraStream]; ok {
		if frame := decodeCameraFrame(value); frame != nil {
			out.Camera = frame
		}
	}

	if value, ok := inputs[s.audioStream]; ok {
		if audio := audioChunkToSnapshot(value); audio != nil {
			out.Audio = audio
		}
	}

	if value, ok := inputs[s.presenceStream]; ok {
		if b, ok := value.(bool); ok {
			out.Presence = &b
		}
	}

	if value, ok := inputs[s.strategyStream]; ok {
		if text, ok := value.(string); ok {
			out.Strategy = &text
		}
	}

	return out
}

func decodeCameraFrame(value any) map[string]any {
	switch v := value.(type) {
	case map[string]any:
		return v
	case map[any]any:
		converted := make(map[string]any, len(v))
		for key, item := range v {
			if k, ok := key.(string); ok {
				converted[k] = item
			}
		}
		return converted
	default:
		return nil
	}
}

func audioChunkToSnapshot(value any) *audioSnapshot {
	switch v := value.(type) {
	case AudioChunk:
		return &audioSnapshot{RMS: v.RMS, Seq: v.Seq}
	case map[string]any:
		rms, _ := asFloat(v["rms"])
		seq, _ := asInt(v["seq"])
		return &audioSnapshot{RMS: rms, Seq: seq}
	default:
		return nil
	}
}

func (s *frontendSink) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(frontendIndexHTML)
}

func (s *frontendSink) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	snap := s.latest
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *frontendSink) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan []byte, 16)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	latest := s.latest
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
	}()

	if initial, err := json.Marshal(latest); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", initial)
		flusher.Flush()
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case payload := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		}
	}
}

// Close корректно гасит HTTP-сервер и отсоединяет всех SSE-клиентов.
func (s *frontendSink) Close() error {
	var err error
	s.closeOne.Do(func() {
		close(s.closed)
		if s.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			err = s.server.Shutdown(ctx)
		}
	})
	return err
}

func asFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}

func asInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		return int(n), err == nil
	}
	return 0, false
}
