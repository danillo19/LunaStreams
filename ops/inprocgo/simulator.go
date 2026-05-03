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

//go:embed web/simulator.html
var simulatorIndexHTML []byte

// runtimeSimulatorSink serves a generic runtime graph view. It does not
// depend on presence streams or any other domain payloads: the page is fed only
// by runtime telemetry events emitted by the engine.
type runtimeSimulatorSink struct {
	logger *rt.BeautifulLogger

	server   *http.Server
	listener net.Listener

	closed   chan struct{}
	closeOne sync.Once
}

func newRuntimeSimulatorSink(spec ir.Operation) (rt.Operator, error) {
	logger := rt.DefaultLogger()
	port := intConfig(spec.Config, "port", 8090)
	bind := stringConfig(spec.Config, "bind", "127.0.0.1")

	sink := &runtimeSimulatorSink{
		logger: logger,
		closed: make(chan struct{}),
	}

	addr := fmt.Sprintf("%s:%d", bind, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("runtime simulator %q: listen %s: %w", spec.ID, addr, err)
	}
	sink.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("/", sink.handleIndex)
	mux.HandleFunc("/events", sink.handleEvents)

	sink.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := sink.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(spec.ID, "runtime simulator http serve failed: %v", err)
		}
	}()

	logger.Info(spec.ID, "runtime simulator ready on http://%s", listener.Addr())
	return sink, nil
}

func (s *runtimeSimulatorSink) Run(_ context.Context, _ map[string]any) (map[string]any, error) {
	return nil, nil
}

func (s *runtimeSimulatorSink) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(simulatorIndexHTML)
}

func (s *runtimeSimulatorSink) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	events, unsubscribe := rt.SubscribeRuntimeEvents(256)
	defer unsubscribe()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case event := <-events:
			payload, err := json.Marshal(event)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		}
	}
}

func (s *runtimeSimulatorSink) Close() error {
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
