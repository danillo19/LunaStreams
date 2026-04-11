// Package drivers содержит реализации различных runtime-драйверов для
// LunaStreams. Драйверы позволяют подключать операции, написанные не на
// Go: внешние процессы (Python/Node/Rust/Bash), Docker-контейнеры, gRPC-
// и HTTP-сервисы. Engine не знает о конкретной природе операции — он
// пользуется только контрактом runtime.Operator.
//
// Добавление новой реализации состоит из двух шагов:
//  1. реализовать runtime.OperationRuntime (метод Create);
//  2. зарегистрировать драйвер в runtime.Registry через RegisterDriver.
//
// Драйвер subprocess в этом файле — это справочная эталонная реализация
// polyglot-контракта, описанного в docs/operation-interface.md.
package drivers

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"LunaStreams/internal/ir"
	rt "LunaStreams/internal/runtime"
)

// SubprocessDriver запускает каждую операцию в отдельном долгоживущем
// процессе. Взаимодействие идёт через stdin/stdout в формате NDJSON:
// одна строка на запрос, одна строка на ответ. Формат совместим с
// polyglot-контрактом (RunRequest / RunResult).
type SubprocessDriver struct {
	logger *rt.BeautifulLogger
}

// NewSubprocessDriver создаёт драйвер для kind="subprocess".
func NewSubprocessDriver(logger *rt.BeautifulLogger) *SubprocessDriver {
	return &SubprocessDriver{logger: logger}
}

// Create запускает процесс и возвращает оператор, сериализующий вызовы.
func (d *SubprocessDriver) Create(spec ir.Operation) (rt.Operator, error) {
	if len(spec.Runtime.Command) == 0 {
		return nil, fmt.Errorf("subprocess op %q requires runtime.command", spec.ID)
	}

	return newSubprocessOperator(spec, d.logger, spec.Runtime.Command, spec.Runtime.Args, spec.Runtime.WorkDir, spec.Runtime.Env)
}

// inputValue повторяет полезную часть InputValue из docs/operation-interface.md.
type inputValue struct {
	StreamID string            `json:"stream_id"`
	Type     string            `json:"type"`
	Schema   string            `json:"schema"`
	Encoding string            `json:"encoding"`
	Seq      uint64            `json:"seq"`
	Present  bool              `json:"present"`
	Payload  []byte            `json:"payload"`
	Meta     map[string]string `json:"meta,omitempty"`
}

// outputBinding описывает выход операции в запросе (чтобы внешний процесс
// знал, какие stream_id/type/schema использовать при ответе).
type outputBinding struct {
	StreamID string `json:"stream_id"`
	Type     string `json:"type"`
	Schema   string `json:"schema"`
	Encoding string `json:"encoding"`
}

type runRequest struct {
	OperationID string                `json:"operation_id"`
	Kind        string                `json:"kind"`
	Mode        string                `json:"mode"`
	Config      map[string]any        `json:"config"`
	Inputs      map[string]inputValue `json:"inputs"`
	Outputs     []outputBinding       `json:"outputs"`
	Meta        requestMeta           `json:"meta"`
}

type requestMeta struct {
	Attempt         int   `json:"attempt"`
	TimestampUnixMs int64 `json:"timestamp_unix_ms"`
}

type outputValue struct {
	StreamID string            `json:"stream_id"`
	Type     string            `json:"type"`
	Schema   string            `json:"schema"`
	Encoding string            `json:"encoding"`
	Payload  []byte            `json:"payload"`
	Meta     map[string]string `json:"meta,omitempty"`
}

type runResult struct {
	Outputs map[string]outputValue `json:"outputs"`
	Meta    map[string]string      `json:"meta,omitempty"`
	Error   string                 `json:"error,omitempty"`
}

// subprocessOperator — один долгоживущий дочерний процесс. Запросы
// сериализуются через mutex, чтобы не смешивать ответы.
type subprocessOperator struct {
	opID    string
	kind    string
	mode    string
	config  map[string]any
	inputs  []ir.StreamRef
	outputs []ir.StreamRef
	logger  *rt.BeautifulLogger

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader

	mu        sync.Mutex
	attempt   int
	closeOnce sync.Once
	closed    chan struct{}
}

func newSubprocessOperator(spec ir.Operation, logger *rt.BeautifulLogger, command, extraArgs []string, workDir string, env map[string]string) (*subprocessOperator, error) {
	name := command[0]
	args := append([]string{}, command[1:]...)
	args = append(args, extraArgs...)

	cmd := exec.Command(name, args...)
	cmd.Dir = workDir
	if len(env) > 0 {
		envSlice := make([]string, 0, len(env))
		for key, value := range env {
			envSlice = append(envSlice, fmt.Sprintf("%s=%s", key, value))
		}
		cmd.Env = append(cmd.Environ(), envSlice...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start subprocess: %w", err)
	}

	op := &subprocessOperator{
		opID:    spec.ID,
		kind:    string(spec.Kind),
		mode:    string(spec.Mode),
		config:  spec.Config,
		inputs:  append([]ir.StreamRef(nil), spec.Inputs...),
		outputs: append([]ir.StreamRef(nil), spec.Outputs...),
		logger:  logger,
		cmd:     cmd,
		stdin:   stdin,
		stdout:  bufio.NewReaderSize(stdout, 1<<16),
		closed:  make(chan struct{}),
	}

	go op.forwardStderr(stderr)

	if logger != nil {
		logger.Info(spec.ID, "subprocess started pid=%d cmd=%q", cmd.Process.Pid, strings.Join(command, " "))
	}
	return op, nil
}

func (o *subprocessOperator) forwardStderr(stderr io.Reader) {
	reader := bufio.NewScanner(stderr)
	reader.Buffer(make([]byte, 0, 1<<16), 1<<20)
	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}
		if o.logger != nil {
			o.logger.Debug(o.opID, "stderr: %s", line)
		}
	}
}

// Run конвертирует inputs в polyglot-запрос, отправляет в stdin и
// читает одну строку ответа. Контекст прерывания транслируется в закрытие
// stdin, что заставляет дочерний процесс увидеть EOF.
func (o *subprocessOperator) Run(ctx context.Context, inputs map[string]any) (map[string]any, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	select {
	case <-o.closed:
		return nil, fmt.Errorf("operator %q closed", o.opID)
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	o.attempt++
	request, err := o.buildRequest(inputs)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	if _, err := o.stdin.Write(append(payload, '\n')); err != nil {
		return nil, fmt.Errorf("write to subprocess: %w", err)
	}

	line, err := readLineWithCtx(ctx, o.stdout)
	if err != nil {
		return nil, err
	}

	var result runResult
	if err := json.Unmarshal(line, &result); err != nil {
		return nil, fmt.Errorf("decode subprocess response: %w (line=%s)", err, strings.TrimSpace(string(line)))
	}
	if result.Error != "" {
		return nil, fmt.Errorf("subprocess error: %s", result.Error)
	}

	outputs := make(map[string]any, len(result.Outputs))
	for streamID, out := range result.Outputs {
		value, err := decodeOutput(out)
		if err != nil {
			return nil, fmt.Errorf("decode output %q: %w", streamID, err)
		}
		outputs[streamID] = value
	}
	return outputs, nil
}

func (o *subprocessOperator) buildRequest(inputs map[string]any) (runRequest, error) {
	encodedInputs := make(map[string]inputValue, len(o.inputs))
	for _, ref := range o.inputs {
		value, ok := inputs[ref.Stream]
		encoded, err := encodeInput(ref.Stream, value, ok)
		if err != nil {
			return runRequest{}, err
		}
		encodedInputs[ref.Stream] = encoded
	}

	outputs := make([]outputBinding, 0, len(o.outputs))
	for _, ref := range o.outputs {
		outputs = append(outputs, outputBinding{
			StreamID: ref.Stream,
			Type:     "",
			Schema:   "",
			Encoding: "json",
		})
	}

	return runRequest{
		OperationID: o.opID,
		Kind:        o.kind,
		Mode:        o.mode,
		Config:      o.config,
		Inputs:      encodedInputs,
		Outputs:     outputs,
		Meta: requestMeta{
			Attempt:         o.attempt,
			TimestampUnixMs: time.Now().UnixMilli(),
		},
	}, nil
}

// Close корректно завершает процесс: закрывает stdin, ждёт exit, при
// необходимости убивает процесс.
func (o *subprocessOperator) Close() error {
	var closeErr error
	o.closeOnce.Do(func() {
		close(o.closed)
		if o.stdin != nil {
			_ = o.stdin.Close()
		}
		if o.cmd == nil || o.cmd.Process == nil {
			return
		}

		done := make(chan error, 1)
		go func() { done <- o.cmd.Wait() }()

		select {
		case err := <-done:
			if err != nil && !isExpectedExit(err) {
				closeErr = err
			}
		case <-time.After(500 * time.Millisecond):
			_ = o.cmd.Process.Kill()
			<-done
		}
	})
	return closeErr
}

func encodeInput(streamID string, value any, present bool) (inputValue, error) {
	encoded := inputValue{
		StreamID: streamID,
		Encoding: "json",
		Present:  present,
	}
	if !present {
		return encoded, nil
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return inputValue{}, fmt.Errorf("marshal input %q: %w", streamID, err)
	}
	encoded.Payload = payload
	return encoded, nil
}

func decodeOutput(out outputValue) (any, error) {
	if len(out.Payload) == 0 {
		return nil, nil
	}
	encoding := out.Encoding
	if encoding == "" {
		encoding = "json"
	}
	switch encoding {
	case "json":
		var value any
		if err := json.Unmarshal(out.Payload, &value); err != nil {
			return nil, fmt.Errorf("unmarshal json payload: %w", err)
		}
		return value, nil
	case "raw", "bytes":
		return out.Payload, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

func readLineWithCtx(ctx context.Context, reader *bufio.Reader) ([]byte, error) {
	type lineResult struct {
		data []byte
		err  error
	}
	ch := make(chan lineResult, 1)
	go func() {
		line, err := reader.ReadBytes('\n')
		ch <- lineResult{data: line, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		if result.err != nil && len(result.data) == 0 {
			return nil, fmt.Errorf("read subprocess response: %w", result.err)
		}
		return result.data, nil
	}
}

func isExpectedExit(err error) bool {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		// мы сами закрыли stdin — Python exit 0 или 1 это ок.
		return true
	}
	return false
}
