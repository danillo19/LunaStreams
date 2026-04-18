// Package subprocess реализует runtime-драйвер, запускающий операцию как
// отдельный долгоживущий процесс и общающийся с ней по polyglot-контракту
// (NDJSON поверх stdin/stdout). См. docs/operation-interface.md.
//
// Пакет также хостит эталонные реализации операций на других языках — см.
// подкаталог python/. Добавление новой языковой реализации — это
// новый скрипт рядом со ссылкой на него в YAML через runtime.command.
//
// Регистрация драйвера в общем Registry выполняется через Register.
package subprocess

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

// DriverKind — runtime.kind, под которым драйвер регистрируется.
const DriverKind = "subprocess"

// Driver запускает каждую операцию в отдельном долгоживущем процессе.
// Взаимодействие идёт через stdin/stdout в формате NDJSON:
// одна строка на запрос, одна строка на ответ. Формат совместим с
// polyglot-контрактом (RunRequest / RunResult).
type Driver struct {
	logger *rt.BeautifulLogger
}

// NewDriver создаёт драйвер для kind="subprocess".
func NewDriver(logger *rt.BeautifulLogger) *Driver {
	return &Driver{logger: logger}
}

// Register регистрирует subprocess-драйвер в переданном Registry. Удобно
// вызывать из общего ops.RegisterAll.
func Register(registry *rt.Registry) error {
	return registry.RegisterDriver(DriverKind, NewDriver(rt.DefaultLogger()))
}

// Create запускает процесс и возвращает оператор, сериализующий вызовы.
func (d *Driver) Create(spec ir.Operation) (rt.Operator, error) {
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

	command []string
	args    []string
	workDir string
	env     map[string]string

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader

	mu        sync.Mutex
	attempt   int
	closeOnce sync.Once
	closed    chan struct{}

	stderrMu   sync.Mutex
	stderrTail []string
}

func newSubprocessOperator(spec ir.Operation, logger *rt.BeautifulLogger, command, extraArgs []string, workDir string, env map[string]string) (*subprocessOperator, error) {
	op := &subprocessOperator{
		opID:     spec.ID,
		kind:     string(spec.Kind),
		mode:     string(spec.Mode),
		config:   spec.Config,
		inputs:   append([]ir.StreamRef(nil), spec.Inputs...),
		outputs:  append([]ir.StreamRef(nil), spec.Outputs...),
		logger:   logger,
		command:  append([]string(nil), command...),
		args:     append([]string(nil), extraArgs...),
		workDir:  workDir,
		env:      cloneRuntimeEnv(env),
		closed:   make(chan struct{}),
		stderrMu: sync.Mutex{},
	}
	if err := op.startProcessLocked(); err != nil {
		return nil, err
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
		o.pushStderrLine(line)
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
		if isRecoverableSubprocessIO(err) {
			restartErr := o.restartProcessLocked()
			if restartErr == nil {
				if _, retryErr := o.stdin.Write(append(payload, '\n')); retryErr == nil {
					return o.readAndDecode(ctx)
				} else {
					return nil, o.withStderrTail(fmt.Errorf("write to subprocess after restart: %w", retryErr))
				}
			}
			return nil, o.withStderrTail(fmt.Errorf("write to subprocess: %w (restart failed: %v)", err, restartErr))
		}
		return nil, o.withStderrTail(fmt.Errorf("write to subprocess: %w", err))
	}
	return o.readAndDecode(ctx)
}

func (o *subprocessOperator) readAndDecode(ctx context.Context) (map[string]any, error) {
	line, err := readLineWithCtx(ctx, o.stdout)
	if err != nil {
		if isRecoverableSubprocessIO(err) {
			restartErr := o.restartProcessLocked()
			if restartErr == nil {
				return nil, o.withStderrTail(fmt.Errorf("read subprocess response: %w (process restarted; retry next event)", err))
			}
			return nil, o.withStderrTail(fmt.Errorf("read subprocess response: %w (restart failed: %v)", err, restartErr))
		}
		return nil, o.withStderrTail(err)
	}

	var result runResult
	if err := json.Unmarshal(line, &result); err != nil {
		return nil, fmt.Errorf("decode subprocess response: %w (line=%s)", err, strings.TrimSpace(string(line)))
	}
	if result.Error != "" {
		return nil, o.withStderrTail(fmt.Errorf("subprocess error: %s", result.Error))
	}

	outputs := make(map[string]any, len(result.Outputs))
	for streamID, out := range result.Outputs {
		value, err := decodeOutput(out)
		if err != nil {
			return nil, o.withStderrTail(fmt.Errorf("decode output %q: %w", streamID, err))
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
		if err := o.stopProcessLocked(); err != nil {
			closeErr = err
		}
	})
	return closeErr
}

func (o *subprocessOperator) startProcessLocked() error {
	if len(o.command) == 0 {
		return fmt.Errorf("subprocess op %q has empty command", o.opID)
	}

	name := o.command[0]
	args := append([]string{}, o.command[1:]...)
	args = append(args, o.args...)

	cmd := exec.Command(name, args...)
	cmd.Dir = o.workDir
	if len(o.env) > 0 {
		envSlice := make([]string, 0, len(o.env))
		for key, value := range o.env {
			envSlice = append(envSlice, fmt.Sprintf("%s=%s", key, value))
		}
		cmd.Env = append(cmd.Environ(), envSlice...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start subprocess: %w", err)
	}

	o.cmd = cmd
	o.stdin = stdin
	o.stdout = bufio.NewReaderSize(stdout, 1<<16)
	go o.forwardStderr(stderr)
	if o.logger != nil {
		o.logger.Info(o.opID, "subprocess started pid=%d cmd=%q", cmd.Process.Pid, strings.Join(append([]string{name}, args...), " "))
	}
	return nil
}

func (o *subprocessOperator) stopProcessLocked() error {
	if o.stdin != nil {
		_ = o.stdin.Close()
		o.stdin = nil
	}
	if o.cmd == nil || o.cmd.Process == nil {
		o.cmd = nil
		o.stdout = nil
		return nil
	}

	done := make(chan error, 1)
	cmd := o.cmd
	go func() { done <- cmd.Wait() }()

	var stopErr error
	select {
	case err := <-done:
		if err != nil && !isExpectedExit(err) {
			stopErr = err
		}
	case <-time.After(500 * time.Millisecond):
		_ = cmd.Process.Kill()
		<-done
	}

	o.cmd = nil
	o.stdout = nil
	return stopErr
}

func (o *subprocessOperator) restartProcessLocked() error {
	_ = o.stopProcessLocked()
	return o.startProcessLocked()
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

func isRecoverableSubprocessIO(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "read subprocess response: eof")
}

func (o *subprocessOperator) withStderrTail(err error) error {
	if err == nil {
		return nil
	}
	tail := o.stderrSnapshot()
	if tail == "" {
		return err
	}
	return fmt.Errorf("%w | stderr_tail=%q", err, tail)
}

func (o *subprocessOperator) pushStderrLine(line string) {
	o.stderrMu.Lock()
	defer o.stderrMu.Unlock()
	const maxTail = 8
	o.stderrTail = append(o.stderrTail, line)
	if len(o.stderrTail) > maxTail {
		o.stderrTail = o.stderrTail[len(o.stderrTail)-maxTail:]
	}
}

func (o *subprocessOperator) stderrSnapshot() string {
	o.stderrMu.Lock()
	defer o.stderrMu.Unlock()
	if len(o.stderrTail) == 0 {
		return ""
	}
	return strings.Join(o.stderrTail, " || ")
}

func cloneRuntimeEnv(env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = value
	}
	return out
}
