package subprocess

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"LunaStreams/internal/ir"
)

func TestSubprocessDriverRoundTripJSON(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("subprocess round-trip test requires POSIX shell")
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("sh not available: %v", err)
	}

	script := filepath.Join(t.TempDir(), "echo_operator.sh")
	if err := os.WriteFile(script, []byte(echoOperatorScript), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	driver := NewDriver(nil)
	operator, err := driver.Create(ir.Operation{
		ID:   "echo_op",
		Kind: ir.OperationKindTransform,
		Mode: ir.OperationModeTaskPerEvent,
		Impl: "test.echo",
		Runtime: ir.RuntimeSpec{
			Kind:    "subprocess",
			Command: []string{"sh", script},
		},
		Inputs:  []ir.StreamRef{{Stream: "in"}},
		Outputs: []ir.StreamRef{{Stream: "out"}},
		Config:  map[string]any{"greeting": "hello"},
	})
	if err != nil {
		t.Fatalf("driver.Create() error = %v", err)
	}
	t.Cleanup(func() {
		if closer, ok := operator.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	outputs, err := operator.Run(ctx, map[string]any{"in": map[string]any{"value": 42}})
	if err != nil {
		t.Fatalf("operator.Run() error = %v", err)
	}
	value, ok := outputs["out"]
	if !ok {
		t.Fatalf("missing output stream; got %#v", outputs)
	}
	if value != true {
		t.Fatalf("output[out] = %#v, want true", value)
	}
}

// echoOperatorScript реализует минимальный subprocess-оператор: читает
// каждую строку stdin как RunRequest и отвечает RunResult-ом с bool true.
// Содержимое полезной нагрузки вычисляется один раз заранее.
var echoOperatorScript = `#!/bin/sh
set -e
PAYLOAD=` + shellQuote(mustBase64([]byte("true"))) + `
while IFS= read -r line; do
  printf '{"outputs":{"out":{"stream_id":"out","type":"bool","encoding":"json","payload":"%s"}}}\n' "$PAYLOAD"
done
`

func mustBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func shellQuote(value string) string {
	out, _ := json.Marshal(value)
	return string(out)
}
