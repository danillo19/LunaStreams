# ops/ — реализации операций LunaStreams

Все операции LunaStreams живут здесь и разложены по **driver / языку
исполнения**. Ядро (`internal/runtime`, `internal/graph`, `internal/planner`)
ничего про конкретные операции и языки не знает — оно работает через
абстракцию `runtime.Registry` и интерфейс `runtime.OperationRuntime`.

## Раскладка

```
ops/
├── ops.go                 # общий RegisterAll — единственная точка сборки
├── inprocgo/              # runtime.kind = inproc_go (Go-native)
│   ├── register.go        # Register(registry) — подключает Go-фабрики
│   ├── realtime.go        # microphone.capture, keyboard.read, selectors
│   ├── frontend.go        # frontend.web (HTTP + SSE)
│   ├── mock.go            # webcam.read, output.console и др.
│   └── web/index.html     # UI, embed-ится в бинарь
└── subprocess/            # runtime.kind = subprocess (NDJSON по stdin/stdout)
    ├── driver.go          # сам драйвер + Register(registry)
    └── python/            # эталонные polyglot-операции на Python
        ├── camera_capture.py
        ├── face_presence.py
        └── requirements.txt
```

## Как подключаются операции

Каждый подпакет сам знает, как зарегистрировать себя, и экспортирует
`Register(registry *runtime.Registry) error`. Верхний пакет `ops` собирает
их в одном месте:

```go
func RegisterAll(registry *rt.Registry) error {
    if err := inprocgo.Register(registry); err != nil {
        return err
    }
    if err := subprocess.Register(registry); err != nil {
        return err
    }
    return nil
}
```

Приложение (`cmd/runtime`) и тесты вызывают только `ops.RegisterAll` —
ни одно место кода вне `ops/` не знает конкретных impl-ов.

## Как добавить новый driver

Например, gRPC/Docker:

1. Создайте подпакет `ops/<kind>/` (например, `ops/grpc/`).
2. Реализуйте `runtime.OperationRuntime`:

   ```go
   type Driver struct { /* клиент, пул соединений, ... */ }

   func (d *Driver) Create(spec ir.Operation) (rt.Operator, error) { ... }
   ```

3. Экспортируйте `Register(registry *rt.Registry) error`, который зовёт
   `registry.RegisterDriver("<kind>", NewDriver())`.
4. Добавьте вызов `<kind>.Register(registry)` в `ops/ops.go`.

После этого любая операция в IR с `runtime.kind: <kind>` автоматически
пойдёт через новый драйвер — engine, planner и валидатор менять не надо.

## Как добавить новую Go-реализацию (inproc_go)

1. Положите файл в `ops/inprocgo/` и напишите фабрику
   `func newMyOp(spec ir.Operation) (rt.Operator, error)`.
2. Зарегистрируйте её в `ops/inprocgo/mock.go` в списке `registrations`
   под уникальным `impl`.

## Как добавить новую polyglot-реализацию

- **Python**: положите скрипт в `ops/subprocess/python/`, следуйте
  контракту из [`docs/operation-interface.md`](../docs/operation-interface.md).
- **Другой язык**: любой скрипт/бинарь, умеющий NDJSON на stdin/stdout,
  подходит — протокол не привязан к языку.

В IR укажите:

```yaml
runtime:
  kind: subprocess
  command: [python3, ops/subprocess/python/my_op.py]
```
