# LunaStreams

`LunaStreams` — runtime для исполнения графа операций, описанного в YAML IR.

IR разделён на две части:
- `model` — вычислительная модель: операции, streams, связи
- `task` — постановка задачи: доступные входы, требуемые выходы, ограничения, конфиг

Основной пример — `examples/presence/`: один `model.yaml` и несколько `task_*.yaml`.

## Структура

```text
cmd/runtime/                 # запуск runtime
cmd/gen-graphs/              # генерация Mermaid-диаграмм для examples/presence
internal/ir/                 # модель IR, load/resolve/validate
internal/planner/            # compile-time выбор redundant-плана
internal/graph/              # producer/consumer graph
internal/runtime/            # engine, registry, bus, store, logger
ops/inprocgo/                # Go-native операции
ops/subprocess/              # subprocess driver
ops/subprocess/python/       # Python операции и launcher-скрипты
examples/presence/           # model.yaml + task_*.yaml
examples/presence/graphs/    # mmd/svg/png для presence
docs/                        # документация и общие диаграммы
```

## Как работает runtime

Поток запуска:

1. `ops.RegisterAll` регистрирует реализации и runtime-драйверы.
2. `ir.Load` читает YAML-файл или объединяет все `*.yaml` в директории.
3. `ir.ResolveTask` выбирает `task` и накладывает `task.operation_configs`.
4. `ir.Validate` проверяет IR.
5. `planner.CompileRedundantChoices` выбирает нужные ветки и вырезает лишние.
6. `graph.Build` строит producer/consumer graph.
7. `runtime.NewEngine` создаёт операторы.
8. `engine.Start` запускает `always_on` операции и dispatcher.

Операции:
- `source` — генерирует данные
- `transform` — преобразует входные streams
- `sink` — завершает цепочку побочным эффектом

Режимы:
- `always_on`
- `task_per_event`

## Формат IR

Минимальная схема:

```yaml
model:
  operations:
    - id: microphone_source
      kind: source
      mode: always_on
      impl: microphone.capture
      outputs:
        - stream: audio_chunk
  streams:
    - id: audio_chunk
      type: audio_chunk

task:
  id: audio_only_fast
  inputs:
    - stream: audio_chunk
  outputs:
    - stream: presence_decision
    - sink: presence_sink
  operation_configs:
    - operation: microphone_source
      config:
        sample_rate: 16000
```

`task.outputs[]` — единый список целей:
- `stream: <id>` — нужен выходной stream
- `sink: <id>` — нужен конкретный sink

Planner строит граф от `outputs`: оставляет только нужные операции, streams и sinks.

## Runtime kinds

`operation.runtime.kind` определяет, где исполняется операция:

- `inproc_go` или пусто — Go factory из registry
- `subprocess` — внешний процесс по NDJSON-протоколу
- `docker` — заготовка под контейнер
- `grpc` / `http` — заготовка под внешний сервис

Для `subprocess` используются `ops/subprocess/driver.go` и polyglot-контракт из `docs/operation-interface.md`.

## Пример `presence`

`examples/presence/model.yaml` содержит:
- микрофон и аудио presence в Go
- клавиатуру в Go
- камеру и face detection в Python subprocess
- selector для выбора лучшего плана
- console sinks и web frontend sink

Варианты задач лежат рядом:
- `task_audio_only_fast.yaml`
- `task_low_cost_interactive.yaml`
- `task_high_confidence_monitoring.yaml`
- `task_keyboard_only_degraded.yaml`
- `task_camera_vision_accurate.yaml`
- `task_camera_plus_soft_audio.yaml`
- `task_frontend_full.yaml`

## Запуск

Обычный запуск:

```bash
go run ./cmd/runtime -ir ./examples/presence -task low_cost_interactive -debug
```

Другие варианты:

```bash
go run ./cmd/runtime -ir ./examples/presence -task audio_only_fast -debug
go run ./cmd/runtime -ir ./examples/presence -task camera_vision_accurate -debug
go run ./cmd/runtime -ir ./examples/presence -task frontend_full -debug
```

## Python и `.venv`

Для camera/vision:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r ops/subprocess/python/requirements.txt
```

Python subprocess-операции запускаются через `run_*.sh`, которые сначала ищут `.venv/bin/python`, затем fallback на `python3`.

## macOS: микрофон и камера

На macOS доступ выдаётся приложению, которое запустило процесс: `Terminal.app`, `Visual Studio Code`, `GoLand`.

Если нет аудио или камера не открывается:
- выдай права в `System Settings -> Privacy & Security -> Microphone / Camera`
- запускай runtime из внешнего терминала

## Диаграммы

Presence-диаграммы генерируются из текущего IR:

```bash
make graphs-mmd
make graphs-validate
make graphs
```

Что делает:
- `make graphs-mmd` — обновляет `examples/presence/graphs/*.mmd`
- `make graphs-validate` — проверяет Mermaid-схемы
- `make graphs` — валидирует и рендерит `svg/png` для `docs/diagrams` и `examples/presence/graphs`

## Где смотреть дальше

- `docs/runtime-components.md` — устройство runtime
- `docs/operation-interface.md` — контракт subprocess-операций
- `examples/presence/` — основной пример
