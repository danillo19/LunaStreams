# Operation Interface

Этот документ описывает, каким должен быть интерфейс операции

Ниже описана рекомендуемая модель для следующей версии runtime.

## Зачем менять текущий интерфейс

Сейчас операция в MVP выглядит очень просто:

```go
type Operator interface {
    Run(ctx context.Context, inputs map[string]any) (map[string]any, error)
}
```

Такой интерфейс удобен внутри одного Go-процесса, но плохо подходит для:
- сериализации данных
- межпроцессного вызова
- вызова операций на Python, Rust, Node.js и других языках
- строгой типизации streams
- стабильной схемы данных между runtime и операцией


## Требования к новому интерфейсу операции

Новый интерфейс должен:
- явно получать входные данные, а не просто `any`
- явно возвращать выходные данные
- включать metadata stream-а
- включать execution metadata
- быть сериализуемым
- не зависеть от конкретного языка реализации

## Рекомендуемые структуры на Go

### Основные типы

```go
package runtime

import "context"

type Operation interface {
    Run(ctx context.Context, req RunRequest) (RunResult, error)
}

type ClosableOperation interface {
    Close() error
}

type ValidatableOperation interface {
    ValidateConfig(spec OperationSpec) error
}
```

### Спецификация операции

`OperationSpec` это нормализованное описание операции, которое runtime передаёт в factory/driver:

```go
type OperationSpec struct {
    ID      string
    Kind    string
    Mode    string
    Impl    string
    Inputs  []StreamBinding
    Outputs []StreamBinding
    Config  map[string]any
    Runtime RuntimeSpec
}

type StreamBinding struct {
    StreamID string
    Type     string
    Schema   string
    Encoding string
}
```

### Описание runtime-режима

Это нужно, чтобы runtime понимал, как именно запускать impl:

```go
type RuntimeSpec struct {
    Kind    string            // inproc_go | subprocess | grpc
    Command []string          // для subprocess
    Endpoint string           // для grpc
    Env     map[string]string // дополнительные переменные среды
}
```

### Запрос на выполнение операции

```go
type RunRequest struct {
    OperationID string
    Kind        string
    Mode        string
    Config      map[string]any
    Inputs      map[string]InputValue
    Meta        RequestMeta
}

type RequestMeta struct {
    TriggerStreamID string
    TriggerSeq      uint64
    Attempt         int
    TimestampUnixMs int64
}
```

### Входное значение

```go
type InputValue struct {
    StreamID string
    Type     string
    Schema   string
    Encoding string
    Seq      uint64
    Present  bool
    Payload  []byte
    Meta     map[string]string
}
```

### Результат выполнения

```go
type RunResult struct {
    Outputs map[string]OutputValue
    Meta    map[string]string
}

type OutputValue struct {
    StreamID string
    Type     string
    Schema   string
    Encoding string
    Payload  []byte
    Meta     map[string]string
}
```

## Почему именно такой контракт

Этот контракт решает основные проблемы текущего MVP:

`Payload []byte` можно передать:
- по сети
- через stdin/stdout
- через gRPC
- через message broker

### 2. У каждого значения есть metadata

Операция видит:
- из какого stream пришёл input
- какой это тип
- какая схема
- какой `seq`
- есть значение или нет

### 3. Интерфейс не привязан к Go runtime

Этот же `RunRequest` можно представить:
- как JSON
- как Protobuf
- как MessagePack

### 4. Runtime может запускать операции по-разному

Через `RuntimeSpec` можно поддержать несколько execution modes:
- встроенная Go-операция
- subprocess
- gRPC-сервис

## Что должен уметь runtime

Чтобы такой интерфейс реально работал, runtime должен получить несколько новых абстракций.

### Codec

```go
type Codec interface {
    Encode(value any, schema string, encoding string) ([]byte, error)
    Decode(payload []byte, schema string, encoding string) (any, error)
}
```

Назначение:
- сериализация значений streams
- десериализация для локальных операций

### OperationRuntime / Driver

```go
type OperationRuntime interface {
    Create(spec OperationSpec) (Operation, error)
}
```

Идея:
- `inproc_go` runtime создаёт обычный Go object
- `subprocess` runtime запускает внешний процесс
- `grpc` runtime создаёт клиент к удалённой операции

### Registry нового типа

Вместо текущего registry, который знает только Go factories, нужен более общий registry:

```go
type RuntimeRegistry interface {
    Register(kind string, runtime OperationRuntime) error
    Get(kind string) (OperationRuntime, bool)
}
```

Тогда runtime выбирает:
1. `spec.Runtime.Kind`
2. нужный `OperationRuntime`
3. вызывает `Create(spec)`

## Как должен выглядеть внешний wire contract

Самый простой и понятный способ для polyglot-support:
- JSON для MVP
- Protobuf для production/distributed режима

Ниже JSON-форма того же `RunRequest`:

```json
{
  "operation_id": "volume_presence",
  "kind": "transform",
  "mode": "task_per_event",
  "config": {
    "threshold": 0.08
  },
  "inputs": {
    "audio_chunk": {
      "stream_id": "audio_chunk",
      "type": "audio_chunk",
      "schema": "luna.audio_chunk.v1",
      "encoding": "json",
      "seq": 42,
      "present": true,
      "payload": "eyJzZXEiOjQyLCJybXMiOjAuMTJ9",
      "meta": {}
    }
  },
  "meta": {
    "trigger_stream_id": "audio_chunk",
    "trigger_seq": 42,
    "attempt": 1,
    "timestamp_unix_ms": 1741500000000
  }
}
```

`payload` здесь base64-представление байтов.

## Как может выглядеть операция в YAML

Ниже пример операции, которая может быть реализована не на Go, а как внешний Python subprocess.

```yaml
operations:
  - id: volume_presence
    kind: transform
    mode: task_per_event
    impl: audio.volume_presence
    runtime:
      kind: subprocess
      command:
        - python3
        - ops/volume_presence.py
    inputs:
      - stream: audio_chunk
        type: audio_chunk
        schema: luna.audio_chunk.v1
        encoding: json
    outputs:
      - stream: audio_presence
        type: bool
        schema: luna.bool.v1
        encoding: json
    config:
      threshold: 0.08

streams:
  - id: audio_chunk
    type: audio_chunk
  - id: audio_presence
    type: bool
```

### Что тут важно

- `impl` остаётся логическим именем операции
- `runtime.kind` говорит, как запускать impl
- `command` описывает внешний процесс
- `schema` и `encoding` задают стабильный контракт данных

## Пример внешней операции на Python

Ниже пример очень простой реализации `audio.volume_presence` на Python.

Она:
- получает `RunRequest` через stdin
- читает `audio_chunk`
- извлекает `rms`
- сравнивает с порогом
- печатает `RunResult` в stdout

```python
#!/usr/bin/env python3
import base64
import json
import sys


def decode_payload(input_value: dict) -> dict:
    raw = base64.b64decode(input_value["payload"])
    return json.loads(raw.decode("utf-8"))


def encode_payload(value) -> str:
    raw = json.dumps(value).encode("utf-8")
    return base64.b64encode(raw).decode("utf-8")


def main() -> int:
    request = json.load(sys.stdin)

    threshold = float(request.get("config", {}).get("threshold", 0.08))
    audio_input = request["inputs"].get("audio_chunk")

    if not audio_input or not audio_input.get("present"):
        result = False
    else:
        chunk = decode_payload(audio_input)
        rms = float(chunk.get("rms", 0.0))
        result = rms >= threshold

    response = {
        "outputs": {
            "audio_presence": {
                "stream_id": "audio_presence",
                "type": "bool",
                "schema": "luna.bool.v1",
                "encoding": "json",
                "payload": encode_payload(result),
                "meta": {}
            }
        },
        "meta": {}
    }

    json.dump(response, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

## Как runtime будет вызывать такую операцию

Для `subprocess`-варианта runtime должен:
1. прочитать `runtime.command` из IR
2. запустить процесс
3. отправить `RunRequest` в stdin
4. прочитать `RunResult` из stdout
5. провалидировать outputs
6. записать outputs в streams

Для `grpc`-варианта runtime делает то же логически, но вместо stdin/stdout использует RPC-вызов.

## Какие режимы запуска операций стоит поддержать

### 1. `inproc_go`

Лучший вариант для MVP и fast path.

Плюсы:
- минимальная задержка
- простой дебаг
- нет сетевого overhead

Минусы:
- только Go
- impl должен быть встроен в бинарник

### 2. `subprocess`

Хороший первый шаг для polyglot-support.

Плюсы:
- легко подключить Python/Node/Rust
- просто реализовать
- удобно для экспериментов

Минусы:
- выше задержка
- сложнее lifecycle
- хуже для high-throughput

### 3. `grpc`

Правильный вариант для распределённой системы.

Плюсы:
- язык-независимость
- удалённое исполнение
- health checks
- проще масштабирование

Минусы:
- больше инфраструктуры
- сложнее локальная разработка

## Что нужно изменить в текущем MVP минимально

Если делать самый практичный следующий шаг, то лучше менять систему так:

### Шаг 1

Заменить текущий интерфейс:

```go
type Operator interface {
    Run(ctx context.Context, inputs map[string]any) (map[string]any, error)
}
```

на:

```go
type Operation interface {
    Run(ctx context.Context, req RunRequest) (RunResult, error)
}
```

### Шаг 2

Ввести `InputValue` и `OutputValue` вместо голых `any`.

### Шаг 3

Добавить `Codec` слой.

### Шаг 4

Добавить `runtime.kind` в IR:
- `inproc_go`
- `subprocess`
- позже `grpc`

### Шаг 5

Переписать registry так, чтобы он создавал не только Go impl, но и runtime drivers.

## Рекомендуемый минимальный целевой дизайн

Для ближайшей версии системы я бы рекомендовал:

### Внутри Go runtime
- `Operation`
- `RunRequest`
- `RunResult`
- `InputValue`
- `OutputValue`
- `Codec`
- `OperationRuntime`

### В IR
- `runtime.kind`
- `runtime.command`
- `runtime.endpoint`
- `schema`
- `encoding`

### Для внешних языков
- JSON contract для MVP
- Protobuf contract для production

## Итог

Чтобы операция могла быть полноценной частью системы в MVP и в будущем работать не только на Go, нужны три вещи:

### 1. Нормальный контракт вызова

Не `map[string]any`, а структурированный `RunRequest` / `RunResult`.

### 2. Нормальный контракт данных

Не локальные Go values, а сериализуемые payloads со schema и encoding.

### 3. Отделение impl от способа запуска

Операция должна описывать:
- что она делает (`impl`)
- как её запускать (`runtime.kind`)

Тогда одна и та же система сможет выполнять операции:
- внутри Go runtime
- как subprocess
- как удалённый сервис на другом языке
