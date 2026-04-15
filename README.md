# LunaStreams

`LunaStreams` это MVP runtime для исполнения простого интерактивного IR на Go.

IR теперь разделяет:
- `model.operations[]` и `model.streams[]`: вычислительную модель
- `task.*`: постановку задачи на этой модели

Каждый пример в `examples/` хранится в виде директории с двумя файлами:
- `presence.yaml` — вычислительная модель (operations + streams)
- `task.yaml` — набор постановок задачи (`task_variants`): доступные входы, требуемые выходы, профили операций, `constraints` и `objective`

Такое разделение позволяет фиксировать модель предметной области отдельно от того, какую именно задачу на ней решают. Loader автоматически объединяет все `*.yaml` в директории в один документ.

Флагом `-ir` CLI принимает либо путь к директории примера (новая схема), либо путь к одиночному YAML-файлу (обратная совместимость).

Основной пример — `examples/presence/`: redundant pipeline, где задача отдельно задаёт требования, а planner сам выбирает маршрут. Он же демонстрирует смешанный polyglot-stack: Go-операции для микрофона и клавиатуры, Python-subprocess для камеры и компьютерного зрения.

Подробная архитектура компонентов вынесена в `docs/runtime-components.md`, а polyglot-контракт — в `docs/operation-interface.md`.

## Идея

Runtime исполняет граф операций, где:
- `source` производит события и пишет данные в streams
- `transform` реагирует на события входных streams и вычисляет новые значения
- `selector` периодически читает несколько streams и принимает итоговое решение
- `sink` реагирует на события и выводит/сохраняет результат

Каждый `stream` хранит только последнее значение и счётчик версий `seq`.

## Структура репозитория

```text
cmd/runtime/main.go                 # entrypoint runtime
internal/ir/                        # IR-модель, загрузка, валидация
internal/planner/                   # compile-time выбор redundant планов
internal/graph/                     # построение producer/consumer graph
internal/runtime/                   # engine, store, bus, registry, logger
internal/runtime/ops/               # inproc_go impl операций
internal/runtime/drivers/           # runtime-драйверы: subprocess (NDJSON), задел под docker/grpc/http
examples/                           # примеры IR (каждый пример — отдельная директория)
examples/presence/                  # redundant polyglot-пример: presence.yaml + task.yaml
examples/ops/                       # polyglot-реализации операций (Python)
docs/runtime-components.md          # подробная архитектура компонентов
docs/operation-interface.md         # контракт polyglot-операции (RunRequest / RunResult)
```

## Формат IR

IR задаётся YAML-документом с разделением `model` и `task`:

```yaml
model:
  operations:
    - id: microphone_source
      kind: source
      mode: always_on
      impl: microphone.capture
      outputs:
        - stream: audio_chunk
      config:
        sample_rate: 16000

  streams:
    - id: audio_chunk
      type: audio_chunk

task:
  inputs:
    - stream: audio_chunk

  outputs:
    - stream: presence_decision
```

Для обратной совместимости loader всё ещё понимает старый плоский формат с корневыми `operations[]` и `streams[]`, но новые примеры используют явное разделение.

### Поля `model.operation`

- `id`: уникальный идентификатор операции
- `kind`: тип операции
  - `source`
  - `transform`
  - `selector`
  - `sink`
- `mode`: режим исполнения
  - `always_on`
  - `task_per_event`
- `impl`: строковый ключ реализации, который должен быть зарегистрирован в `Registry`
- `runtime`: описывает, как запускать реализацию (опционально, см. ниже)
- `inputs[]`: входные streams
- `outputs[]`: выходные streams
- `config`: свободная конфигурация реализации
- `domain`: необязательные доменные метаданные операции
  - `cost`: условная стоимость использования операции
  - `weight`: вклад операции в итоговую надёжность/полезность решения

`domain` сохранён для совместимости, но для новых постановок задачи предпочтительно использовать `task.operation_profiles`.

### Поле `operation.runtime`

`runtime` описывает способ запуска реализации операции. Поле опционально:
пустой `runtime` эквивалентен `kind: inproc_go` — Go-фабрика из `Registry`.
Runtime позволяет смешивать на одном графе операции разной природы:

```yaml
runtime:
  kind: subprocess                # | inproc_go | docker | grpc | http
  command: [python3, ops/face_presence.py]
  args: []
  work_dir: ""
  env:
    PYTHONUNBUFFERED: "1"
  image: ""                       # для docker runtime
  endpoint: ""                    # для grpc/http runtime
```

Поддерживаемые kind-ы и их требования:

| kind          | Когда используется                    | Обязательные поля       |
|---------------|----------------------------------------|-------------------------|
| `inproc_go`   | обычная Go-операция                    | —                       |
| `subprocess`  | Python / Node / Rust / bash-процессы   | `command`               |
| `docker`      | контейнеризированный код (roadmap)     | `image`                 |
| `grpc`/`http` | внешний сервис (roadmap)               | `endpoint`              |

Каждому `kind` соответствует драйвер, зарегистрированный в `runtime.Registry`
через `RegisterDriver(kind, driver)`. Драйвер реализует интерфейс
`OperationRuntime.Create(spec) -> Operator`. Чтобы добавить новую среду
исполнения (например, WASM, Kubernetes Job, AWS Lambda), достаточно
реализовать один тип и зарегистрировать его — IR, planner и engine менять
не нужно.

Контракт данных для polyglot-драйверов (RunRequest/RunResult, schema,
encoding) описан в `docs/operation-interface.md`. Справочная реализация
субпроцесс-драйвера лежит в `internal/runtime/drivers/subprocess.go`, а
примеры polyglot-операций — в `examples/ops/`.

### Поля `model.stream`

- `id`: уникальный идентификатор потока
- `type`: логический тип данных
  - `video_frame`
  - `audio_chunk`
  - `bool`
  - `text`

### Поля `task`

- `inputs[]`: какие входные streams считаются доступными для решения задачи
- `outputs[]`: какие выходные streams требуется получить
- `operation_profiles[]`: нефункциональные свойства операций в рамках задачи
  - `operation`
  - `cost`
  - `weight`
  - `latency_ms`
- `constraints`
  - `min_total_weight`
  - `max_total_latency_ms`
  - `max_total_cost`
- `objective`
  - `primary`
  - `secondary`
- `task_variants[]`: альтернативные постановки задачи, которые можно выбрать через `-task`

## Валидация IR

Перед запуском runtime проверяет:
- уникальность `operation.id`
- уникальность `stream.id`
- существование всех streams, указанных в `inputs` и `outputs`
- что `source` не имеет `inputs`
- что `sink` не имеет `outputs`
- что у каждого stream ровно один producer
- что каждый `impl` зарегистрирован в `Registry`
- что `domain.cost` и `domain.weight`, если заданы, неотрицательны

Если хотя бы одно правило нарушено, запуск прекращается на этапе `validate`.

## Как работает runtime

Поток запуска:
1. `cmd/runtime/main.go` читает `-ir`
2. IR загружается из YAML
3. выполняется валидация
4. для redundant selectors при необходимости компилируется execution plan
5. строится dependency graph
6. создаётся `Engine`
7. через `Registry` создаются конкретные операторы
8. запускаются `always_on source` и `always_on selector`
9. dispatcher начинает слушать общий `EventBus`

### Общая модель исполнения

- `StreamStore` хранит последнее значение каждого stream и его `seq`
- `EventBus` публикует события вида `{StreamID, Seq}`
- dispatcher читает `EventBus` и запускает все `task_per_event` операции, подписанные на соответствующий stream
- результат операции записывается обратно в `StreamStore`
- после записи публикуется новое событие в `EventBus`

### Гарантия по параллелизму

Для `task_per_event` операций в MVP включён `max_in_flight=1`:
- на каждую операцию выделен свой семафор
- если операция уже выполняется, повторный запуск пропускается

Это защищает runtime от повторного параллельного запуска одной и той же операции при быстром потоке событий.

## Виды операций

### `source`

`source` не имеет входов и сам генерирует выходные данные.

Примеры:
- `webcam.read`
- `microphone.read`
- `microphone.capture`
- `keyboard.read`

Обычно запускается в `always_on` режиме: отдельная goroutine циклически вызывает `Run()`.

### `transform`

`transform` получает входы из `StreamStore` и создаёт новые значения.

Примеры:
- `cv.face_presence`
- `cv.motion_presence`
- `audio.voice_presence`
- `audio.volume_presence`

Обычно запускается в `task_per_event` режиме: при событии на одном из входных streams.

### `selector`

`selector` агрегирует несколько streams и вычисляет итоговое решение.

Он полезен, когда:
- нужно объединить несколько источников сигналов
- нужна логика приоритетов
- решение должно вычисляться периодически

Примеры:
- `selector.priority_failover`
- `selector.audio_or_recent_key`
- `selector.redundant_choice`

### `sink`

`sink` не производит outputs, а завершает цепочку.

Примеры:
- `output.console` — печатает последнее значение stream в лог
- `frontend.web` — запускает HTTP-сервер (SSE) и стримит состояние pipeline
  в браузер (camera, audio RMS, presence-индикатор)

`sink` обычно срабатывает в `task_per_event` режиме и реагирует на каждое
событие любого из своих входных streams. `frontend.web` использует это,
чтобы на каждый apдейт рассылать свежий snapshot подписанным HTTP-клиентам.

## Реальные реализации операций

### `microphone.capture`

Реальный `source`, который:
- создаёт `malgo` context
- перечисляет capture devices
- по желанию выбирает устройство по `config.device_name_contains`
- запускает устройство захвата
- конвертирует входные байты в `AudioChunk`
- считает `RMS`
- публикует `audio_chunk`

Поддерживаемые настройки:
- `sample_rate`
- `channels`
- `frames_per_chunk`
- `device_name_contains`

Важно:
- в sandbox среде доступ к аудио backend может не работать
- для реальной работы лучше запускать в обычном терминале пользователя

### `keyboard.read`

Реальный `source`, который читает символы из `os.Stdin`.

Текущее поведение:
- это не глобальный keyboard hook ОС
- это чтение из stdin процесса runtime
- сейчас ввод line-buffered, поэтому символы приходят после `Enter`
- `Enter` сам игнорируется

### `audio.volume_presence`

Реальный `transform`, который:
- получает `AudioChunk`
- берёт его `RMS`
- сравнивает с `config.threshold`
- пишет `bool` stream

### `selector.audio_or_recent_key`

Реальный `selector`, который:
- читает `audio_presence`
- читает последнее `pressed_key`
- проверяет, насколько недавно пришёл `KeyEvent`
- возвращает `audio_presence || recent_key`

Поддерживаемые настройки:
- `audio_stream`
- `key_stream`
- `keyboard_window_ms`
- `tick_ms`

### `selector.redundant_choice`

Selector для избыточного описания одного доменного факта. В новой схеме он описывает в `model` множество допустимых альтернативных сигналов, а planner до старта runtime сам выбирает execution plan на основе `task`.

Поддерживаемые настройки в `model.operation.config`:
- `default_choice`
- `variants[]`
- `tick_ms`

Каждый элемент `variants[]` может описывать:
- `kind: bool` для обычного булевого stream
- `kind: recent_key` для проверки свежести последнего `KeyEvent`
- `cost`, `weight` и `latency_ms`, если нужно переопределить профили producer-операции
- `latency_ms`, если задача ограничивает суммарную задержку выбранного набора

Если `cost`, `weight` и `latency_ms` не заданы внутри `variants[]`, planner/runtime сначала пытается взять их из `task.operation_profiles[]`, а затем уже из `operation.domain` producer-операции.

Planner для требуемых `task.outputs`:
- берёт из `model` все альтернативы selector'а
- отбрасывает ветки, которым не хватает доступных `task.inputs`
- строит кандидатные подграфы
- оценивает их по `cost/weight/latency`
- выбирает лучший план по `task.constraints` и `task.objective`
- переписывает selector в already-selected режим и вырезает неиспользуемые ветки из документа перед `Build/NewEngine`

Если выбранный или активный набор даёт решение, selector публикует:
- `presence_decision = true`
- строку стратегии вида `<labels> | cost=<sum> weight=<sum>`

Если решение не сработало, selector публикует:
- `presence_decision = false`
- `default_choice`

## Пример: redundant presence

`examples/presence/` показывает, как вычислительная модель отделяется от постановки задачи для доменного факта `presence`. Модель лежит в `examples/presence/presence.yaml`, набор `task_variants` — в `examples/presence/task.yaml`.

### Логика графа

- `microphone_source` через `microphone.capture` пишет в `audio_chunk`
- два `audio.volume_presence` строят два альтернативных сигнала: `loud_audio_presence` и `soft_audio_presence`
- `keyboard_source` через `keyboard.read` пишет в `pressed_key`
- сама задача не перечисляет selector и не задаёт маршрут явно
- задача отдельно задаёт:
- доступные входы и требуемые выходы
- профили операций (`cost/weight/latency_ms`)
- ограничения (`min_total_weight`, `max_total_latency_ms`)
- целевую функцию (`objective`)
- planner до старта runtime сам выводит допустимые маршруты из `model` и выбирает лучший план
- в этом примере planner оставляет `soft_audio_presence + pressed_key` и вырезает ветку `loud_audio_presence`
- после компиляции selector работает только по выбранному плану и публикует:
- `presence_decision`
- `presence_strategy`

### Что демонстрирует пример

- один и тот же доменный факт `presence` описан несколькими избыточными способами
- маршрут исполнения не задаётся в `task`, а выводится planner'ом из `model`
- разные `task_variants` могут приводить к разным итоговым execution plan
- система умеет выбрать избыточное описание предметной области до старта runtime, а не после вычисления всех дорогих веток
- дорогая операция может вообще не попасть в execution plan, если более дешёвый набор операций даёт достаточный суммарный вес

## Логирование

Runtime использует `BeautifulLogger` с уровнями:
- `INFO`
- `DEBUG`
- `ERROR`
- `RESULT`

`RESULT` используется для финальных значений в `sink`.

Пример:

```text
23:57:05.370 | RESULT | sink       | presence_decision <= true
```

Для подробной трассировки можно включить debug:

```bash
go run ./cmd/runtime -ir ./examples/presence -task low_cost_interactive -debug
```

## Запуск

CLI принимает путь к директории примера через `-ir` и id постановки задачи через `-task`. Loader сам объединит `presence.yaml` + `task.yaml` в один документ.

### Redundant presence (low cost)

```bash
go run ./cmd/runtime -ir ./examples/presence -task low_cost_interactive -debug
```

Другой вариант постановки задачи:

```bash
go run ./cmd/runtime -ir ./examples/presence -task high_confidence_monitoring -debug
```

Если нужно зафиксировать конкретный микрофон:

```yaml
config:
  device_name_contains: "MacBook"
```

### Camera + Python vision

`camera_vision_accurate` прогоняет камеру через Python-операцию детекции
лица (OpenCV Haar cascade):

```bash
pip install -r examples/ops/requirements.txt
go run ./cmd/runtime -ir ./examples/presence -task camera_vision_accurate -debug
```

### Web UI (frontend.web sink)

`frontend_full` поднимает простой HTTP-сервер и отдаёт в браузер камеру,
audio RMS и индикатор presence. Это обычный sink (`impl: frontend.web`),
который сам по себе слушает 127.0.0.1:8080 и стримит состояние через
Server-Sent Events:

```bash
pip install -r examples/ops/requirements.txt
go run ./cmd/runtime -ir ./examples/presence -task frontend_full -debug
# далее открыть http://127.0.0.1:8080
```

UI отображает:
- последний `camera_frame` (JPEG, base64-encoded) из Python subprocess
- RMS звука по последнему `audio_chunk` из Go microphone источника
- булевый индикатор `presence_decision` и текст `presence_strategy` от selector'а

Frontend sink умеет работать и в урезанных task-вариантах: planner автоматически
дропает из его inputs стримы, которые не вошли в выбранный план (например,
если task не запрашивает камеру, sink останется, но без `camera_frame`).

Go runtime запускает `examples/ops/camera_capture.py` и
`examples/ops/face_presence.py` как долгоживущие subprocess-ы и общается
с ними по protocol-у из `docs/operation-interface.md`. Никакого C-binding
для камеры в Go не требуется — любая реализация сводится к добавлению
скрипта/бинаря и записи `runtime.kind` + `runtime.command` в IR.

## Точка входа

`cmd/runtime/main.go`:
- читает путь к IR через `-ir`
- при необходимости выбирает `task` через `-task`
- создаёт logger
- регистрирует все операции
- выполняет `Load -> Validate -> CompileRedundantChoices -> Validate -> Build -> NewEngine -> Start`
- ждёт `Ctrl+C`

## Ограничения текущего MVP

- `StreamStore` хранит только последнее значение stream, без истории
- `task_per_event` операции имеют `max_in_flight=1`
- `keyboard.read` пока требует `Enter`
- типы streams описаны логически, но не enforced на уровне generic-типов
- selector запускается по ticker, а не напрямую по событию

## Что читать дальше

- общее устройство компонентов: `docs/runtime-components.md`
- пример выбора между избыточными вариантами: `examples/presence/` (`presence.yaml` + `task.yaml`)
- polyglot-контракт операций: `docs/operation-interface.md`
- entrypoint runtime: `cmd/runtime/main.go`