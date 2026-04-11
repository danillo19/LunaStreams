# LunaStreams

`LunaStreams` это MVP runtime для исполнения простого интерактивного IR на Go.

IR теперь разделяет:
- `model.operations[]` и `model.streams[]`: вычислительную модель
- `task.*`: постановку задачи на этой модели

Проект уже поддерживает три основных сценария:
- `examples/presence.yaml`: полностью mock pipeline для presence detection
- `examples/presence_v2.yaml`: pipeline с реальным микрофоном и клавиатурой
- `examples/presence_redundant_v2.yaml`: redundant pipeline, где задача отдельно задаёт требования и planner сам выбирает маршрут. Этот же пример демонстрирует смешанный polyglot-stack: Go-операции для микрофона и клавиатуры, Python-subprocess для камеры и компьютерного зрения.

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
examples/                           # примеры IR
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

Пример:
- `output.console`

`sink` обычно срабатывает в `task_per_event` режиме и печатает последнее значение в лог.

## Пример 1: mock presence pipeline

`examples/presence.yaml` содержит полностью mock-граф:

- `webcam_source -> video_frame`
- `microphone_source -> audio_chunk`
- `face_presence(video_frame) -> face_presence`
- `motion_presence(video_frame) -> motion_presence`
- `voice_presence(audio_chunk) -> voice_presence`
- `presence_selector(face_presence, motion_presence, voice_presence) -> presence_decision`
- `decision_sink(presence_decision)`

Это пример для проверки IR, графа и runtime без реальных устройств.

## Пример 2: real presence_v2

`examples/presence_v2.yaml` сочетает реальный микрофон и клавиатуру.

### Логика графа

- `microphone_source` через `microphone.capture` пишет в `audio_chunk`
- `volume_presence` вычисляет `audio_presence` по RMS громкости
- `keyboard_source` через `keyboard.read` пишет `pressed_key`
- `presence_selector` через `selector.audio_or_recent_key` выдаёт `presence_decision`
- `presence_sink` печатает `presence_decision`
- `keyboard_sink` печатает сами события клавиатуры

### Когда `presence_decision = true`

В `presence_v2` итоговое presence становится `true`, если выполняется хотя бы одно условие:
- RMS аудио выше порога `threshold`
- была клавиша за последние `keyboard_window_ms`

Это даёт две независимые ветки влияния на итоговое presence:
- звук
- клавиатурная активность

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

## Пример 3: redundant presence_v2

`examples/presence_redundant_v2.yaml` показывает, как вычислительная модель отделяется от постановки задачи для доменного факта `presence`.

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
go run ./cmd/runtime -ir ./examples/presence_v2.yaml -debug
```

## Запуск

### Mock pipeline

```bash
go run ./cmd/runtime -ir ./examples/presence.yaml
```

### Real presence_v2

```bash
go run ./cmd/runtime -ir ./examples/presence_v2.yaml -debug
```

### Redundant real presence_v2

```bash
go run ./cmd/runtime -ir ./examples/presence_redundant_v2.yaml -task low_cost_interactive -debug
```

Другой вариант постановки задачи:

```bash
go run ./cmd/runtime -ir ./examples/presence_redundant_v2.yaml -task high_confidence_monitoring -debug
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
go run ./cmd/runtime -ir ./examples/presence_redundant_v2.yaml -task camera_vision_accurate -debug
```

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
- пример реального графа: `examples/presence_v2.yaml`
- пример выбора между избыточными вариантами: `examples/presence_redundant_v2.yaml`
- entrypoint runtime: `cmd/runtime/main.go`