# План перехода к распределённой системе с NFR-профилями

## Цель

Перейти от текущего локального MVP к системе, которая:
- умеет работать и на одной машине, и в распределённом режиме
- поддерживает разные режимы исполнения под разные нефункциональные требования
- не требует полного переписывания существующих `operations` и example-графов на первом этапе

Практическая цель:
- сохранить текущую функциональную модель графа
- отделить её от способа исполнения
- добавить execution profiles: `realtime`, `interactive`, `durable`, `batch`

## Что есть сейчас

Сейчас система завязана на single-process модель:
- `Engine` создаёт store и bus внутри одного процесса
- `StreamStore` хранит состояние в локальной map
- `EventBus` это локальный `chan Event`
- impl-операторы создаются и исполняются локально
- реальные операции работают напрямую с локальным устройством и локальным временем
- IR описывает топологию, но почти не описывает execution policy

Главные ограничения текущего MVP:
- данные между операциями передаются как `map[string]any`
- значения streams не сериализуются
- `seq` локален для процесса
- `task_per_event` ближе к модели `read latest state`, а не `process every event`
- selectors зависят от локального `ticker` и `time.Now()`

## Какая должна быть целевая архитектура

Целевая система должна состоять из следующих логических слоёв:
- `IR + Policies`: описывает topology и execution profile
- `Planner / Control Plane`: валидирует IR, строит graph и назначает операции на workers
- `Workers / Data Plane`: исполняют только назначенные операции
- `State Backend`: хранит stream state, `seq`, offsets, leases и checkpoints
- `Event Broker`: доставляет stream events между узлами
- `QoS / Scheduler`: выбирает поведение системы под разные NFR

## Какие режимы исполнения нужно поддержать

Минимальный набор execution profiles:
- `realtime`: минимальная задержка, допускаются потери и coalescing
- `interactive`: низкая задержка, но мягче требования к bounded latency
- `durable`: важнее сохранность и replay, чем latency
- `batch`: throughput важнее времени реакции

Чтобы это работало, IR должен уметь выражать не только topology, но и execution policy.

Минимум, что нужно добавить в модель:
- `execution_profile`
- `latency_budget_ms`
- `delivery_semantics`
- `ordering`
- `durability`
- `queue_policy`
- `degradation_policy`
- `replicas`
- `placement`
- `stateful`
- `partition_by`

## Поэтапный план перехода

### Этап 1. Отделить runtime от in-memory реализации

Цель:
- не менять текущее поведение
- убрать жёсткую связь `Engine` с локальными реализациями

Изменения:
- ввести интерфейсы:
  - `StateBackend`
  - `EventTransport`
  - `Codec`
  - `Clock`
  - `SchedulerPolicy`
- переписать `Engine`, чтобы он принимал эти зависимости извне
- сохранить `StreamStore` и `EventBus` как `inmemory` реализации

Какие файлы меняются:
- `internal/runtime/engine.go`
- `internal/runtime/store.go`
- `internal/runtime/bus.go`
- `cmd/runtime/main.go`

Что получаем:
- текущий MVP продолжает работать
- появляется фундамент для distributed и разных execution profiles

### Этап 2. Убрать зависимость runtime от `map[string]any`

Цель:
- подготовить систему к сети, persistence и replay

Изменения:
- ввести сериализуемый envelope для stream values
- добавить codecs для `audio_chunk`, `bool`, `text` и других типов
- отделить внутренние Go-структуры impl’ов от wire-level representation

Какие файлы меняются:
- `internal/runtime/engine.go`
- `ops/inprocgo/mock.go`
- `ops/inprocgo/realtime.go`
- `internal/ir/model.go`

Что получаем:
- один и тот же stream можно безопасно передавать между процессами
- появляется база под broker и shared state

### Этап 3. Расширить IR для distributed и NFR

Цель:
- чтобы граф описывал не только что считать, но и как это исполнять

Что добавить в `operation`:
- `execution_profile`
- `placement`
- `replicas`
- `max_in_flight`
- `retry_policy`
- `stateful`
- `priority`

Что добавить в `stream`:
- `transport`
- `schema`
- `partition_by`
- `delivery_semantics`
- `ordering`
- `retention`
- `durability`

Что добавить на runtime-level:
- backend defaults
- scheduler defaults
- degradation policies

Какие файлы меняются:
- `internal/ir/model.go`
- `internal/ir/validate.go`
- `examples/presence_v2.yaml`

Что получаем:
- IR начинает выражать execution intent
- появляется возможность запускать один и тот же graph в разных режимах

### Этап 4. Ввести execution profiles

Цель:
- сделать управление нефункциональными требованиями частью системы, а не набором ad-hoc флагов

Нужно определить профили:
- `realtime`
- `interactive`
- `durable`
- `batch`

Каждый профиль должен разворачиваться в настройки:
- transport
- state backend
- queue policy
- retry policy
- ordering
- durability
- latency budget
- degradation behavior

Пример:
- `realtime`: минимальная latency, допускается coalescing и `drop_old`
- `interactive`: низкая latency, но мягче требования
- `durable`: no drop, replay, retries, storage-first semantics
- `batch`: буферизация, grouping, высокая пропускная способность

Какие файлы/зоны меняются:
- новый пакет `internal/runtime/profile/...`
- `internal/runtime/engine.go`
- `cmd/runtime/main.go`

Что получаем:
- система начинает адаптироваться к разным NFR без переписывания графов

### Этап 5. Вынести transport в broker-backed реализацию

Цель:
- заменить локальный `chan Event` на межузловую доставку

Изменения:
- реализовать `EventTransport` поверх брокера
- сохранить inmemory transport как локальный профиль
- привязать streams к topic/subject/partition

Практически лучше начинать с:
- простого pub/sub transport
- `at-least-once`
- idempotent consumers

Какие файлы/пакеты меняются:
- новый пакет `internal/runtime/transport/...`
- `internal/runtime/bus.go` как reference
- `internal/runtime/engine.go`

Что получаем:
- события начинают жить вне одного процесса

### Этап 6. Вынести stream state в shared backend

Цель:
- убрать локальный state как единственный источник истины

Изменения:
- реализовать `StateBackend` поверх shared storage
- определить, кто выдаёт `seq`
- формализовать atomicity между `Set` и `Publish`

Это ключевой архитектурный выбор. Нужно выбрать одну модель:
- outbox pattern
- store-first + publish
- log-first + state projection

Какие файлы/пакеты меняются:
- новый пакет `internal/runtime/state/...`
- `internal/runtime/store.go`
- `internal/runtime/engine.go`

Что получаем:
- workers могут читать общий latest state
- selectors и transforms перестают зависеть от одной памяти процесса

### Этап 7. Разделить runtime на planner и workers

Цель:
- перестать запускать весь graph в одном процессе

Новая модель:
- `planner`:
  - загружает IR
  - валидирует
  - строит graph
  - разворачивает execution profiles
  - назначает операции на workers
- `worker`:
  - поднимает только назначенные impl’ы
  - подписывается только на нужные streams
  - пишет outputs в shared state и broker

Какие файлы меняются:
- `cmd/runtime/main.go`
- `internal/graph/build.go`
- `internal/runtime/registry.go`

Что получаем:
- появляется реальное распределённое исполнение

### Этап 8. Добавить placement, ownership и leases

Цель:
- гарантировать корректное исполнение singleton operations и side effects

Нужно добавить:
- leases для hardware-bound sources
- ownership stream producer
- health checks workers
- reassignment
- singleton placement rules

Особенно важно для:
- `ops/inprocgo/realtime.go`

Что получаем:
- правило “один producer на stream” начинает соблюдаться и в runtime, а не только в static validation

### Этап 9. Ввести QoS и degradation policies

Цель:
- чтобы одна система могла по-разному вести себя под разные условия нагрузки

Нужно добавить:
- admission control
- backpressure
- queue policies
- coalescing policies
- drop strategies
- sampling strategies
- fallback from realtime to interactive/durable mode

Примеры:
- realtime: drop old frames, bounded queues
- durable: ничего не дропать, но разрешать lag
- interactive: коалесить latest state

Какие зоны меняются:
- новый scheduler/QoS слой
- `internal/runtime/engine.go`
- расширенный IR

Что получаем:
- одна платформа под разные NFR

### Этап 10. Переработать время и selectors

Цель:
- убрать скрытую зависимость от локального времени

Изменения:
- ввести abstraction for time
- различать `processing time` и `event time`
- в metadata передавать timestamps
- пересмотреть time-based selector logic

Какие файлы меняются:
- `internal/runtime/engine.go`
- `ops/inprocgo/realtime.go`

Что получаем:
- selectors становятся корректнее в distributed execution

### Этап 11. Добавить observability и production controls

Цель:
- сделать систему управляемой и дебажимой

Нужно добавить:
- structured logs
- metrics
- traces
- lag metrics
- queue metrics
- deadline miss metrics
- health endpoints
- reconciliation and restart logic

Что получаем:
- можно проверять, выполняется ли профиль `realtime`
- можно видеть, когда система фактически деградировала в другой режим

## Какие изменения нужны в текущей системе по модулям

### Изменения в `internal/runtime`

- выделить абстракции backend’ов
- убрать прямую зависимость от `chan Event` и локальной map
- сделать `Engine` orchestration-only слоем
- вынести scheduler/QoS/time logic в отдельные компоненты

### Изменения в `internal/ir`

- расширить модель operation/stream/runtime policy
- усилить validation distributed-инвариантов

### Изменения в `internal/graph`

- сохранить текущий graph
- добавить layer для placement/partitioning/ownership

### Изменения в `ops/`

- отделить business logic от wire format
- запретить неявную зависимость impl’ов от локальной памяти процесса
- для hardware ops ввести singleton semantics

### Изменения в `cmd/runtime`

- текущий entrypoint разделить на planner-mode и worker-mode

## Порядок внедрения без большого риска

### Фаза 1. Стабилизировать архитектурные швы

- интерфейсы backend’ов
- codecs
- time abstraction
- tests на эквивалентность current semantics

### Фаза 2. Добавить policy layer

- execution profiles
- IR extensions
- degradation policies

### Фаза 3. Добавить distributed infrastructure

- broker
- shared state
- seq/ordering model

### Фаза 4. Разделить процессы

- planner
- workers
- placement
- leases

### Фаза 5. Довести до production-quality

- QoS
- observability
- failover
- replay
- recovery

## Самый правильный первый шаг

Если начинать с одного конкретного изменения, то первым шагом должен быть такой:
- переписать `internal/runtime/engine.go` на работу через `StateBackend`, `EventTransport`, `Codec`, `Clock`
- не менять поведение `presence.yaml` и `presence_v2.yaml`
- оставить `inmemory` режим как baseline

Это даст безопасную стартовую точку. После этого уже можно добавлять broker, shared state и execution profiles без разрушения текущего MVP.
