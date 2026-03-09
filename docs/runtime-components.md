# Runtime Components

Этот документ описывает внутреннее устройство `LunaStreams` на уровне структур данных и основных компонентов runtime.

## 1. Концептуальная модель

Система состоит из трёх слоёв:

- декларативный слой: IR (`operations`, `streams`)
- связующий слой: dependency graph (`producer -> stream -> consumers`)
- исполняющий слой: runtime (`Registry`, `Engine`, `StreamStore`, `EventBus`, operators)

В результате IR превращается в исполняемый граф операций, реагирующий на события streams.

## 2. IR-модель

Пакет: `internal/ir`

### `Document`

`Document` это корневой объект YAML:

- `Operations []Operation`
- `Streams []Stream`

Он представляет всю конфигурацию pipeline.

### `Operation`

`Operation` описывает один узел графа:

- `ID`
- `Kind`
- `Mode`
- `Impl`
- `Inputs`
- `Outputs`
- `Config`

#### Назначение полей

- `ID` нужен для идентификации узла в графе, логах и runtime
- `Kind` определяет роль узла в вычислении
- `Mode` определяет модель запуска
- `Impl` связывает декларативный IR с конкретной Go-реализацией
- `Inputs/Outputs` задают wiring по stream ids
- `Config` передаёт runtime-специфичные параметры в конкретный operator

### `OperationKind`

Поддерживаемые значения:

- `source`
- `transform`
- `selector`
- `sink`

#### Семантика

- `source`: производит данные без входов
- `transform`: преобразует данные из входов в новые выходы
- `selector`: объединяет несколько входов в решение
- `sink`: завершает цепочку и не производит outputs

### `OperationMode`

Поддерживаемые значения:

- `always_on`
- `task_per_event`

#### Семантика

- `always_on`: runtime сам постоянно вызывает `Run()`
- `task_per_event`: операция запускается в ответ на событие на входных streams

### `Stream`

`Stream` описывает именованный канал данных:

- `ID`
- `Type`

В текущем MVP `Type` используется как семантическое описание контракта, а не как жёсткая runtime-типизация.

Поддерживаемые типы:

- `video_frame`
- `audio_chunk`
- `bool`
- `text`

## 3. Валидация IR

Пакет: `internal/ir`

Функция: `Validate(doc, registry)`

Цель валидации: отсеять некорректный граф до старта runtime.

### Какие инварианты проверяются

- каждый `stream.id` уникален
- каждый `operation.id` уникален
- каждый stream из `inputs` существует
- каждый stream из `outputs` существует
- `source` не имеет входов
- `sink` не имеет выходов
- у каждого stream ровно один producer
- каждый `impl` заранее зарегистрирован в `Registry`

### Почему это важно

Без этих ограничений runtime сталкивался бы с неоднозначностями:

- кто пишет в stream
- кого запускать при событии
- существует ли входной stream вообще
- можно ли создать operator для указанного `impl`

## 4. Dependency Graph

Пакет: `internal/graph`

### `Graph`

`Graph` хранит три основных индекса:

- `Operations map[string]ir.Operation`
- `ProducerByStream map[string]string`
- `ConsumersByStream map[string][]string`

### Что даёт graph

Graph не исполняет операции сам по себе, но даёт runtime быструю адресацию:

- по `streamID` узнать producer
- по `streamID` узнать список consumers
- по `operationID` получить спецификацию операции

### Producer/consumer модель

Для каждого stream:

- producer: одна операция, которая пишет его в `outputs`
- consumers: все операции, которые читают его из `inputs`

Это и есть топология вычислительного графа.

## 5. Registry

Пакет: `internal/runtime`

### Назначение

`Registry` связывает строковый `impl` из IR с фабрикой Go-оператора.

### Структуры

- `type Operator interface { Run(ctx, inputs) }`
- `type ClosableOperator interface { Close() error }`
- `type Factory func(spec ir.Operation) (Operator, error)`
- `type Registry struct { factories map[string]Factory }`

### Жизненный цикл

1. На старте runtime регистрирует все доступные `impl`
2. После загрузки IR `Engine` вызывает `Registry.Create(spec)` для каждой операции
3. Если impl не найден, создание engine завершается ошибкой

### Почему registry важен

IR остаётся декларативным и независимым от конкретных Go-типов.

Например:

- в YAML указан `impl: microphone.capture`
- в registry это имя связано с фабрикой `newRealtimeMicrophoneSource`

## 6. StreamStore

Пакет: `internal/runtime`

### Назначение

`StreamStore` хранит последнее известное значение каждого stream.

### Структура

- `values map[string]StreamValue`
- `StreamValue { Value any, Seq uint64 }`

### Поведение

- `Set(streamID, value)`:
  - обновляет последнее значение
  - увеличивает `seq`
  - возвращает новый `seq`
- `Get(streamID)`:
  - возвращает последнее значение, его `seq` и признак наличия

### Почему store нужен

Runtime отделяет:

- факт появления нового события
- текущее состояние streams

Dispatcher слушает события, но сами данные читает из `StreamStore`.

### Важное ограничение

Store хранит только последнее значение. История значений в MVP отсутствует.

## 7. EventBus

Пакет: `internal/runtime`

### Назначение

`EventBus` это общий buffered channel для сигналов об изменении streams.

### Структуры

- `Event { StreamID string, Seq uint64 }`
- `EventBus { ch chan Event }`

### Семантика

Событие сообщает:

- какой stream обновился
- какая это версия (`seq`)

Само значение в событии не передаётся: оно уже лежит в `StreamStore`.

### Почему bus отделён от store

Это упрощает архитектуру:

- `Store` отвечает за состояние
- `Bus` отвечает за уведомление

## 8. Engine

Пакет: `internal/runtime`

`Engine` это центральный координатор runtime.

### Состав `Engine`

- `doc`: исходный IR
- `graph`: dependency graph
- `registry`: фабрика реализаций
- `store`: последнее состояние streams
- `bus`: шина событий
- `logger`: логирование
- `operators`: созданные экземпляры операторов
- `semaphores`: per-op ограничители параллелизма

### Что делает `NewEngine`

При создании engine:

- проверяет входные зависимости
- создаёт `StreamStore`
- создаёт `EventBus`
- создаёт все операторы через `Registry`
- заводит per-op semaphore с `capacity=1`

### Что делает `Start`

После старта engine:

- запускает dispatcher
- запускает graceful shutdown watcher
- запускает все `always_on source`
- запускает все `always_on selector`

## 9. Dispatcher

### Общая идея

Dispatcher слушает `EventBus` и запускает `task_per_event` операции.

### Алгоритм

1. приходит `Event{StreamID, Seq}`
2. engine получает `consumers := graph.ConsumersByStream[event.StreamID]`
3. для каждого consumer проверяет `mode == task_per_event`
4. запускает `runTaskOp(ctx, op)` в отдельной goroutine

### Почему не selector

`always_on selector` работает отдельно через ticker и не запускается dispatcher-ом.

## 10. Per-op semaphore

### Задача

Защитить одну и ту же `task_per_event` операцию от повторного параллельного запуска.

### Реализация

Для каждой операции создаётся:

- `semaphores[op.ID] = make(chan struct{}, 1)`

При запуске:

- если семафор свободен, операция стартует
- если уже занят, повторный запуск пропускается

### Следствие

Если поток событий очень быстрый, часть запусков может быть пропущена, но операция всегда будет работать на актуальном состоянии `StreamStore`.

Это сознательная упрощённая модель MVP.

## 11. Исполнение `source`

`always_on source` запускается в отдельной горутине.

Цикл:

1. `Run(ctx, nil)`
2. получить `outputs`
3. сохранить outputs в `StreamStore`
4. опубликовать события в `EventBus`
5. повторить

### Примеры

- mock `webcam.read`
- mock `microphone.read`
- real `microphone.capture`
- real `keyboard.read`

## 12. Исполнение `task_per_event`

При событии на входном stream:

1. runtime читает все входы операции из `StreamStore`
2. вызывает `operator.Run(ctx, inputs)`
3. пишет outputs в `StreamStore`
4. публикует события на выходных streams

### Особенность

Даже если операция была вызвана событием на одном входе, ей передаются значения всех её входных streams.

Это делает операции похожими на "read latest state and compute".

## 13. Исполнение `selector`

`always_on selector` запускается отдельно и живёт на `ticker`.

### Алгоритм

1. каждые `tick_ms` selector читает все свои входы из `StreamStore`
2. если значения нет, подставляется `false`
3. selector получает `inputs map[string]any`
4. selector возвращает `outputs`
5. outputs записываются в `StreamStore` и публикуются в `EventBus`

### Почему selector отдельный

Selector нужен для периодического принятия решения на основе нескольких streams, а не только на основе одного входного события.

## 14. Graceful shutdown

Когда `ctx.Done()` закрывается:

- `Engine` обходит все созданные operators
- если operator реализует `ClosableOperator`, вызывается `Close()`

Это важно для реальных источников:

- `microphone.capture` закрывает audio device и malgo context
- `keyboard.read` завершает свой loop

## 15. Logger

Пакет: `internal/runtime`

`BeautifulLogger` нужен для трассировки выполнения.

### Уровни

- `INFO`: жизненный цикл engine и operators
- `DEBUG`: детальная трасса выполнения
- `ERROR`: ошибки
- `RESULT`: финальные значения в sink

### Что логируется

- старт runtime и операций
- события bus
- inputs/outputs операторов
- запись значений в store
- публикация событий
- итоговые значения sink

## 16. Реальные типы данных

Хотя streams типизированы логически, реальные значения на Go-уровне зависят от impl.

### `AudioChunk`

Используется в `microphone.capture` и `audio.volume_presence`.

Содержит:

- `Seq`
- `Samples []int16`
- `RMS float64`

### `KeyEvent`

Используется в `keyboard.read` и selector, который учитывает клавиатурную активность.

Содержит:

- `Seq`
- `Key`
- `At`

## 17. Реальные реализации в `presence_v2`

### `microphone.capture`

Реальный source:

- создаёт backend через `malgo`
- читает аудио буферы
- декодирует `[]byte -> []int16`
- считает RMS
- публикует `AudioChunk`

### `audio.volume_presence`

Transform:

- получает `AudioChunk`
- сравнивает `RMS` с `threshold`
- выдаёт `bool`

### `keyboard.read`

Source:

- читает `os.Stdin`
- создаёт `KeyEvent`
- публикует `pressed_key`

### `selector.audio_or_recent_key`

Selector:

- читает `audio_presence`
- читает последнее `pressed_key`
- считает клавиатуру активной, если событие пришло недавно
- выдаёт `presence_decision = audio_presence || recent_key`

## 18. Ментальная модель вычисления

Удобно думать о runtime так:

- `IR` описывает "что связано с чем"
- `Graph` описывает "кто у кого producer/consumer"
- `Store` хранит "последнее известное состояние"
- `Bus` сообщает "что обновилось"
- `Engine` решает "кого запускать и когда"
- `Operator` содержит прикладную логику

## 19. Ограничения архитектуры

Текущий MVP намеренно простой:

- без очередей задач на операцию
- без истории stream values
- без replay событий
- без подтверждений доставки
- без typed contracts между streams на уровне компилятора
- без отдельного scheduler-а

Но при этом уже есть:

- валидируемый IR
- граф зависимостей
- реактивная модель исполнения
- реальный ввод с устройств
- расширяемая система impl через registry

## 20. Куда расширять дальше

Наиболее естественные направления развития:

- typed schemas для streams
- очереди вместо skip при `max_in_flight=1`
- raw-mode keyboard input
- windowed/aggregated streams
- richer selector API
- metrics/tracing
- тестовые harness-ы для операторов и IR графов

