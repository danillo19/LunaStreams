# План поддержки альтернативных путей исполнения при отказе операций

## Цель

Нужно добавить в систему функциональность, при которой при отказе отдельной операции runtime не просто логирует ошибку, а может:
- переключиться на альтернативную операцию
- перестроить путь исполнения
- продолжить вычисление результата с пониженным качеством или в другом режиме
- явно фиксировать, что система работает в degraded mode

Идея в том, чтобы граф был не только статической цепочкой `producer -> stream -> consumer`, но и содержал допустимые резервные ветки.

## Что есть сейчас

В текущем MVP:
- операция либо успешно выполняется, либо возвращает ошибку
- `Engine` логирует ошибку и не получает никакого альтернативного результата
- граф исполнения жёстко задан через `inputs` и `outputs`
- у stream ровно один producer
- fallback path отсутствует как понятие

Это значит, что при отказе операции:
- часть графа перестаёт обновляться
- downstream operations начинают видеть старые значения или отсутствие значений
- runtime не принимает решение о перестройке исполнения

## Какой должна быть целевая модель

Система должна поддерживать:
- primary path
- fallback path
- degraded path
- recovery back to primary path

Пример:
- основная операция: `cv.face_presence`
- fallback операция: `audio.voice_presence`
- если `cv.face_presence` недоступна, selector или planner переключает presence decision на альтернативный сигнал

## Какие новые понятия нужно ввести

### 1. Health state операции

У каждой операции должно появиться runtime-состояние:
- `healthy`
- `degraded`
- `unavailable`
- `recovering`

Это состояние не должно храниться только в логах. Оно должно быть доступно runtime и selector-логике.

### 2. Политика отказа

У операции или у группы операций должна появиться политика поведения при ошибке:
- `fail_closed`
- `fail_open`
- `use_last_value`
- `switch_to_fallback`
- `skip_branch`
- `emit_default`

### 3. Альтернативные ветки графа

Нужно уметь выразить в конфигурации, какие операции или streams являются резервными.

### 4. Режим деградации

Нужно явно задавать, как система должна ухудшать качество результата:
- снижать точность
- уменьшать частоту обновления
- отключать дорогие операции
- переходить на менее надёжный, но доступный путь

## Что нужно изменить в IR

Текущий IR описывает только прямую топологию. Для failover нужно добавить дополнительные поля.

### Для `operation`

Минимально полезные поля:
- `failure_policy`
- `fallback_operations`
- `health_timeout_ms`
- `max_failures`
- `recovery_policy`
- `degraded_outputs`

Пример:

```yaml
operations:
  - id: face_presence
    kind: transform
    mode: task_per_event
    impl: cv.face_presence
    inputs:
      - stream: video_frame
    outputs:
      - stream: face_presence
    failure_policy:
      mode: switch_to_fallback
      fallback_operations:
        - voice_presence
      health_timeout_ms: 1000
      max_failures: 3
      recovery_policy:
        mode: retry_and_restore
        stable_window_ms: 5000
```

### Для `selector`

Selector должен уметь учитывать доступность веток:
- приоритет healthy источников
- переключение на fallback streams
- degraded decision mode

### Для runtime-level policy

Полезно добавить:
- default failure policy
- global degraded mode policy
- fallback preference rules

## Что нужно изменить в runtime

## Этап 1. Добавить health monitoring операций

Цель:
- runtime должен понимать, что операция не просто “ошиблась один раз”, а находится в нездоровом состоянии

Что добавить:
- health tracker для каждой операции
- счётчик подряд идущих ошибок
- timestamp последнего успешного запуска
- timestamp последней ошибки
- текущее runtime state: `healthy/degraded/unavailable/recovering`

Какие файлы изменятся:
- `internal/runtime/engine.go`
- новый пакет `internal/runtime/health/...`

Что получаем:
- runtime начинает видеть состояние операций, а не только отдельные ошибки

## Этап 2. Ввести явную модель fallback decision

Цель:
- определить, кто принимает решение о переключении пути

Есть два варианта:

### Вариант A. Runtime-driven failover

`Engine` сам решает:
- primary operation не работает
- надо активировать fallback operation

Плюсы:
- меньше логики в IR selectors
- централизованный control

Минусы:
- больше сложности в `Engine`

### Вариант B. Selector-driven failover

Runtime публикует health streams, а selectors уже решают:
- какой сигнал использовать
- какую ветку считать доступной

Плюсы:
- логика гибче
- лучше укладывается в графовую модель

Минусы:
- сложнее IR

Рекомендуемый путь:
- short term: runtime-driven failover для простых случаев
- long term: health streams + selector-driven failover

## Этап 3. Добавить health streams

Цель:
- сделать доступность операций частью графа

Пример новых streams:
- `face_presence_health`
- `voice_presence_health`
- `camera_available`
- `microphone_available`

Пример значений:
- `true/false`
- или enum-like status через отдельную схему

Зачем это нужно:
- selectors смогут принимать решение не только по данным, но и по состоянию источников
- деградация станет частью declarative graph

## Этап 4. Разрешить альтернативные producers логически

Сейчас у stream должен быть ровно один producer. Для failover в таком виде это ограничение слишком жёсткое.

Нужно перейти к одной из моделей:

### Модель 1. Отдельные streams + selector merge

Каждая ветка пишет в свой stream:
- `face_presence`
- `voice_presence`
- `motion_presence`

А selector уже выбирает финальный `presence_decision`.

Это лучший вариант для текущей архитектуры, потому что:
- не ломает правило “один producer на stream”
- хорошо ложится на текущую модель graph + selector
- проще валидировать

### Модель 2. Несколько producers на один logical stream

Это сложнее и потребует полной переработки validation и merge semantics.

Рекомендуемый путь:
- использовать модель 1

## Этап 5. Добавить failure-aware selectors

Нужно расширить selectors так, чтобы они учитывали:
- health stream
- freshness stream data
- fallback priority
- degraded mode

Пример логики:
- если `face_presence` healthy и свежий, использовать его
- иначе взять `voice_presence`
- иначе взять `motion_presence`
- иначе вернуть default false + пометить degraded mode

Это можно оформить как новый selector impl, например:
- `selector.failover_priority`
- `selector.health_aware_merge`

## Этап 6. Ввести freshness / staleness policy

При отказе компонента важно понимать не только доступность операции, но и свежесть её результата.

Нужно добавить:
- TTL для stream values
- stale detection
- policy использования старого значения

Примеры:
- `use_last_value` не дольше 500ms
- после этого считать ветку unavailable

Без этого runtime может бесконечно опираться на давно устаревшие outputs.

## Этап 7. Ввести degraded mode на уровне runtime

Runtime должен уметь работать в нескольких состояниях:
- `normal`
- `degraded`
- `critical`

Примеры:
- если отказала камера, но работает микрофон, это `degraded`
- если отказали все источники presence, это `critical`

Это состояние должно:
- логироваться
- экспортироваться в metrics
- при необходимости публиковаться как stream

## Этап 8. Добавить recovery semantics

После восстановления primary path система должна уметь:
- распознать, что операция снова здорова
- не переключаться обратно слишком часто
- вернуться на primary path только после стабильного окна

Нужно добавить:
- hysteresis
- stable window
- cooldown

Иначе получится flapping:
- primary отказала
- fallback включился
- primary на секунду ожила
- runtime переключился обратно
- снова отказ

## Этап 9. Добавить observability и аудит failover

Нужно логировать и измерять:
- сколько раз была активирована fallback-ветка
- какая операция считается unhealthy
- сколько времени система была в degraded mode
- какой fallback path использовался
- были ли ложные переключения

Минимум:
- structured logs
- per-operation health metrics
- degraded mode metric
- fallback activation counters

## Этап 10. Расширить validation

IR validation должен уметь проверять:
- что fallback operation существует
- что fallback outputs совместимы с selector logic
- что нет циклов в fallback graph
- что fallback path действительно может дать нужный результат

Иначе можно получить формально валидный, но неработающий резервный путь.

## Рекомендуемый путь внедрения

### Фаза 1. Минимальный failover без ломки архитектуры

Что делать:
- не менять правило “один producer на stream”
- использовать отдельные streams на каждую ветку
- добавить health tracking в runtime
- добавить failure-aware selector

Что получится:
- если одна операция падает, selector выбирает другую ветку
- изменения локальны и хорошо ложатся на текущий MVP

Это лучший стартовый вариант.

### Фаза 2. Вынести health в streams

Что делать:
- публиковать health state как streams
- научить selectors принимать решение по данным и health одновременно

Что получится:
- failover станет частью графа, а не только частью `Engine`

### Фаза 3. Добавить policy-driven failover

Что делать:
- расширить IR `failure_policy`
- добавить stale/freshness policy
- добавить degraded mode
- добавить recovery semantics

Что получится:
- поведение при отказах станет декларативным

### Фаза 4. Подготовить это к distributed execution

Что делать:
- хранить health state в shared backend
- распространять failover state между workers
- учитывать placement, leases и ownership

Что получится:
- failover начнёт работать не только внутри одного процесса, но и в распределённой системе

## Самый практичный первый инкремент

Если делать самый безопасный и полезный первый шаг в текущей системе, то он должен быть таким:

1. Добавить runtime health tracking для операций.
2. Не менять текущую базовую модель streams.
3. Для альтернативных путей использовать отдельные streams.
4. Добавить новый selector impl, который выбирает лучший доступный источник.
5. Добавить freshness timeout и fallback priority.

Это даст рабочий failover path уже в текущем runtime без полной переработки архитектуры.

## Короткий итог

Чтобы поддержать альтернативный путь исполнения при отказе операций, системе нужно:
- знать health state операций
- уметь выражать failure policy
- уметь различать primary и fallback ветки
- учитывать stale data
- уметь входить в degraded mode и выходить из него
- валидировать fallback graph

Самая совместимая с текущим MVP стратегия:
- не делать много producers на один stream
- строить альтернативные ветки через отдельные streams
- сводить их через failure-aware selector
