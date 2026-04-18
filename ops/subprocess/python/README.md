# Python polyglot operations for LunaStreams

Эта директория содержит операции, реализованные на **Python**. Они
запускаются Go runtime через subprocess-драйвер (`ops/subprocess`) по
контракту из [`docs/operation-interface.md`](../../../docs/operation-interface.md):
один RunRequest в stdin — один RunResult в stdout, все payload-ы идут в
base64+JSON.

Цель — показать, что любую операцию LunaStreams можно подменить
внешней реализацией: Python-скриптом, Docker-контейнером, бинарём на
Rust или удалённым HTTP/gRPC-сервисом. При этом IR (`operations`,
`streams`, `task`) остаётся неизменным, меняется только секция
`runtime` в YAML.

## Что здесь лежит

| Файл                 | Impl                | Описание                                              |
|----------------------|---------------------|-------------------------------------------------------|
| `camera_capture.py`  | `camera.capture`    | Source: читает кадр с камеры, кодирует в JPEG         |
| `face_presence.py`   | `cv.face_presence`  | Transform: JPEG → bool (есть ли лицо, Haar cascade)   |

Обе операции используются в `examples/presence/model.yaml` вместе
с Go-операциями `microphone.capture`, `audio.volume_presence` и
`keyboard.read`.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r ops/subprocess/python/requirements.txt
```

## Запуск

```bash
go run ./cmd/runtime -ir ./examples/presence -task camera_vision_accurate
```

## Как добавить свою реализацию

1. Реализуйте скрипт/бинарь, который читает NDJSON из stdin и пишет
   NDJSON в stdout. Минимальный пример — в `face_presence.py`.
2. Опишите операцию в IR:

   ```yaml
   - id: my_op
     kind: transform
     mode: task_per_event
     impl: my.operation
     runtime:
       kind: subprocess
       command: [python3, ops/subprocess/python/my_op.py]
     inputs:
       - stream: frame
     outputs:
       - stream: my_result
   ```

3. (Опционально) для Docker — используйте `runtime.kind: docker` и поле
   `runtime.image`. Для внешних сервисов — `runtime.kind: grpc` или
   `http` с `runtime.endpoint`. Новые драйверы регистрируются через
   `registry.RegisterDriver(kind, driver)`.

## Протокол одним взглядом

Запрос (из Go в Python):

```json
{
  "operation_id": "face_presence",
  "kind": "transform",
  "mode": "task_per_event",
  "config": {"min_neighbors": 5},
  "inputs": {
    "frame": {
      "stream_id": "frame",
      "type": "video_frame",
      "encoding": "json",
      "present": true,
      "payload": "<base64 of json with jpeg_base64>"
    }
  },
  "outputs": [
    {"stream_id": "face_presence", "type": "bool", "encoding": "json"}
  ],
  "meta": {"attempt": 7, "timestamp_unix_ms": 1741500000123}
}
```

Ответ (из Python в Go):

```json
{
  "outputs": {
    "face_presence": {
      "stream_id": "face_presence",
      "type": "bool",
      "encoding": "json",
      "payload": "dHJ1ZQ=="
    }
  }
}
```
