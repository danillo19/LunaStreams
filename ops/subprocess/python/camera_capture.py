#!/usr/bin/env python3
"""camera_capture — внешний source-оператор LunaStreams.

Работает как долгоживущий subprocess-драйвер, описанный в
`docs/operation-interface.md`. Протокол общения с Go runtime — NDJSON:
одна строка на RunRequest, одна строка на RunResult.

Оператор:
  * держит cv2.VideoCapture между запросами (меньше накладных расходов,
    чем переоткрывать устройство на каждый frame);
  * на каждый запрос читает один кадр, кодирует в JPEG и отдаёт его в
    outputs как video_frame с base64-полезной нагрузкой.

Config (из YAML operation.config):
  * device_index: int     — индекс устройства (default 0)
  * jpeg_quality: int     — качество JPEG (default 80)
  * max_width:    int     — если задан, кадр уменьшается до этой ширины

Пример зависимости: `pip install opencv-python`.
"""

from __future__ import annotations

import base64
import json
import sys
import time
from typing import Any

try:
    import cv2  # type: ignore
except ImportError:  # pragma: no cover
    sys.stderr.write("camera_capture: opencv (cv2) is not installed\n")
    sys.exit(1)


def log(message: str) -> None:
    sys.stderr.write(f"camera_capture: {message}\n")
    sys.stderr.flush()


def read_config(request: dict[str, Any]) -> dict[str, Any]:
    return request.get("config") or {}


def open_capture(config: dict[str, Any]):
    device_index = int(config.get("device_index", 0))
    cap = cv2.VideoCapture(device_index)
    if not cap.isOpened():
        raise RuntimeError(f"cannot open camera device {device_index}")
    return cap


def encode_frame(frame, jpeg_quality: int, max_width: int | None) -> dict[str, Any]:
    if max_width and frame.shape[1] > max_width:
        scale = max_width / float(frame.shape[1])
        frame = cv2.resize(frame, (max_width, int(frame.shape[0] * scale)))
    ok, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, int(jpeg_quality)])
    if not ok:
        raise RuntimeError("jpeg encode failed")
    jpeg_bytes = buffer.tobytes()
    return {
        "width": int(frame.shape[1]),
        "height": int(frame.shape[0]),
        "jpeg_base64": base64.b64encode(jpeg_bytes).decode("ascii"),
        "captured_unix_ms": int(time.time() * 1000),
    }


def encode_output(stream_id: str, payload_obj: dict[str, Any]) -> dict[str, Any]:
    raw = json.dumps(payload_obj, ensure_ascii=False).encode("utf-8")
    return {
        "stream_id": stream_id,
        "type": "video_frame",
        "schema": "luna.video_frame.v1",
        "encoding": "json",
        "payload": base64.b64encode(raw).decode("ascii"),
        "meta": {},
    }


def main() -> int:
    cap = None
    config_cache: dict[str, Any] = {}
    seq = 0

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            json.dump({"outputs": {}, "error": f"bad json: {exc}"}, sys.stdout)
            sys.stdout.write("\n")
            sys.stdout.flush()
            continue

        config = read_config(request)
        if cap is None or config != config_cache:
            if cap is not None:
                cap.release()
            try:
                cap = open_capture(config)
                config_cache = config
            except Exception as exc:  # noqa: BLE001
                json.dump({"outputs": {}, "error": str(exc)}, sys.stdout)
                sys.stdout.write("\n")
                sys.stdout.flush()
                continue

        ok, frame = cap.read()
        retries = 0
        while not ok and retries < 5:
            time.sleep(0.02)
            ok, frame = cap.read()
            retries += 1
        if not ok:
            json.dump({"outputs": {}, "error": "camera read failed"}, sys.stdout)
            sys.stdout.write("\n")
            sys.stdout.flush()
            continue

        seq += 1
        payload_obj = encode_frame(
            frame,
            jpeg_quality=int(config.get("jpeg_quality", 80)),
            max_width=int(config.get("max_width")) if config.get("max_width") else None,
        )
        payload_obj["seq"] = seq

        outputs_binding = request.get("outputs") or []
        output_stream = outputs_binding[0]["stream_id"] if outputs_binding else "frame"

        response = {
            "outputs": {output_stream: encode_output(output_stream, payload_obj)},
            "meta": {"seq": str(seq)},
        }
        json.dump(response, sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        sys.stdout.flush()

    if cap is not None:
        cap.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
