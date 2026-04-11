#!/usr/bin/env python3
"""face_presence — transform-оператор: кадр → bool (есть ли лицо).

Работает как долгоживущий subprocess-оператор LunaStreams. Контракт
общения описан в `docs/operation-interface.md`: один RunRequest в stdin,
один RunResult в stdout.

Логика:
  1. читает InputValue с type=video_frame (payload — JSON, содержащий
     поле jpeg_base64);
  2. декодирует JPEG через OpenCV;
  3. запускает Haar-каскад детекции лица;
  4. возвращает bool (есть ли лицо) в первом output-stream.

Config:
  * cascade_path     — путь к XML каскада (default: frontalface);
  * scale_factor     — cv2 scaleFactor (default 1.2);
  * min_neighbors    — cv2 minNeighbors (default 5);
  * min_face_px      — минимальный размер лица в пикселях (default 60).

Зависимости: `pip install opencv-python numpy`.
"""

from __future__ import annotations

import base64
import json
import sys
from typing import Any

try:
    import cv2  # type: ignore
    import numpy as np  # type: ignore
except ImportError:  # pragma: no cover
    sys.stderr.write("face_presence: opencv/numpy are not installed\n")
    sys.exit(1)


def log(message: str) -> None:
    sys.stderr.write(f"face_presence: {message}\n")
    sys.stderr.flush()


def load_cascade(config: dict[str, Any]) -> cv2.CascadeClassifier:
    cascade_path = config.get("cascade_path") or (
        cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    )
    cascade = cv2.CascadeClassifier(cascade_path)
    if cascade.empty():
        raise RuntimeError(f"cannot load cascade {cascade_path!r}")
    return cascade


def decode_frame(input_value: dict[str, Any]):
    payload_b64 = input_value.get("payload")
    if not payload_b64:
        return None
    raw = base64.b64decode(payload_b64)
    try:
        frame_meta = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return None
    jpeg_b64 = frame_meta.get("jpeg_base64")
    if not jpeg_b64:
        return None
    jpeg_bytes = base64.b64decode(jpeg_b64)
    array = np.frombuffer(jpeg_bytes, dtype=np.uint8)
    return cv2.imdecode(array, cv2.IMREAD_COLOR)


def encode_bool_output(stream_id: str, value: bool) -> dict[str, Any]:
    raw = json.dumps(bool(value)).encode("utf-8")
    return {
        "stream_id": stream_id,
        "type": "bool",
        "schema": "luna.bool.v1",
        "encoding": "json",
        "payload": base64.b64encode(raw).decode("ascii"),
        "meta": {},
    }


def find_frame_input(request: dict[str, Any]) -> dict[str, Any] | None:
    inputs = request.get("inputs") or {}
    # prefer explicitly typed video_frame inputs
    for value in inputs.values():
        if value.get("type") == "video_frame":
            return value
    # fall back to the first input if there is only one
    if len(inputs) == 1:
        return next(iter(inputs.values()))
    return None


def main() -> int:
    cascade: cv2.CascadeClassifier | None = None
    cached_config: dict[str, Any] = {}

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

        config = request.get("config") or {}
        if cascade is None or config != cached_config:
            try:
                cascade = load_cascade(config)
                cached_config = config
            except Exception as exc:  # noqa: BLE001
                json.dump({"outputs": {}, "error": str(exc)}, sys.stdout)
                sys.stdout.write("\n")
                sys.stdout.flush()
                continue

        outputs_binding = request.get("outputs") or []
        output_stream = outputs_binding[0]["stream_id"] if outputs_binding else "face_presence"

        frame_input = find_frame_input(request)
        present = False
        if frame_input and frame_input.get("present"):
            frame = decode_frame(frame_input)
            if frame is not None:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                min_face_px = int(config.get("min_face_px", 60))
                faces = cascade.detectMultiScale(
                    gray,
                    scaleFactor=float(config.get("scale_factor", 1.2)),
                    minNeighbors=int(config.get("min_neighbors", 5)),
                    minSize=(min_face_px, min_face_px),
                )
                present = len(faces) > 0

        response = {
            "outputs": {output_stream: encode_bool_output(output_stream, present)},
            "meta": {},
        }
        json.dump(response, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
