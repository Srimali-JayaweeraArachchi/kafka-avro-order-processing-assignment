from __future__ import annotations

import json
import struct
from io import BytesIO
from pathlib import Path
from typing import Any


def load_schema(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as schema_file:
        return json.load(schema_file)


def _write_long(buffer: BytesIO, value: int) -> None:
    encoded = (value << 1) ^ (value >> 63)
    while encoded & ~0x7F:
        buffer.write(bytes([(encoded & 0x7F) | 0x80]))
        encoded >>= 7
    buffer.write(bytes([encoded]))


def _read_long(buffer: BytesIO) -> int:
    shift = 0
    result = 0
    while True:
        raw = buffer.read(1)
        if not raw:
            raise ValueError("unexpected end of Avro payload")
        byte = raw[0]
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return (result >> 1) ^ -(result & 1)


def _write_string(buffer: BytesIO, value: str) -> None:
    data = value.encode("utf-8")
    _write_long(buffer, len(data))
    buffer.write(data)


def _read_string(buffer: BytesIO) -> str:
    length = _read_long(buffer)
    return buffer.read(length).decode("utf-8")


def serialize_avro(record: dict[str, Any], schema: dict[str, Any]) -> bytes:
    buffer = BytesIO()
    for field in schema["fields"]:
        name = field["name"]
        field_type = field["type"]
        value = record[name]
        if field_type == "string":
            _write_string(buffer, str(value))
        elif field_type == "float":
            buffer.write(struct.pack("<f", float(value)))
        elif field_type == "int":
            _write_long(buffer, int(value))
        else:
            raise ValueError(f"unsupported Avro field type: {field_type}")
    return buffer.getvalue()


def deserialize_avro(payload: bytes, schema: dict[str, Any]) -> dict[str, Any]:
    buffer = BytesIO(payload)
    record: dict[str, Any] = {}
    for field in schema["fields"]:
        field_type = field["type"]
        if field_type == "string":
            value = _read_string(buffer)
        elif field_type == "float":
            value = struct.unpack("<f", buffer.read(4))[0]
        elif field_type == "int":
            value = _read_long(buffer)
        else:
            raise ValueError(f"unsupported Avro field type: {field_type}")
        record[field["name"]] = value
    return record

