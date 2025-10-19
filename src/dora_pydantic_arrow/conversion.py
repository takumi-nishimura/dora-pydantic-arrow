"""Conversion helpers between Python objects/Pydantic models and Apache Arrow."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Callable, TypeVar, get_args, get_origin
from uuid import UUID

import pyarrow as pa
from pydantic import BaseModel
from pydantic.version import VERSION as PYDANTIC_VERSION

from .config import DAConfig
from .exceptions import SchemaMismatchError, UnsupportedTypeError
from .schema import schema_from_model

UUID_METADATA_KEY = b"uuid_columns"
UUID_VERSION_KEY = b"uuid.version"
UUID_ENCODING_KEY = b"uuid.encoding"
UUID_UTILS_KEY = b"uuid_utils"
UUID_ENCODING_VALUE = b"binary16"
UUID_UTILS_VALUE = b"true"

T = TypeVar("T")


def to_arrow(
    obj: Any,
    *,
    schema: pa.Schema | None = None,
    config: DAConfig | None = None,
) -> pa.Table | pa.RecordBatch:
    """Convert Python objects or Pydantic models into Arrow data structures."""

    rows, model_type = _normalise_input(obj)
    active_config = config or DAConfig()

    if not rows:
        if schema is None:
            raise ValueError("Unable to infer schema from empty input; provide a schema")
        table = pa.Table.from_pylist([], schema=schema)
        return _apply_metadata(table, model_type, active_config)

    if schema is None and model_type is not None:
        schema = schema_from_model(model_type, config=active_config)

    rows = _normalise_datetime_values(rows, active_config)
    rows, extra_metadata = _encode_special_types(rows)

    table = pa.Table.from_pylist(rows, schema=schema)
    table = _apply_metadata(table, model_type, active_config)

    if extra_metadata:
        metadata = dict(table.schema.metadata or {})
        metadata.update(extra_metadata)
        table = table.replace_schema_metadata(metadata)

    if _is_single_object(obj):
        batches = table.to_batches()
        return batches[0] if batches else pa.RecordBatch.from_arrays([], schema=table.schema)
    return table


def from_arrow(
    data: pa.Array | pa.RecordBatch | pa.Table,
    *,
    type_hint: type[T] | None = None,
    validate: bool = True,
    config: DAConfig | None = None,
) -> T:
    """Convert Arrow data back into Python/Pydantic objects."""

    table = _ensure_table(data)
    active_config = config or DAConfig()
    rows = table.to_pylist()
    rows = _decode_special_types(rows, table.schema.metadata)

    datetime_policy = _metadata_datetime_policy(table.schema.metadata, active_config)
    rows = _restore_datetime_values(rows, datetime_policy)

    if type_hint is None:
        return rows  # type: ignore[return-value]

    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin in (list, Sequence):
        if not args:
            raise UnsupportedTypeError("List type hints must provide an inner type")
        item_type = args[0]
        return [_coerce_row(row, item_type, validate=validate) for row in rows]  # type: ignore[return-value]

    if isinstance(type_hint, type) and issubclass(type_hint, BaseModel):
        if len(rows) != 1:
            raise SchemaMismatchError("Expected a single row when decoding into a BaseModel")
        return _coerce_row(rows[0], type_hint, validate=validate)  # type: ignore[return-value]

    return rows  # type: ignore[return-value]


def _normalise_input(obj: Any) -> tuple[list[dict[str, Any]], type[BaseModel] | None]:
    if isinstance(obj, BaseModel):
        return [obj.model_dump(mode="python")], type(obj)

    if isinstance(obj, dict):
        return [obj], None

    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        rows: list[dict[str, Any]] = []
        model_type: type[BaseModel] | None = None
        for item in obj:
            if isinstance(item, BaseModel):
                if model_type is None:
                    model_type = type(item)
                elif type(item) is not model_type:
                    raise SchemaMismatchError("All items must share the same Pydantic model type")
                rows.append(item.model_dump(mode="python"))
            elif isinstance(item, dict):
                rows.append(item)
            else:
                raise UnsupportedTypeError(f"Unsupported item type: {type(item)!r}")
        return rows, model_type

    raise UnsupportedTypeError(f"Unsupported object type: {type(obj)!r}")


def _apply_metadata(
    table: pa.Table, model_type: type[BaseModel] | None, config: DAConfig | None
) -> pa.Table:
    if model_type is None:
        return table

    metadata = dict(table.schema.metadata or {})

    config = config or DAConfig()
    metadata[b"pydantic_model_fqn"] = _fully_qualified_name(model_type).encode()
    metadata[b"pydantic_version"] = PYDANTIC_VERSION.encode()
    metadata[b"datetime_policy"] = config.datetime_policy.encode()
    metadata[b"model_schema_hash"] = _hash_json(model_type.model_json_schema())

    return table.replace_schema_metadata(metadata)


def _fully_qualified_name(model_type: type[BaseModel]) -> str:
    return f"{model_type.__module__}.{model_type.__qualname__}"


def _hash_json(value: Any) -> bytes:
    payload = json.dumps(value, sort_keys=True, default=_json_default).encode()
    return hashlib.sha256(payload).hexdigest().encode()


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if dataclass_like := getattr(obj, "__dataclass_fields__", None):
        return asdict(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")


def _ensure_table(data: pa.Array | pa.RecordBatch | pa.Table) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    if isinstance(data, pa.Array):
        return pa.Table.from_arrays([data], names=["value"])
    raise UnsupportedTypeError(f"Unsupported Arrow type: {type(data)!r}")


def _coerce_row(row: dict[str, Any], target: type[Any], *, validate: bool) -> Any:
    if isinstance(target, type) and issubclass(target, BaseModel):
        if validate:
            return target.model_validate(row)
        return target.model_construct(**row)
    return row


def _is_single_object(obj: Any) -> bool:
    return isinstance(obj, (BaseModel, dict))


def _encode_special_types(
    rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[bytes, bytes]]:
    uuid_paths: dict[str, dict[str, Any]] = {}

    def encode(value: Any, path: str) -> Any:
        if isinstance(value, UUID):
            info = uuid_paths.setdefault(path, {"version": value.version})
            if info.get("version") != value.version:
                info["version"] = None
            return value.bytes

        if isinstance(value, list):
            return [encode(item, f"{path}[]") for item in value]

        if isinstance(value, tuple):
            return [encode(item, f"{path}[]") for item in value]

        if isinstance(value, dict):
            return {key: encode(child, f"{path}.{key}") for key, child in value.items()}

        return value

    encoded_rows: list[dict[str, Any]] = []
    for original in rows:
        encoded_row: dict[str, Any] = {}
        for key, value in original.items():
            encoded_row[key] = encode(value, key)
        encoded_rows.append(encoded_row)

    metadata: dict[bytes, bytes] = {}
    if uuid_paths:
        metadata[UUID_METADATA_KEY] = json.dumps(uuid_paths).encode()
        metadata[UUID_ENCODING_KEY] = UUID_ENCODING_VALUE
        metadata[UUID_UTILS_KEY] = UUID_UTILS_VALUE
        versions = {info["version"] for info in uuid_paths.values() if info.get("version") is not None}
        if len(versions) == 1:
            metadata[UUID_VERSION_KEY] = str(next(iter(versions))).encode()
    return encoded_rows, metadata


def _decode_special_types(
    rows: list[dict[str, Any]], metadata: dict[bytes, bytes] | None
) -> list[dict[str, Any]]:
    if not metadata or UUID_METADATA_KEY not in metadata:
        return rows

    raw = json.loads(metadata[UUID_METADATA_KEY].decode())
    if isinstance(raw, dict):
        uuid_paths = set(raw.keys())
    else:
        uuid_paths = set(raw)

    def decode(value: Any, path: str) -> Any:
        if path in uuid_paths:
            if value is None:
                return None
            if isinstance(value, list):
                return [decode(item, f"{path}[]") for item in value]
            if isinstance(value, tuple):
                return tuple(decode(item, f"{path}[]") for item in value)
            if isinstance(value, (bytes, bytearray)):
                return UUID(bytes=bytes(value))
            return value

        if isinstance(value, list):
            return [decode(item, f"{path}[]") for item in value]

        if isinstance(value, tuple):
            return tuple(decode(item, f"{path}[]") for item in value)

        if isinstance(value, dict):
            return {key: decode(child, f"{path}.{key}") for key, child in value.items()}

        return value

    decoded_rows: list[dict[str, Any]] = []
    for original in rows:
        decoded_row: dict[str, Any] = {}
        for key, value in original.items():
            decoded_row[key] = decode(value, key)
        decoded_rows.append(decoded_row)
    return decoded_rows


def _normalise_datetime_values(rows: list[dict[str, Any]], config: DAConfig) -> list[dict[str, Any]]:
    return [_map_values(row, lambda value: _normalise_datetime(value, config.datetime_policy)) for row in rows]


def _restore_datetime_values(rows: list[dict[str, Any]], policy: str) -> list[dict[str, Any]]:
    return [_map_values(row, lambda value: _restore_datetime(value, policy)) for row in rows]


def _map_values(payload: dict[str, Any], transform: Callable[[Any], Any]) -> dict[str, Any]:
    def apply(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: apply(child) for key, child in value.items()}
        if isinstance(value, list):
            return [apply(child) for child in value]
        if isinstance(value, tuple):
            return tuple(apply(child) for child in value)
        return transform(value)

    return {key: apply(value) for key, value in payload.items()}


def _normalise_datetime(value: Any, policy: str) -> Any:
    if not isinstance(value, datetime):
        return value

    if value.tzinfo is None:
        if policy == "normalize_utc":
            return value.replace(tzinfo=timezone.utc)
        if policy == "error_on_naive":
            raise ValueError("Naive datetime encountered under 'error_on_naive' policy")
        return value

    if policy == "normalize_utc":
        return value.astimezone(timezone.utc)
    return value


def _restore_datetime(value: Any, policy: str) -> Any:
    if not isinstance(value, datetime):
        return value

    if policy == "normalize_utc":
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    return value


def _metadata_datetime_policy(metadata: dict[bytes, bytes] | None, config: DAConfig) -> str:
    if metadata and b"datetime_policy" in metadata:
        return metadata[b"datetime_policy"].decode()
    return config.datetime_policy
