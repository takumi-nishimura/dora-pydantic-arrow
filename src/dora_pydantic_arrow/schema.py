"""Helpers for deriving Arrow schemas from Pydantic models."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Any, Union, get_args, get_origin
from uuid import UUID

import pyarrow as pa
from pydantic import BaseModel

from .config import DAConfig
from .exceptions import UnsupportedTypeError

SERIALIZED_FIELD_METADATA_KEY = b"dpa.serialized"
SERIALIZED_FIELD_KIND_DICT_ANY = b"dict_any"


def schema_from_model(model_type: type[BaseModel], *, config: DAConfig | None = None) -> pa.Schema:
    """Create an Arrow schema representing the supplied Pydantic model."""

    fields = []
    for name, field in model_type.model_fields.items():
        annotation = field.annotation or field.outer_type_  # type: ignore[assignment]
        arrow_type, nullable = _annotation_to_arrow(annotation, config)
        field_metadata = _field_metadata_for_annotation(annotation)
        fields.append(pa.field(name, arrow_type, nullable=nullable, metadata=field_metadata))
    return pa.schema(fields)


def _annotation_to_arrow(annotation: Any, config: DAConfig | None) -> tuple[pa.DataType, bool]:
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is None:
        return _simple_type_to_arrow(annotation, config), False

    if origin in (list, tuple, set):
        if not args:
            raise UnsupportedTypeError("Container annotations must declare an inner type")
        if origin is tuple:
            child_fields = []
            for index, child in enumerate(args):
                child_type, child_nullable = _annotation_to_arrow(child, config)
                child_fields.append(pa.field(f"f{index}", child_type, nullable=child_nullable))
            return pa.struct(child_fields), False
        item_type, _ = _annotation_to_arrow(args[0], config)
        return pa.list_(item_type), False

    if origin is dict:
        key_annotation, value_annotation = args
        key_type, key_nullable = _annotation_to_arrow(key_annotation, config)
        if key_nullable:
            raise UnsupportedTypeError("Dictionary key annotations cannot be optional")
        if not pa.types.is_string(key_type):
            raise UnsupportedTypeError("Only string dictionary keys are supported")
        if value_annotation is Any:
            return pa.large_binary(), False
        value_type, _ = _annotation_to_arrow(value_annotation, config)
        return pa.map_(key_type, value_type), False

    if origin in (Union, UnionType):
        return _handle_union(args, config)

    raise UnsupportedTypeError(f"Unsupported type annotation: {annotation!r}")


def _handle_union(args: tuple[Any, ...], config: DAConfig | None) -> tuple[pa.DataType, bool]:
    non_none = [arg for arg in args if arg is not type(None)]  # noqa: E721
    contains_none = len(non_none) != len(args)

    if len(non_none) != 1:
        raise UnsupportedTypeError("Only Optional[T] unions are supported")

    child_type, _ = _annotation_to_arrow(non_none[0], config)
    return child_type, True if contains_none else False


def _simple_type_to_arrow(annotation: Any, config: DAConfig | None) -> pa.DataType:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        nested_schema = schema_from_model(annotation, config=config)
        return pa.struct(nested_schema)

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        value_types = {type(member.value) for member in annotation}
        if not value_types:
            raise UnsupportedTypeError("Enum annotations must declare at least one value")
        if len(value_types) != 1:
            raise UnsupportedTypeError("Enums with mixed value types are not supported")
        value_type = value_types.pop()
        return _simple_type_to_arrow(value_type, config)

    if annotation is int:
        return pa.int64()

    if annotation is bool:
        return pa.bool_()

    if annotation is float:
        return pa.float64()

    if annotation is str:
        return pa.string()

    if annotation is bytes:
        return pa.binary()

    if annotation is datetime:
        return pa.timestamp("us", tz="UTC")

    if annotation is date:
        return pa.date32()

    if annotation is time:
        return pa.time64("us")

    if annotation is Decimal:
        cfg = config or DAConfig()
        return pa.decimal128(cfg.decimal_precision, cfg.decimal_scale)

    if annotation is UUID:
        return pa.binary(16)

    raise UnsupportedTypeError(f"Unsupported type annotation: {annotation!r}")


def _field_metadata_for_annotation(annotation: Any) -> dict[bytes, bytes] | None:
    base_annotation = _strip_optional(annotation)

    if _is_dict_with_any_values(base_annotation):
        return {
            SERIALIZED_FIELD_METADATA_KEY: SERIALIZED_FIELD_KIND_DICT_ANY,
        }

    return None


def _strip_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]  # noqa: E721
        if len(args) == 1:
            return args[0]
    return annotation


def _is_dict_with_any_values(annotation: Any) -> bool:
    if get_origin(annotation) is not dict:
        return False
    args = get_args(annotation)
    if len(args) != 2:
        return False
    key_annotation, value_annotation = args
    return key_annotation is str and value_annotation is Any
