from datetime import date, time, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, List, Dict, Tuple, Union, get_args, get_origin, Optional
from pydantic import BaseModel
from pydantic.fields import FieldInfo
import pyarrow as pa
import uuid
import pydantic
import numpy as np

from .config import DAConfig
from .errors import UnsupportedTypeError

# Type mapping from Python/Pydantic to PyArrow
# This will be expanded as we add support for more types.
PYDANTIC_TO_ARROW_TYPE_MAP = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
    date: pa.date32(),
    time: pa.time64("us"),
    datetime: pa.timestamp("us", tz="UTC"), # Default based on spec
    Decimal: pa.decimal128(38, 9), # Default based on spec
    uuid.UUID: pa.binary(16),
}

def schema_from_model(model_type: type[BaseModel], *, config: DAConfig = None) -> pa.Schema:
    """
    Generates a PyArrow Schema from a Pydantic BaseModel.
    """
    if not issubclass(model_type, BaseModel):
        raise TypeError(f"Expected a Pydantic BaseModel, got {model_type.__name__}")

    config = config or DAConfig()
    fields = []
    for field_name, field_info in model_type.model_fields.items():
        arrow_type = _convert_pydantic_type_to_arrow(field_info, config)
        is_nullable = field_info.is_required is False or (
            get_origin(field_info.annotation) is Union and type(None) in get_args(field_info.annotation)
        )

        field = pa.field(field_name, arrow_type, nullable=is_nullable)

        # Add metadata for UUIDs
        type_hint = field_info.annotation
        # Unwrap Optional
        if get_origin(type_hint) is Union and len(get_args(type_hint)) == 2 and type(None) in get_args(type_hint):
            type_hint = next(arg for arg in get_args(type_hint) if arg is not type(None))

        is_uuid7 = any(getattr(m, 'uuid_version', 0) == 7 for m in field_info.metadata)
        if type_hint is uuid.UUID or is_uuid7:
             meta = {
                "uuid.version": "7" if is_uuid7 else "unknown",
                "uuid.encoding": "binary16",
                "uuid_utils": "true"
             }
             field = field.with_metadata({k.encode(): v.encode() for k, v in meta.items()})

        fields.append(field)

    # TODO: Add model-level metadata from spec
    return pa.schema(fields)


def _convert_pydantic_type_to_arrow(field_info: FieldInfo, config: DAConfig) -> pa.DataType:
    """
    Converts a Pydantic field's type to a PyArrow DataType.
    """
    type_hint = field_info.annotation
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    # Handle Optional[T] by unwrapping it
    if origin is Union and len(args) == 2 and type(None) in args:
        # It's an Optional, get the non-None type
        type_hint = next(arg for arg in args if arg is not type(None))
        origin = get_origin(type_hint)
        args = get_args(type_hint)

    # Check for UUID before simple types because it's more specific
    if type_hint is uuid.UUID or any(isinstance(m, pydantic.types.UuidVersion) for m in field_info.metadata):
        return pa.binary(16)

    # Simple types
    if type_hint in PYDANTIC_TO_ARROW_TYPE_MAP:
        return PYDANTIC_TO_ARROW_TYPE_MAP[type_hint]

    # List[T]
    if origin in (list, List):
        if not args:
            raise UnsupportedTypeError("List type hint must be parameterized, e.g., List[int]")
        # TODO: This is a simplified placeholder. We need a proper recursive call.
        item_type = args[0]
        if item_type in PYDANTIC_TO_ARROW_TYPE_MAP:
             return pa.list_(PYDANTIC_TO_ARROW_TYPE_MAP[item_type])
        elif issubclass(item_type, BaseModel):
             # Recursive call for nested models
             nested_schema = schema_from_model(item_type, config=config)
             return pa.list_(pa.struct(nested_schema))

    # Enum
    if isinstance(type_hint, type) and issubclass(type_hint, Enum):
        # Check the type of the enum values
        value_types = {type(e.value) for e in type_hint}
        if len(value_types) > 1:
            raise UnsupportedTypeError(f"Mixed-type Enums are not supported: {type_hint}")

        value_type = value_types.pop()
        if value_type is int:
            return pa.int32() # Default as per test
        elif value_type is str:
            return pa.string()
        else:
            raise UnsupportedTypeError(f"Enum with value type {value_type} is not supported.")

    # More complex types will be added here...

    raise UnsupportedTypeError(f"Type '{type_hint}' is not yet supported.")