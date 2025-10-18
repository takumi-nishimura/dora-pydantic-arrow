"""Minimal stub implementation of the parts of PyArrow used in the tests."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import Any, Iterable, Iterator, Sequence

__all__ = [
    "Array",
    "RecordBatch",
    "Schema",
    "Table",
    "array",
    "binary",
    "bool_",
    "date32",
    "decimal128",
    "field",
    "float64",
    "int64",
    "list_",
    "map_",
    "schema",
    "string",
    "struct",
    "time64",
    "timestamp",
    "types",
]


@dataclass(frozen=True)
class DataType:
    name: str
    args: tuple[Any, ...] = ()

    def __repr__(self) -> str:
        if not self.args:
            return self.name
        arg_repr = ", ".join(repr(arg) for arg in self.args)
        return f"{self.name}({arg_repr})"


@dataclass(frozen=True)
class Field:
    name: str
    type: DataType
    nullable: bool = False


class Schema:
    def __init__(self, fields: Sequence[Field], metadata: dict[bytes, bytes] | None = None) -> None:
        self._fields = list(fields)
        self._metadata = dict(metadata or {})

    def field(self, name: str) -> Field:
        for field in self._fields:
            if field.name == name:
                return field
        raise KeyError(name)

    def __iter__(self) -> Iterator[Field]:
        return iter(self._fields)

    @property
    def metadata(self) -> dict[bytes, bytes]:
        return dict(self._metadata)

    def with_metadata(self, metadata: dict[bytes, bytes]) -> "Schema":
        return Schema(self._fields, metadata)


def field(name: str, type: DataType, nullable: bool = False) -> Field:
    return Field(name, type, nullable)


def schema(fields: Sequence[Field]) -> Schema:
    return Schema(fields)


@dataclass
class Array:
    values: list[Any]
    type: DataType


def array(values: Iterable[Any], type: DataType | None = None) -> Array:
    values_list = list(values)
    if type is None and values_list:
        type = _infer_type(values_list[0])
    if type is None:
        raise ValueError("Unable to infer type for empty array")
    return Array(values_list, type)


class RecordBatch:
    def __init__(self, rows: list[dict[str, Any]], schema: Schema) -> None:
        self._rows = rows
        self.schema = schema

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    @classmethod
    def from_arrays(cls, arrays: Sequence[Array], schema: Schema) -> "RecordBatch":
        row_count = max((len(array.values) for array in arrays), default=0)
        rows: list[dict[str, Any]] = []
        names = [field.name for field in schema]
        for index in range(row_count):
            row = {}
            for name, array in zip(names, arrays):
                values = array.values
                row[name] = values[index] if index < len(values) else None
            rows.append(row)
        return cls(rows, schema)


class Table:
    def __init__(self, rows: list[dict[str, Any]], schema: Schema) -> None:
        self._rows = rows
        self.schema = schema

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    def to_batches(self) -> list[RecordBatch]:
        return [RecordBatch(list(self._rows), self.schema)]

    def to_pylist(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows]

    def replace_schema_metadata(self, metadata: dict[bytes, bytes]) -> "Table":
        return Table(self._rows, self.schema.with_metadata(metadata))

    @classmethod
    def from_pylist(cls, rows: Sequence[dict[str, Any]], schema: Schema | None = None) -> "Table":
        rows_list = [dict(row) for row in rows]
        if schema is None:
            schema = _infer_schema(rows_list)
        return cls(rows_list, schema)

    @classmethod
    def from_batches(cls, batches: Sequence[RecordBatch]) -> "Table":
        rows: list[dict[str, Any]] = []
        schema: Schema | None = None
        for batch in batches:
            rows.extend(batch._rows)
            schema = batch.schema
        if schema is None:
            raise ValueError("Cannot create table from empty batch sequence")
        return cls(rows, schema)

    @classmethod
    def from_arrays(cls, arrays: Sequence[Array], names: Sequence[str]) -> "Table":
        schema_fields = [field(name, array.type, nullable=True) for name, array in zip(names, arrays)]
        schema_obj = Schema(schema_fields)
        rows: list[dict[str, Any]] = []
        length = max((len(array.values) for array in arrays), default=0)
        for index in range(length):
            row = {}
            for name, array in zip(names, arrays):
                values = array.values
                row[name] = values[index] if index < len(values) else None
            rows.append(row)
        return cls(rows, schema_obj)


class _Types:
    @staticmethod
    def is_string(dtype: DataType) -> bool:
        return isinstance(dtype, DataType) and dtype.name == "string"


types = _Types()


def list_(value_type: DataType) -> DataType:
    return DataType("list", (value_type,))


def struct(fields: Sequence[Field]) -> DataType:
    return DataType("struct", tuple(fields))


def map_(key_type: DataType, value_type: DataType) -> DataType:
    return DataType("map", (key_type, value_type))


def int64() -> DataType:
    return DataType("int64")


def bool_() -> DataType:
    return DataType("bool")


def float64() -> DataType:
    return DataType("float64")


def string() -> DataType:
    return DataType("string")


def binary() -> DataType:
    return DataType("binary")


def timestamp(unit: str, tz: str | None = None) -> DataType:
    return DataType("timestamp", (unit, tz))


def date32() -> DataType:
    return DataType("date32")


def time64(unit: str) -> DataType:
    return DataType("time64", (unit,))


def decimal128(precision: int, scale: int) -> DataType:
    return DataType("decimal128", (precision, scale))


def _infer_type(value: Any) -> DataType:
    if isinstance(value, bool):
        return bool_()
    if isinstance(value, int):
        return int64()
    if isinstance(value, float):
        return float64()
    if isinstance(value, str):
        return string()
    if isinstance(value, bytes):
        return binary()
    if isinstance(value, dict):
        return struct([field(name, _infer_type(val), nullable=val is None) for name, val in value.items()])
    raise TypeError(f"Cannot infer type for value {value!r}")


def _infer_schema(rows: list[dict[str, Any]]) -> Schema:
    if not rows:
        raise ValueError("Cannot infer schema from empty rows")
    first = rows[0]
    fields_list = []
    for name, value in first.items():
        dtype = _infer_type(value)
        nullable = any(row.get(name) is None for row in rows)
        fields_list.append(field(name, dtype, nullable=nullable))
    return Schema(fields_list)
