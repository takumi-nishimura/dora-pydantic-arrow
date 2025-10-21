import json
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union
from uuid import UUID

import numpy as np
import pyarrow as pa
import uuid_utils
from pydantic import UUID7, BaseModel, Field

import dora_pydantic_arrow as dpa
from dora_pydantic_arrow import (
    BatchBuilder,
    DAConfig,
)


class StrKind(Enum):
    EXAMPLE = "example"
    ANOTHER = "another"


class IntKind(Enum):
    FIRST = 1
    SECOND = 2


class ExampleModel(BaseModel):
    id: UUID7 = Field(default_factory=lambda: UUID(bytes=uuid_utils.uuid7().bytes))
    date: datetime = Field(default_factory=datetime.now)
    kind_str: StrKind = StrKind.EXAMPLE
    kind_int: IntKind = IntKind.FIRST
    name: str
    score: float | None = None
    payload: Optional[Dict[str, Union[str, int, float, bool, None, List, Dict]]] = None
    images: Optional[Dict[str, np.ndarray]] = None


def test_to_arrow_from_arrow_roundtrip_models() -> None:
    rows = [
        ExampleModel(name="Alice", score=0.5),
        ExampleModel(
            name="Bob",
            score=None,
            payload={
                "string": "value",
                "number": 42,
                "list": [1, 2, 3],
                "nested": {"a": 1},
            },
            images={"img1": np.random.randint(0, 255, (64, 64, 3), dtype=np.uint8)},
        ),
    ]

    record_batch = dpa.to_arrow(rows)
    assert isinstance(record_batch, pa.RecordBatch)
    assert record_batch.num_rows == len(rows)

    table = dpa.to_arrow(rows, as_table=True)
    assert isinstance(table, pa.Table)

    metadata = table.schema.metadata or {}
    assert metadata[b"pydantic_model_fqn"].decode() == (
        f"{ExampleModel.__module__}.{ExampleModel.__qualname__}"
    )
    assert metadata[b"datetime_policy"].decode() == DAConfig().datetime_policy
    assert metadata[b"uuid.encoding"] == b"binary16"
    assert metadata[b"uuid_utils"] == b"true"
    assert metadata[b"uuid.version"] == b"7"
    uuid_columns = json.loads(metadata[b"uuid_columns"].decode())
    assert "id" in uuid_columns

    restored = dpa.from_arrow(record_batch, type_hint=list[ExampleModel])
    assert restored == rows


def test_from_arrow_accepts_struct_array() -> None:
    rows = [
        ExampleModel(name="Alice", score=0.5),
        ExampleModel(name="Bob", score=1.5),
    ]

    struct_array = dpa.to_arrow(rows).to_struct_array()
    restored = dpa.from_arrow(struct_array, type_hint=list[ExampleModel])

    assert restored == rows


def test_schema_from_model_respects_optional() -> None:
    schema = dpa.schema_from_model(ExampleModel)

    id_field = schema.field("id")
    score_field = schema.field("score")
    payload_field = schema.field("payload")

    assert id_field.nullable is False
    assert id_field.type == pa.binary(16)
    assert score_field.nullable is True
    assert score_field.type == pa.float64()
    assert payload_field.nullable is True
    assert payload_field.type == pa.large_binary()
    assert (payload_field.metadata or {}).get(b"dpa.serialized") == b"dict_any"


def test_batch_builder_creates_record_batch() -> None:
    rows = [
        ExampleModel(name="Alice", score=0.5),
        ExampleModel(name="Bob", score=1.0),
    ]

    builder = BatchBuilder(schema=ExampleModel)
    for row in rows:
        builder.append(row)

    record_batch = builder.to_record_batch()
    assert isinstance(record_batch, pa.RecordBatch)
    assert record_batch.num_rows == len(rows)

    builder.clear()
    assert builder.to_record_batch().num_rows == 0

    restored = dpa.from_arrow(
        pa.Table.from_batches([record_batch]), type_hint=list[ExampleModel]
    )
    assert restored == rows
