from uuid import UUID

import pyarrow as pa
import uuid_utils
from pydantic import UUID7, BaseModel, Field

import dora_pydantic_arrow as dpa
from dora_pydantic_arrow import (
    BatchBuilder,
    DAConfig,
)


class ExampleModel(BaseModel):
    id: UUID7 = Field(default_factory=lambda: UUID(bytes=uuid_utils.uuid7().bytes))
    name: str
    score: float | None = None


def test_to_arrow_from_arrow_roundtrip_models() -> None:
    rows = [
        ExampleModel(name="Alice", score=0.5),
        ExampleModel(name="Bob", score=None),
    ]

    table = dpa.to_arrow(rows)

    assert isinstance(table, pa.Table)
    assert table.num_rows == len(rows)

    metadata = table.schema.metadata or {}
    assert metadata[b"pydantic_model_fqn"].decode() == (
        f"{ExampleModel.__module__}.{ExampleModel.__qualname__}"
    )
    assert metadata[b"datetime_policy"].decode() == DAConfig().datetime_policy

    restored = dpa.from_arrow(table, type_hint=list[ExampleModel])
    assert restored == rows


def test_schema_from_model_respects_optional() -> None:
    schema = dpa.schema_from_model(ExampleModel)

    id_field = schema.field("id")
    score_field = schema.field("score")

    assert id_field.nullable is False
    assert id_field.type == pa.int64()
    assert score_field.nullable is True
    assert score_field.type == pa.float64()


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
