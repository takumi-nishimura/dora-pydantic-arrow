import pyarrow as pa
from pydantic import BaseModel

from dora_pydantic_arrow import (
    BatchBuilder,
    DAConfig,
    from_arrow,
    schema_from_model,
    to_arrow,
)


class ExampleModel(BaseModel):
    id: int
    name: str
    score: float | None = None


def test_to_arrow_from_arrow_roundtrip_models() -> None:
    rows = [
        ExampleModel(id=1, name="Alice", score=0.5),
        ExampleModel(id=2, name="Bob", score=None),
    ]

    table = to_arrow(rows)

    assert isinstance(table, pa.Table)
    assert table.num_rows == len(rows)

    metadata = table.schema.metadata or {}
    assert metadata[b"pydantic_model_fqn"].decode() == (
        f"{ExampleModel.__module__}.{ExampleModel.__qualname__}"
    )
    assert metadata[b"datetime_policy"].decode() == DAConfig().datetime_policy

    restored = from_arrow(table, type_hint=list[ExampleModel])
    assert restored == rows


def test_schema_from_model_respects_optional() -> None:
    schema = schema_from_model(ExampleModel)

    id_field = schema.field("id")
    score_field = schema.field("score")

    assert id_field.nullable is False
    assert id_field.type == pa.int64()
    assert score_field.nullable is True
    assert score_field.type == pa.float64()


def test_batch_builder_creates_record_batch() -> None:
    rows = [
        ExampleModel(id=1, name="Alice", score=0.5),
        ExampleModel(id=2, name="Bob", score=1.0),
    ]

    builder = BatchBuilder(schema=ExampleModel)
    for row in rows:
        builder.append(row)

    record_batch = builder.to_record_batch()
    assert isinstance(record_batch, pa.RecordBatch)
    assert record_batch.num_rows == len(rows)

    builder.clear()
    assert builder.to_record_batch().num_rows == 0

    restored = from_arrow(pa.Table.from_batches([record_batch]), type_hint=list[ExampleModel])
    assert restored == rows
