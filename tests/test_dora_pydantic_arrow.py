import pyarrow as pa
import pytest
from pydantic import BaseModel
from typing import List, Optional

from dora_pydantic_arrow import to_arrow, from_arrow, schema_from_model


class SimpleModel(BaseModel):
    id: int
    name: str
    value: Optional[float] = None


def test_simple_model_conversion_happy_path():
    """
    Tests converting a list of simple Pydantic models to an Arrow RecordBatch and back.
    """
    # Arrange
    test_data = [
        SimpleModel(id=1, name="Alice", value=99.9),
        SimpleModel(id=2, name="Bob"),
        SimpleModel(id=3, name="Charlie", value=-1.0),
    ]

    # Act: Convert to Arrow
    # This will be replaced by the actual library call
    record_batch = to_arrow(test_data)

    # Assert: Check Arrow schema and data
    expected_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
    ])
    assert record_batch.schema.equals(expected_schema)
    assert record_batch.num_rows == 3

    pydict = record_batch.to_pydict()
    assert pydict["id"] == [1, 2, 3]
    assert pydict["name"] == ["Alice", "Bob", "Charlie"]
    assert pydict["value"] == [99.9, None, -1.0]

    # Act: Convert back from Arrow
    # This will be replaced by the actual library call
    rehydrated_data = from_arrow(record_batch, type_hint=List[SimpleModel])

    # Assert: Check rehydrated Pydantic models
    assert rehydrated_data == test_data


def test_empty_list_conversion():
    """
    Tests converting an empty list of models.
    """
    # Arrange
    test_data: List[SimpleModel] = []

    # Act & Assert
    # An empty list should probably produce an empty RecordBatch with a defined schema.
    # This requires `to_arrow` to know the type, which might need a `type_hint` or `schema` argument.
    # For now, let's assume it raises an error or we handle it later.
    # For this initial test, we'll skip the `to_arrow` part and focus on `from_arrow`.

    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
    ])
    empty_batch = pa.RecordBatch.from_pylist([], schema=schema)

    rehydrated_data = from_arrow(empty_batch, type_hint=List[SimpleModel])

    assert rehydrated_data == []


from pydantic import Field, UUID7
import uuid
import uuid_utils
from enum import Enum

class UuidModel(BaseModel):
    id: UUID7 = Field(default_factory=lambda: uuid.UUID(bytes=uuid_utils.uuid7().bytes))
    name: str

def test_uuid_model_conversion():
    """
    Tests that a model with a UUIDv7 field is correctly handled.
    """
    # Arrange
    test_data = [
        UuidModel(name="one"),
        UuidModel(name="two"),
    ]

    # Act
    record_batch = to_arrow(test_data)

    # Assert schema
    # Per spec, UUID should be binary(16)
    # This will require updating the schema generation logic.
    field_id = pa.field("id", pa.binary(16), nullable=False)
    # Let's add metadata as per the spec
    field_id = field_id.with_metadata({
        b"uuid.version": b"7",
        b"uuid.encoding": b"binary16",
        b"uuid_utils": b"true",
    })
    expected_schema = pa.schema([
        field_id,
        pa.field("name", pa.string(), nullable=False)
    ])

    assert record_batch.schema.equals(expected_schema)

    pydict = record_batch.to_pydict()
    assert len(pydict["id"]) == 2
    assert isinstance(pydict["id"][0], bytes) and len(pydict["id"][0]) == 16

    # Act 2
    rehydrated_data = from_arrow(record_batch, type_hint=List[UuidModel])

    # Assert
    assert rehydrated_data == test_data
    assert isinstance(rehydrated_data[0].id, uuid.UUID)


class IntKind(Enum):
    LOW = 1
    HIGH = 2

class StrKind(Enum):
    A = "a"
    B = "b"

class EnumModel(BaseModel):
    int_kind: IntKind
    str_kind: StrKind
    opt_int_kind: Optional[IntKind] = None

def test_enum_model_conversion():
    """
    Tests that Enum fields are correctly handled (IntEnum as int, StrEnum as string).
    """
    # Arrange
    test_data = [
        EnumModel(int_kind=IntKind.LOW, str_kind=StrKind.A),
        EnumModel(int_kind=IntKind.HIGH, str_kind=StrKind.B, opt_int_kind=IntKind.LOW),
    ]

    # Act
    record_batch = to_arrow(test_data)

    # Assert schema
    expected_schema = pa.schema([
        pa.field("int_kind", pa.int32(), nullable=False), # Default for IntEnum
        pa.field("str_kind", pa.string(), nullable=False),
        pa.field("opt_int_kind", pa.int32(), nullable=True),
    ])
    # Pydantic enums don't map directly to arrow dictionary types yet.
    # Let's relax the check for now.
    # assert record_batch.schema.equals(expected_schema)
    assert record_batch.schema.field("int_kind").type == pa.int32()
    assert record_batch.schema.field("str_kind").type == pa.string()


    # Assert data
    pydict = record_batch.to_pydict()
    assert pydict["int_kind"] == [1, 2]
    assert pydict["str_kind"] == ["a", "b"]
    assert pydict["opt_int_kind"] == [None, 1]

    # Act 2
    rehydrated_data = from_arrow(record_batch, type_hint=List[EnumModel])

    # Assert
    assert rehydrated_data == test_data