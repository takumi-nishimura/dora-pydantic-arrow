from typing import List, Type, TypeVar
from pydantic import BaseModel
import pyarrow as pa
from enum import Enum

from .config import DAConfig
from .schema import schema_from_model

T = TypeVar("T")

def to_arrow(
    obj: object,
    *,
    schema: pa.Schema | None = None,
    config: DAConfig | None = None
) -> pa.RecordBatch | pa.Table:
    """
    Converts a Python object (especially Pydantic models) to an Arrow RecordBatch or Table.
    """
    config = config or DAConfig()

    if not isinstance(obj, list) or not obj:
        # Handle empty list case and non-list inputs
        if schema:
             return pa.RecordBatch.from_pylist([], schema=schema)
        # For now, we require a non-empty list to infer the schema
        raise ValueError("Input must be a non-empty list of Pydantic models to infer schema, or an explicit schema must be provided.")

    first_item = obj[0]
    if not isinstance(first_item, BaseModel):
        raise TypeError("Items in the list must be Pydantic BaseModel instances.")

    model_type = type(first_item)
    if schema is None:
        schema = schema_from_model(model_type, config=config)

    # Convert list of Pydantic models to a dictionary of lists (columnar format)
    pydict = {}
    for field in schema:
        values = []
        for item in obj:
            val = getattr(item, field.name, None)
            if val is None:
                values.append(None)
            elif isinstance(val, Enum):
                values.append(val.value)
            elif field.type == pa.binary(16):
                values.append(val.bytes)
            else:
                values.append(val)
        pydict[field.name] = values

    return pa.RecordBatch.from_pydict(pydict, schema=schema)


def from_arrow(
    data: pa.RecordBatch | pa.Table,
    *,
    type_hint: Type[T],
    validate: bool = True,
    config: DAConfig | None = None
) -> T:
    """
    Converts an Arrow RecordBatch or Table to a Python object (especially Pydantic models).
    """
    config = config or DAConfig()

    if not isinstance(data, (pa.RecordBatch, pa.Table)):
        raise TypeError(f"Input data must be a PyArrow RecordBatch or Table, not {type(data).__name__}")

    # Get the target Pydantic model from the type hint (e.g., List[MyModel])
    # This is a simplification; a more robust solution would handle more complex hints.
    try:
        # Works for List[MyModel], list[MyModel]
        model_type = type_hint.__args__[0]
        if not issubclass(model_type, BaseModel):
            raise TypeError("The parameterized type in the List hint must be a Pydantic BaseModel.")
    except (AttributeError, IndexError):
        raise TypeError("`type_hint` must be a parameterized List, e.g., List[MyModel]")

    pydict = data.to_pydict()
    num_rows = data.num_rows

    # Reconstruct the list of Pydantic models
    if validate:
        # Pydantic V2 automatically validates on instantiation
        return [model_type(**{key: pydict[key][i] for key in pydict}) for i in range(num_rows)]
    else:
        # Skip validation (faster)
        return [model_type.model_construct(**{key: pydict[key][i] for key in pydict}) for i in range(num_rows)]