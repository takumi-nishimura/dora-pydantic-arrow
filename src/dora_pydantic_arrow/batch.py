"""Batch builder utilities for incrementally creating Arrow RecordBatches."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from pydantic import BaseModel

from .config import DAConfig
from .conversion import to_arrow
from .schema import schema_from_model


class BatchBuilder:
    """Accumulate Python objects before emitting an Arrow ``RecordBatch``."""

    def __init__(
        self,
        *,
        schema: pa.Schema | type[BaseModel] | None = None,
        batch_size: int = 65536,
        config: DAConfig | None = None,
    ) -> None:
        self._rows: list[Any] = []
        self._batch_size = batch_size
        self._config = config or DAConfig()
        self._model_type: type[BaseModel] | None = None
        if isinstance(schema, pa.Schema):
            self._schema = schema
        elif isinstance(schema, type) and issubclass(schema, BaseModel):
            self._schema = schema_from_model(schema, config=self._config)
            self._model_type = schema
        elif schema is None:
            self._schema = None
        else:
            raise TypeError("schema must be a pyarrow.Schema or BaseModel type")

    def append(self, obj: Any) -> None:
        """Append a Python object or Pydantic model to the current batch."""

        if isinstance(obj, BaseModel):
            if self._model_type is None:
                self._model_type = type(obj)
                if self._schema is None:
                    self._schema = schema_from_model(self._model_type, config=self._config)
            elif type(obj) is not self._model_type:
                raise TypeError("All appended models must share the same type")
            self._rows.append(obj)
        elif isinstance(obj, dict):
            self._rows.append(obj)
        else:
            raise TypeError("BatchBuilder only accepts Pydantic models or dictionaries")

    def to_record_batch(self) -> pa.RecordBatch:
        """Return the accumulated rows as a ``RecordBatch``."""

        if not self._rows:
            if self._schema is None:
                raise ValueError("Cannot produce an empty RecordBatch without a schema")
            arrays = [pa.array([], type=field.type) for field in self._schema]
            return pa.RecordBatch.from_arrays(arrays, schema=self._schema)

        arrow_obj = to_arrow(
            self._rows,
            schema=self._schema,
            config=self._config,
        )

        if isinstance(arrow_obj, pa.RecordBatch):
            return arrow_obj
        batches = arrow_obj.to_batches()
        return batches[0] if batches else pa.RecordBatch.from_arrays([], schema=arrow_obj.schema)

    def clear(self) -> None:
        """Remove all buffered rows."""

        self._rows.clear()
