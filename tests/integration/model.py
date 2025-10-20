from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

import uuid_utils
from pydantic import UUID7, BaseModel, Field


class StrKind(Enum):
    EXAMPLE = "example"
    ANOTHER = "another"


class IntKind(Enum):
    FIRST = 1
    SECOND = 2


class ExampleModel(BaseModel):
    id: UUID7 = Field(default_factory=lambda: UUID(bytes=uuid_utils.uuid7().bytes))
    str_kind: StrKind
    int_kind: IntKind
    payload: Optional[Dict[str, Any]] = None
    optional_field: str | None = None
