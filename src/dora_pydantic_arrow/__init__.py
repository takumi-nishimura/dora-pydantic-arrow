"""
dora-pydantic-arrow: Pydantic to/from Apache Arrow Conversion
"""
from .main import to_arrow, from_arrow
from .schema import schema_from_model
from .config import DAConfig
from .errors import SchemaMismatchError, UnsupportedTypeError, DoraPydanticArrowError
from pydantic import ValidationError

__all__ = [
    "to_arrow",
    "from_arrow",
    "schema_from_model",
    "DAConfig",
    "SchemaMismatchError",
    "UnsupportedTypeError",
    "DoraPydanticArrowError",
    "ValidationError", # Re-export for convenience
]