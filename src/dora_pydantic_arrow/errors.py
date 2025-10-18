class DoraPydanticArrowError(Exception):
    """Base exception for the library."""
    pass

class SchemaMismatchError(DoraPydanticArrowError):
    """Raised when Arrow and Pydantic schemas do not match."""
    pass

class UnsupportedTypeError(DoraPydanticArrowError):
    """Raised when a Python or Pydantic type is not supported."""
    pass

# Pydantic's ValidationError will be re-exported or used directly.