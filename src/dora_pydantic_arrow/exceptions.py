"""Custom exceptions raised by dora-pydantic-arrow."""


class SchemaMismatchError(RuntimeError):
    """Raised when Arrow data does not match the expected Pydantic schema."""


class UnsupportedTypeError(TypeError):
    """Raised when attempting to map an unsupported Python type to Arrow."""
