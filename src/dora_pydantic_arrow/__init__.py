"""Public API for the dora-pydantic-arrow package."""

from .batch import BatchBuilder
from .config import DAConfig
from .conversion import from_arrow, to_arrow
from .schema import schema_from_model

__all__ = [
    "BatchBuilder",
    "DAConfig",
    "from_arrow",
    "schema_from_model",
    "to_arrow",
]


def main() -> None:  # pragma: no cover - CLI helper
    """Entry-point used by the console script stub."""

    print("dora-pydantic-arrow provides conversion helpers between Pydantic and Arrow.")
