"""Configuration objects for dora-pydantic-arrow."""

from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True)
class DAConfig:
    """Configuration controlling Arrow conversion behaviour."""

    datetime_policy: Literal["normalize_utc", "preserve_tz", "error_on_naive"] = "normalize_utc"
    enum_encoding: Literal["auto"] = "auto"
    dict_key_policy: Literal["string_only"] = "string_only"
    union_encoding: Literal["tagged_struct", "arrow_dense_union"] = "tagged_struct"
    decimal_precision: int = 38
    decimal_scale: int = 9
    ndarray_encoding: Literal["nested_list", "fixed_size_list_if_static"] = "nested_list"
    fast_path_skip_validation: bool = False
