"""Lightweight subset of the Pydantic API used for the tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, get_type_hints

__all__ = [
    "BaseModel",
    "ValidationError",
    "VERSION",
]

VERSION = "2.0.0"


class ValidationError(Exception):
    """Raised when data cannot be validated against a model."""


@dataclass
class ModelField:
    name: str
    annotation: Any
    default: Any = ...


class BaseModel:
    model_fields: Dict[str, ModelField] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        annotations = get_type_hints(cls, include_extras=True)
        cls.model_fields = {}
        for name, annotation in annotations.items():
            if hasattr(BaseModel, name):
                continue
            default = getattr(cls, name, ...)
            cls.model_fields[name] = ModelField(name, annotation, default)

    def __init__(self, **data: Any) -> None:
        for name, field in self.model_fields.items():
            if name in data:
                value = data[name]
            elif field.default is not ...:
                value = field.default
            else:
                raise ValidationError(f"Missing value for field '{name}'")
            setattr(self, name, value)
        extra_keys = set(data) - set(self.model_fields)
        if extra_keys:
            raise ValidationError(f"Unexpected fields: {sorted(extra_keys)}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseModel):
            return NotImplemented
        return type(self) is type(other) and self.model_dump(mode="python") == other.model_dump(mode="python")

    @classmethod
    def model_validate(cls, data: Mapping[str, Any]) -> "BaseModel":
        if not isinstance(data, Mapping):
            raise ValidationError("model_validate expects a mapping")
        return cls(**data)

    @classmethod
    def model_construct(cls, **data: Any) -> "BaseModel":
        obj = cls.__new__(cls)
        for name in cls.model_fields:
            if name in data:
                setattr(obj, name, data[name])
            else:
                raise ValidationError(f"Missing value for field '{name}' in construct")
        return obj

    def model_dump(self, *, mode: str = "python") -> Dict[str, Any]:
        return {name: getattr(self, name) for name in self.model_fields}

    @classmethod
    def model_json_schema(cls) -> Dict[str, Any]:
        properties = {name: {"title": name} for name in cls.model_fields}
        required = [name for name, field in cls.model_fields.items() if field.default is ...]
        return {
            "title": cls.__name__,
            "type": "object",
            "properties": properties,
            "required": required,
        }
