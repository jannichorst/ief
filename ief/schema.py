"""Light-weight JSON schema validation tailored to the task parameter use-case."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict

from .exceptions import SchemaValidationError

_JSON_TYPE_TO_PY = {
    "string": str,
    "number": (int, float),
    "integer": int,
    "boolean": bool,
    "object": Mapping,
    "array": Sequence,
    "null": type(None),
}


def _ensure_number(value: Any, schema_type: str, path: str) -> None:
    if schema_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SchemaValidationError(f"Expected number at {path!r}, got {type(value).__name__}")
    elif schema_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise SchemaValidationError(f"Expected integer at {path!r}, got {type(value).__name__}")


def _assert_type(value: Any, schema_type: str | Sequence[str], path: str) -> None:
    if isinstance(schema_type, str):
        expected = _JSON_TYPE_TO_PY.get(schema_type)
        if expected is None:
            raise SchemaValidationError(f"Unsupported schema type {schema_type!r} at {path!r}")
        if schema_type in {"number", "integer"}:
            _ensure_number(value, schema_type, path)
            return
        if not isinstance(value, expected):
            raise SchemaValidationError(
                f"Expected {schema_type} at {path!r}, got {type(value).__name__}"
            )
    else:
        for option in schema_type:
            try:
                _assert_type(value, option, path)
                return
            except SchemaValidationError:
                continue
        raise SchemaValidationError(
            f"Value at {path!r} does not match any allowed type {list(schema_type)!r}"
        )


def _apply_common_checks(schema: Mapping[str, Any], value: Any, path: str) -> None:
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(
            f"Value {value!r} at {path!r} not in enumeration {schema['enum']!r}"
        )
    if "const" in schema and value != schema["const"]:
        raise SchemaValidationError(
            f"Value {value!r} at {path!r} does not equal required constant {schema['const']!r}"
        )
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        exclusive_minimum = schema.get("exclusiveMinimum")
        exclusive_maximum = schema.get("exclusiveMaximum")
        if minimum is not None and value < minimum:
            raise SchemaValidationError(f"Value {value!r} at {path!r} below minimum {minimum!r}")
        if maximum is not None and value > maximum:
            raise SchemaValidationError(f"Value {value!r} at {path!r} above maximum {maximum!r}")
        if exclusive_minimum is not None and value <= exclusive_minimum:
            raise SchemaValidationError(
                f"Value {value!r} at {path!r} not greater than {exclusive_minimum!r}"
            )
        if exclusive_maximum is not None and value >= exclusive_maximum:
            raise SchemaValidationError(
                f"Value {value!r} at {path!r} not less than {exclusive_maximum!r}"
            )


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _apply_schema(schema: Mapping[str, Any], value: Any, path: str) -> Any:
    schema_type = schema.get("type")
    if schema_type is not None:
        _assert_type(value, schema_type, path)
    _apply_common_checks(schema, value, path)

    if schema_type == "object" or (schema_type is None and isinstance(value, Mapping)):
        return _validate_object(schema, value, path)
    if schema_type == "array" or (schema_type is None and isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))):
        return _validate_array(schema, value, path)
    return value


def _validate_object(schema: Mapping[str, Any], value: Mapping[str, Any], path: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SchemaValidationError(f"Expected object at {path!r}")
    properties = schema.get("properties", {})
    required = set(schema.get("required", ()))
    additional = schema.get("additionalProperties", True)
    result: Dict[str, Any] = {}

    for key, prop_schema in properties.items():
        if key not in value and "default" in prop_schema:
            result[key] = prop_schema["default"]

    for key in required:
        if key not in value and key not in result:
            raise SchemaValidationError(f"Missing required property {key!r} at {path!r}")

    for key, child_value in value.items():
        if key in properties:
            result[key] = _apply_schema(properties[key], child_value, _join(path, key))
        else:
            if isinstance(additional, Mapping):
                result[key] = _apply_schema(additional, child_value, _join(path, key))
            elif additional is False:
                raise SchemaValidationError(f"Unexpected property {key!r} at {path!r}")
            else:
                result[key] = child_value
    return result


def _validate_array(schema: Mapping[str, Any], value: Sequence[Any], path: str) -> list[Any]:
    if isinstance(value, (str, bytes, bytearray)):
        raise SchemaValidationError(f"Expected array at {path!r}")
    result = list(value)
    items_schema = schema.get("items")
    min_items = schema.get("minItems")
    max_items = schema.get("maxItems")
    if min_items is not None and len(result) < min_items:
        raise SchemaValidationError(f"Array at {path!r} shorter than {min_items}")
    if max_items is not None and len(result) > max_items:
        raise SchemaValidationError(f"Array at {path!r} longer than {max_items}")
    if isinstance(items_schema, Mapping):
        for idx, item in enumerate(result):
            result[idx] = _apply_schema(items_schema, item, f"{path}[{idx}]")
    return result


def validate_params(params: Mapping[str, Any] | None, schema: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate ``params`` against ``schema`` and return a normalised copy."""

    params = params or {}
    if not isinstance(params, Mapping):
        raise SchemaValidationError("Task params must be a mapping")
    if not schema:
        return dict(params)
    return _apply_schema(schema, params, path="")
