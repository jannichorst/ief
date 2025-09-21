from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .pipeline import Pipeline, RunConfig
from .registry import TaskRegistry
from .tasks import Task

TaskFactory = Callable[[], Task] | type[Task]


@dataclass(frozen=True)
class LoadedPipeline:
    """Bundle of a configured :class:`Pipeline` and its run configuration."""

    pipeline: Pipeline
    run: RunConfig
    version: str


def load_pipeline_from_path(
    path: str | Path, registry: TaskRegistry | Mapping[str, TaskFactory]
) -> LoadedPipeline:
    """Load a pipeline configuration from a JSON or YAML file."""

    file_path = Path(path)
    data = _load_raw_data(file_path)
    return load_pipeline_from_dict(data, registry)


def load_pipeline_from_dict(
    data: Mapping[str, Any], registry: TaskRegistry | Mapping[str, TaskFactory]
) -> LoadedPipeline:
    """Validate *data* and build a :class:`Pipeline` instance."""

    if not isinstance(data, Mapping):
        raise ValueError("Pipeline configuration must be a mapping")
    registry_obj = _ensure_registry(registry)
    pipeline = Pipeline.from_config_dict(data, registry_obj)
    config = pipeline.config
    return LoadedPipeline(pipeline=pipeline, run=config.run, version=config.version)


def _ensure_registry(
    registry: TaskRegistry | Mapping[str, TaskFactory]
) -> TaskRegistry:
    if isinstance(registry, TaskRegistry):
        return registry
    registry_obj = TaskRegistry()
    for capability, factory in registry.items():
        registry_obj.register(capability, factory)
    return registry_obj


def _load_raw_data(path: Path) -> Mapping[str, Any]:
    text = path.read_text()
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonc"}:
        return json.loads(text)
    if suffix in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ValueError("PyYAML is required to load YAML configuration files") from exc
        return yaml.safe_load(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Unsupported configuration format for '{path}'") from exc
