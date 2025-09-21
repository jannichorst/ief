"""Utilities for loading pipeline configurations from declarative specs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence, Union

from .pipeline import Pipeline, PipelineEdge, PipelineNode, Task


@dataclass(frozen=True)
class RunConfig:
    """Configuration for executing a pipeline run."""

    seed: Optional[int] = None
    batch_size: Optional[int] = None
    capture: Mapping[str, Any] = field(default_factory=dict)
    eval: Mapping[str, Any] = field(default_factory=dict)
    extras: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LoadedPipeline:
    """Bundle of a configured :class:`Pipeline` and its run configuration."""

    pipeline: Pipeline
    run: RunConfig
    version: Optional[str] = None


TaskFactory = Union[Callable[[Mapping[str, Any]], Task], Task]


def load_pipeline_from_path(path: str | Path, registry: Mapping[str, TaskFactory]) -> LoadedPipeline:
    """Load a pipeline configuration from a JSON or YAML file."""

    path = Path(path)
    data = _load_raw_data(path)
    return load_pipeline_from_dict(data, registry)


def load_pipeline_from_dict(data: Mapping[str, Any], registry: Mapping[str, TaskFactory]) -> LoadedPipeline:
    """Validate *data* and build a :class:`Pipeline` instance."""

    if not isinstance(data, Mapping):
        raise ValueError("Pipeline configuration must be a mapping")

    pipeline_block = data.get("pipeline")
    if not isinstance(pipeline_block, Mapping):
        raise ValueError("Configuration must include a 'pipeline' mapping")

    node_defs = pipeline_block.get("nodes")
    if not isinstance(node_defs, Sequence) or not node_defs:
        raise ValueError("'pipeline.nodes' must be a non-empty list")

    edge_defs = pipeline_block.get("edges", [])
    if not isinstance(edge_defs, Sequence):
        raise ValueError("'pipeline.edges' must be a list if provided")

    nodes: list[PipelineNode] = []
    for raw_node in node_defs:
        if not isinstance(raw_node, Mapping):
            raise ValueError("Each pipeline node must be a mapping")
        node_id = raw_node.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("Pipeline nodes require a non-empty string 'id'")
        uses = raw_node.get("uses")
        if not isinstance(uses, str) or not uses:
            raise ValueError(f"Node '{node_id}' is missing a valid 'uses' entry")
        params = raw_node.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, Mapping):
            raise ValueError(f"Node '{node_id}' params must be a mapping if provided")

        task_factory = registry.get(uses)
        if task_factory is None:
            raise ValueError(f"Unknown task registry id '{uses}' for node '{node_id}'")

        task = _instantiate_task(task_factory, params)
        if not isinstance(task, Task):
            raise ValueError(f"Registry entry '{uses}' did not produce a valid Task")

        nodes.append(PipelineNode(id=node_id, task=task, params=dict(params)))

    node_ids = {node.id for node in nodes}
    edges: list[PipelineEdge] = []
    for raw_edge in edge_defs:
        edge = _parse_edge(raw_edge)
        if edge.producer_node != Pipeline.INPUT_NODE_ID and edge.producer_node not in node_ids:
            raise ValueError(
                f"Edge references unknown producer '{edge.producer_node}'"
            )
        if edge.consumer_node not in node_ids:
            raise ValueError(
                f"Edge references unknown consumer '{edge.consumer_node}'"
            )
        edges.append(edge)

    run_config = _parse_run_block(data.get("run", {}))
    version = data.get("version")
    if version is not None and not isinstance(version, (str, int, float)):
        raise ValueError("'version' must be a string or number if provided")

    pipeline = Pipeline(nodes=nodes, edges=edges)
    return LoadedPipeline(pipeline=pipeline, run=run_config, version=str(version) if version is not None else None)


def _instantiate_task(task_factory: TaskFactory, params: Mapping[str, Any]) -> Task:
    if isinstance(task_factory, Task):
        return task_factory
    if not callable(task_factory):
        raise ValueError("Registry entries must be callables or Task instances")
    try:
        task = task_factory(params)
    except TypeError:
        try:
            task = task_factory()  # type: ignore[call-arg]
        except TypeError as exc:
            raise ValueError("Task factory does not accept the provided params") from exc
    return task


def _parse_edge(raw_edge: Any) -> PipelineEdge:
    if isinstance(raw_edge, PipelineEdge):
        return raw_edge
    if isinstance(raw_edge, str):
        parts = raw_edge.split("->")
        if len(parts) != 2:
            raise ValueError(f"Edge specification '{raw_edge}' is not of the form 'node:type -> node'")
        left, right = parts
        left = left.strip()
        right = right.strip()
        if not right:
            raise ValueError(f"Edge specification '{raw_edge}' is missing a consumer node")
        producer_parts = left.split(":", 1)
        if len(producer_parts) != 2:
            raise ValueError(
                f"Edge specification '{raw_edge}' must include 'producer:ArtifactType'"
            )
        producer, artifact_type = producer_parts[0].strip(), producer_parts[1].strip()
        if not producer or not artifact_type:
            raise ValueError(f"Edge specification '{raw_edge}' is missing identifiers")
        return PipelineEdge(producer_node=producer, output_type=artifact_type, consumer_node=right)
    if isinstance(raw_edge, Mapping):
        try:
            producer = raw_edge["producer"]
            artifact_type = raw_edge["type"]
            consumer = raw_edge["consumer"]
        except KeyError as exc:
            raise ValueError("Edge mappings must include 'producer', 'type', and 'consumer'") from exc
        if not isinstance(producer, str) or not isinstance(artifact_type, str) or not isinstance(consumer, str):
            raise ValueError("Edge mapping entries must be strings")
        return PipelineEdge(producer_node=producer, output_type=artifact_type, consumer_node=consumer)
    raise ValueError(f"Unsupported edge specification: {raw_edge!r}")


def _parse_run_block(raw_run: Any) -> RunConfig:
    if raw_run is None:
        raw_run = {}
    if not isinstance(raw_run, Mapping):
        raise ValueError("'run' block must be a mapping if provided")

    seed = raw_run.get("seed")
    if seed is not None and not isinstance(seed, int):
        raise ValueError("run.seed must be an integer if provided")

    batch_size = raw_run.get("batch_size")
    if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
        raise ValueError("run.batch_size must be a positive integer if provided")

    capture = raw_run.get("capture", {})
    if capture is None:
        capture = {}
    if not isinstance(capture, Mapping):
        raise ValueError("run.capture must be a mapping if provided")

    eval_block = raw_run.get("eval", {})
    if eval_block is None:
        eval_block = {}
    if not isinstance(eval_block, Mapping):
        raise ValueError("run.eval must be a mapping if provided")

    extras = {
        key: value
        for key, value in raw_run.items()
        if key not in {"seed", "batch_size", "capture", "eval"}
    }

    return RunConfig(
        seed=seed,
        batch_size=batch_size,
        capture=dict(capture),
        eval=dict(eval_block),
        extras=dict(extras),
    )


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
