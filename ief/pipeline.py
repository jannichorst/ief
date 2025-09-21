"""Pipeline orchestration primitives for the lean-core IE reference implementation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, Mapping, MutableMapping, Protocol, Sequence, Tuple, runtime_checkable

from .exceptions import PipelineError, RegistryError
from .registry import TaskRegistry
from .store import ArtifactStore
from .tasks import RunContext, TaskResult

INPUT_NODE_ID = "__input__"


@dataclass
class Artifact:
    """Typed payload that flows between pipeline tasks."""

    id: str
    type: str
    doc_id: str | None = None
    data: Any = None
    parents: Sequence[str] = field(default_factory=tuple)
    meta: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class Task(Protocol):
    """Protocol describing the runtime interface expected from tasks."""

    id: str
    input_types: Sequence[str]
    output_types: Sequence[str]

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None = None,
    ) -> TaskResult:
        ...


@dataclass
class PipelineNode:
    """Concrete task configured as part of the pipeline DAG."""

    id: str
    task: Task
    params: Mapping[str, Any] = field(default_factory=dict)
    uses: str | None = None


@dataclass(frozen=True)
class PipelineEdge:
    """Typed connection between two nodes in the pipeline."""

    producer_node: str
    output_type: str
    consumer_node: str


@dataclass
class NodeRunRecord:
    """Metadata captured for each node execution during a pipeline run."""

    id: str
    uses: str
    params: Mapping[str, Any]
    config_hash: str | None
    started_at: datetime
    ended_at: datetime
    metrics: Mapping[str, Any]
    model_id: str | None = None
    warnings: Sequence[str] = field(default_factory=tuple)

    @property
    def latency_ms(self) -> float:
        return (self.ended_at - self.started_at).total_seconds() * 1000.0


@dataclass
class RunManifest:
    """Aggregate metadata describing a pipeline run."""

    run_id: str
    seed: int
    started_at: datetime
    ended_at: datetime | None = None
    nodes: MutableMapping[str, NodeRunRecord] = field(default_factory=dict)
    inputs: Dict[str, list[str]] = field(default_factory=dict)
    environment: Dict[str, Any] = field(default_factory=dict)
    hardware: Dict[str, Any] = field(default_factory=dict)
    batch_size: int | None = None
    capture: Mapping[str, Any] | None = None
    eval: Mapping[str, Any] | None = None
    extras: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seed": self.seed,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "batch_size": self.batch_size,
            "inputs": {atype: list(ids) for atype, ids in self.inputs.items()},
            "environment": dict(self.environment),
            "hardware": dict(self.hardware),
            "nodes": {
                node_id: {
                    "uses": record.uses,
                    "params": dict(record.params),
                    "config_hash": record.config_hash,
                    "started_at": record.started_at.isoformat(),
                    "ended_at": record.ended_at.isoformat(),
                    "metrics": dict(record.metrics),
                    "model_id": record.model_id,
                    "warnings": list(record.warnings),
                }
                for node_id, record in self.nodes.items()
            },
            "capture": dict(self.capture) if self.capture else None,
            "eval": dict(self.eval) if self.eval else None,
            "extras": dict(self.extras),
        }


@dataclass
class PipelineRunResult:
    """Container for artifacts and metrics produced during a pipeline run."""

    run_id: str
    node_results: Mapping[str, TaskResult]
    artifacts_by_node: Mapping[str, Mapping[str, Tuple[Artifact, ...]]]
    started_at: datetime
    ended_at: datetime
    artifact_store: ArtifactStore
    manifest: RunManifest

    def __post_init__(self) -> None:
        self.node_results = dict(self.node_results)
        self.artifacts_by_node = {
            node_id: dict(artifact_map)
            for node_id, artifact_map in self.artifacts_by_node.items()
        }

    def artifacts_for(self, node_id: str, artifact_type: str) -> Tuple[Artifact, ...]:
        """Return artifacts produced by *node_id* of the given *artifact_type*."""

        return self.artifacts_by_node.get(node_id, {}).get(artifact_type, ())

    @property
    def metrics(self) -> Dict[str, Any]:
        """Flatten per-node metric mappings into a single dictionary."""

        aggregated: Dict[str, Any] = {}
        for node_id, result in self.node_results.items():
            for key, value in result.metrics.items():
                aggregated[f"{node_id}.{key}"] = value
        return aggregated


class Pipeline:
    """Execute a directed acyclic graph of tasks with typed edges."""

    INPUT_NODE_ID = INPUT_NODE_ID

    def __init__(
        self,
        nodes: Sequence[PipelineNode],
        edges: Sequence[PipelineEdge],
        *,
        version: str | None = None,
        run_defaults: Mapping[str, Any] | None = None,
    ) -> None:
        if not isinstance(nodes, Sequence) or not nodes:
            raise PipelineError("Pipeline requires a non-empty sequence of nodes")

        self._node_map: Dict[str, PipelineNode] = {}
        ordered_nodes: list[PipelineNode] = []
        for node in nodes:
            if node.id in self._node_map:
                raise PipelineError(f"Duplicate node identifier {node.id!r}")
            self._node_map[node.id] = node
            ordered_nodes.append(node)
        self._nodes: Tuple[PipelineNode, ...] = tuple(ordered_nodes)

        self._edges: Tuple[PipelineEdge, ...] = tuple(edges)
        self._incoming: MutableMapping[str, list[PipelineEdge]] = {node.id: [] for node in self._nodes}
        self._outgoing: MutableMapping[str, list[str]] = {node.id: [] for node in self._nodes}

        for edge in self._edges:
            if edge.consumer_node not in self._node_map:
                raise PipelineError(f"Edge references unknown consumer {edge.consumer_node!r}")
            if edge.producer_node != self.INPUT_NODE_ID and edge.producer_node not in self._node_map:
                raise PipelineError(f"Edge references unknown producer {edge.producer_node!r}")
            self._incoming.setdefault(edge.consumer_node, []).append(edge)
            if edge.producer_node != self.INPUT_NODE_ID:
                self._outgoing.setdefault(edge.producer_node, []).append(edge.consumer_node)

        self._order: Tuple[str, ...] = self._compute_topological_order()
        self.version = version

        defaults = dict(run_defaults or {})
        self._default_seed: int | None = defaults.get("seed")
        self._default_batch_size: int | None = defaults.get("batch_size")
        self._default_capture: Mapping[str, Any] | None = (
            dict(defaults["capture"]) if "capture" in defaults and defaults["capture"] is not None else None
        )
        self._default_eval: Mapping[str, Any] | None = (
            dict(defaults["eval"]) if "eval" in defaults and defaults["eval"] is not None else None
        )
        extras = defaults.get("extras") or {}
        if extras is None:
            extras = {}
        if not isinstance(extras, Mapping):
            raise PipelineError("run_defaults.extras must be a mapping if provided")
        self._default_extras: Dict[str, Any] = dict(extras)

    @property
    def nodes(self) -> Tuple[PipelineNode, ...]:
        return self._nodes

    @property
    def edges(self) -> Tuple[PipelineEdge, ...]:
        return self._edges

    def get_node(self, node_id: str) -> PipelineNode:
        try:
            return self._node_map[node_id]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise PipelineError(f"Unknown pipeline node {node_id!r}") from exc

    def _compute_topological_order(self) -> Tuple[str, ...]:
        indegree: Dict[str, int] = {node_id: 0 for node_id in self._node_map}
        for edge in self._edges:
            if edge.producer_node == self.INPUT_NODE_ID:
                continue
            indegree[edge.consumer_node] += 1

        queue = deque(node_id for node_id, deg in indegree.items() if deg == 0)
        order: list[str] = []
        indegree = dict(indegree)
        while queue:
            node_id = queue.popleft()
            order.append(node_id)
            for downstream in self._outgoing.get(node_id, []):
                indegree[downstream] -= 1
                if indegree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(self._node_map):
            raise PipelineError("Pipeline contains a cycle")
        return tuple(order)

    def run(
        self,
        initial_artifacts: Mapping[str, Sequence[Artifact]] | Sequence[Artifact] | None = None,
        *,
        seed: int | None = None,
        params_override: Mapping[str, Mapping[str, Any]] | None = None,
        extras: Mapping[str, Any] | None = None,
    ) -> PipelineRunResult:
        started_at = datetime.now(timezone.utc)
        base_seed = (
            seed
            if seed is not None
            else (
                self._default_seed
                if self._default_seed is not None
                else int(started_at.timestamp())
            )
        )
        run_id = hashlib.sha1(f"{base_seed}-{started_at.isoformat()}".encode("utf-8")).hexdigest()
        manifest = RunManifest(
            run_id=run_id,
            seed=base_seed,
            started_at=started_at,
            batch_size=self._default_batch_size,
            capture=dict(self._default_capture) if self._default_capture else None,
            eval=dict(self._default_eval) if self._default_eval else None,
            extras=dict(self._default_extras),
        )
        if extras:
            extras_dict = dict(extras)
            manifest.extras.update(extras_dict)
            environment = extras_dict.get("environment")
            if isinstance(environment, Mapping):
                manifest.environment.update(environment)
            hardware = extras_dict.get("hardware")
            if isinstance(hardware, Mapping):
                manifest.hardware.update(hardware)

        store = ArtifactStore()
        outputs_by_node: Dict[str, Dict[str, list[Artifact]]] = {}

        def node_outputs(node_id: str) -> Dict[str, list[Artifact]]:
            return outputs_by_node.setdefault(node_id, {})

        if initial_artifacts:
            input_store = node_outputs(self.INPUT_NODE_ID)
            if isinstance(initial_artifacts, Mapping):
                for artifact_type, artifacts in initial_artifacts.items():
                    artifacts_list = list(artifacts)
                    input_store.setdefault(artifact_type, []).extend(artifacts_list)
                    store.add_many(artifacts_list, producer_id=None)
            else:
                artifacts_list = list(initial_artifacts)
                for artifact in artifacts_list:
                    input_store.setdefault(artifact.type, []).append(artifact)
                store.add_many(artifacts_list, producer_id=None)
            manifest.inputs = {
                artifact_type: [artifact.id for artifact in artifacts]
                for artifact_type, artifacts in input_store.items()
            }

        overrides = {node_id: dict(params) for node_id, params in (params_override or {}).items()}
        node_results: Dict[str, TaskResult] = {}

        for index, node_id in enumerate(self._order):
            node = self._node_map[node_id]
            task = node.task
            node_params = dict(node.params)
            if node_id in overrides:
                node_params.update(overrides[node_id])

            if hasattr(task, "validate_params"):
                try:
                    node_params = dict(task.validate_params(node_params))
                except Exception:  # pragma: no cover - best effort validation
                    pass

            incoming_edges = self._incoming.get(node_id, [])
            inputs: Dict[str, list[Artifact]] = {}
            if incoming_edges:
                for edge in incoming_edges:
                    producer = None if edge.producer_node == self.INPUT_NODE_ID else edge.producer_node
                    inputs.setdefault(edge.output_type, []).extend(
                        store.get_from(producer, edge.output_type)
                    )
            for artifact_type in getattr(task, "input_types", []):
                inputs.setdefault(artifact_type, [])

            frozen_inputs = {atype: tuple(values) for atype, values in inputs.items()}

            ctx: RunContext | None = None
            try:
                ctx = RunContext(
                    run_id=run_id,
                    node_id=node_id,
                    capability=node.uses or getattr(task, "id", node_id),
                    seed=base_seed + index,
                    store=store,
                    extras=extras,
                )
            except Exception:  # pragma: no cover - fallback for lightweight tasks
                ctx = None

            result = self._execute_task(task, frozen_inputs, node_params, ctx)
            if not isinstance(result, TaskResult):
                raise PipelineError(
                    f"Task {getattr(task, 'id', node_id)!r} returned invalid result type {type(result)!r}"
                )

            if ctx is not None and getattr(result, "trace", None) is None and hasattr(task, "make_trace"):
                try:
                    result.trace = task.make_trace(ctx, node_params, frozen_inputs, result)  # type: ignore[attr-defined]
                except Exception:  # pragma: no cover - optional feature
                    pass

            node_results[node_id] = result
            output_store = node_outputs(node_id)
            for artifact in result.artifacts:
                output_store.setdefault(artifact.type, []).append(artifact)
                store.add(artifact, producer_id=node_id)

            record_started_at = (
                getattr(result.trace, "started_at", None)
                if getattr(result, "trace", None) is not None
                else getattr(ctx, "started_at", datetime.now(timezone.utc))
            )
            record_ended_at = (
                getattr(result.trace, "ended_at", None)
                if getattr(result, "trace", None) is not None
                else datetime.now(timezone.utc)
            )
            config_hash = None
            if getattr(result, "trace", None) is not None:
                config_hash = getattr(result.trace, "config_hash", None)
            elif hasattr(task, "config_hash"):
                try:
                    config_hash = task.config_hash(node_params)
                except Exception:  # pragma: no cover - fallback hashing
                    config_hash = None
            if config_hash is None:
                try:
                    normalised = json.dumps(node_params, sort_keys=True, default=str)
                except TypeError:
                    normalised = repr(sorted(node_params.items()))
                config_hash = hashlib.sha256(normalised.encode("utf-8")).hexdigest()

            warnings: Sequence[str]
            if getattr(result, "trace", None) is not None:
                warnings = tuple(getattr(result.trace, "warnings", ()))
            elif ctx is not None and hasattr(ctx, "get_warnings"):
                try:
                    warnings = tuple(ctx.get_warnings())
                except Exception:  # pragma: no cover - defensive fallback
                    warnings = ()
            else:
                warnings = ()

            manifest.nodes[node_id] = NodeRunRecord(
                id=node_id,
                uses=node.uses or getattr(task, "id", node_id),
                params=dict(node_params),
                config_hash=config_hash,
                started_at=record_started_at,
                ended_at=record_ended_at,
                metrics=dict(result.metrics),
                model_id=(
                    getattr(result.trace, "model_id", None)
                    if getattr(result, "trace", None) is not None
                    else getattr(task, "default_model_id", None)
                ),
                warnings=warnings,
            )

        ended_at = datetime.now(timezone.utc)
        manifest.ended_at = ended_at

        artifacts_by_node: Dict[str, Dict[str, Tuple[Artifact, ...]]] = {}
        for node_id, outputs in outputs_by_node.items():
            if node_id == self.INPUT_NODE_ID:
                continue
            artifacts_by_node[node_id] = {
                artifact_type: tuple(artifacts)
                for artifact_type, artifacts in outputs.items()
            }
        for node_id in self._node_map:
            artifacts_by_node.setdefault(node_id, {})

        return PipelineRunResult(
            run_id=run_id,
            node_results=node_results,
            artifacts_by_node=artifacts_by_node,
            started_at=started_at,
            ended_at=ended_at,
            artifact_store=store,
            manifest=manifest,
        )

    @staticmethod
    def _execute_task(
        task: Task,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        try:
            return task.run(inputs=inputs, params=params, ctx=ctx)
        except TypeError as exc:
            if "unexpected keyword argument" in str(exc) and "'ctx'" in str(exc):
                return task.run(inputs=inputs, params=params)
            raise

    @classmethod
    def from_config_dict(cls, raw: Mapping[str, Any], registry: TaskRegistry) -> "Pipeline":
        if not isinstance(raw, Mapping):
            raise PipelineError("Configuration must be a mapping")

        version = raw.get("version")
        pipeline_block = raw.get("pipeline")
        if not isinstance(pipeline_block, Mapping):
            raise PipelineError("Configuration must include a 'pipeline' mapping")

        node_defs = pipeline_block.get("nodes")
        if not isinstance(node_defs, Sequence) or not node_defs:
            raise PipelineError("'pipeline.nodes' must be a non-empty list")

        edge_defs = pipeline_block.get("edges", [])
        if not isinstance(edge_defs, Sequence):
            raise PipelineError("'pipeline.edges' must be a list if provided")

        nodes: list[PipelineNode] = []
        for raw_node in node_defs:
            if not isinstance(raw_node, Mapping):
                raise PipelineError("Each pipeline node must be a mapping")
            node_id = raw_node.get("id")
            if not isinstance(node_id, str) or not node_id:
                raise PipelineError("Pipeline nodes require a non-empty string 'id'")
            uses = raw_node.get("uses")
            if not isinstance(uses, str) or not uses:
                raise PipelineError(f"Node '{node_id}' is missing a valid 'uses' entry")
            params = raw_node.get("params", {})
            if params is None:
                params = {}
            if not isinstance(params, Mapping):
                raise PipelineError(f"Node '{node_id}' params must be a mapping if provided")
            if uses not in registry:
                raise RegistryError(f"Capability {uses!r} not registered")
            task = registry.create(uses)
            nodes.append(PipelineNode(id=node_id, task=task, params=dict(params), uses=uses))

        edges: list[PipelineEdge] = []
        for raw_edge in edge_defs:
            edges.append(cls._parse_edge(raw_edge))

        run_defaults: Dict[str, Any] = {}
        run_block = raw.get("run")
        if run_block is not None:
            if not isinstance(run_block, Mapping):
                raise PipelineError("'run' section must be a mapping if provided")
            seed = run_block.get("seed")
            if seed is not None:
                if not isinstance(seed, int):
                    raise PipelineError("run.seed must be an integer if provided")
                run_defaults["seed"] = seed
            batch_size = run_block.get("batch_size")
            if batch_size is not None:
                if not isinstance(batch_size, int) or batch_size <= 0:
                    raise PipelineError("run.batch_size must be a positive integer if provided")
                run_defaults["batch_size"] = batch_size
            capture = run_block.get("capture")
            if capture is not None:
                if not isinstance(capture, Mapping):
                    raise PipelineError("run.capture must be a mapping if provided")
                run_defaults["capture"] = dict(capture)
            eval_block = run_block.get("eval")
            if eval_block is not None:
                if not isinstance(eval_block, Mapping):
                    raise PipelineError("run.eval must be a mapping if provided")
                run_defaults["eval"] = dict(eval_block)
            extras = {
                key: value
                for key, value in run_block.items()
                if key not in {"seed", "batch_size", "capture", "eval"}
            }
            if extras:
                run_defaults["extras"] = extras

        return cls(
            nodes=nodes,
            edges=edges,
            version=str(version) if version is not None else None,
            run_defaults=run_defaults,
        )

    @staticmethod
    def _parse_edge(raw_edge: Any) -> PipelineEdge:
        if isinstance(raw_edge, PipelineEdge):
            return raw_edge
        if isinstance(raw_edge, str):
            parts = raw_edge.split("->")
            if len(parts) != 2:
                raise PipelineError(
                    f"Edge specification '{raw_edge}' is not of the form 'node:Type -> node'"
                )
            left, right = parts
            left = left.strip()
            right = right.strip()
            if not right:
                raise PipelineError(f"Edge specification '{raw_edge}' is missing a consumer node")
            producer_parts = left.split(":", 1)
            if len(producer_parts) != 2:
                raise PipelineError(
                    f"Edge specification '{raw_edge}' must include 'producer:ArtifactType'"
                )
            producer, artifact_type = producer_parts[0].strip(), producer_parts[1].strip()
            if not producer or not artifact_type:
                raise PipelineError(f"Edge specification '{raw_edge}' is missing identifiers")
            return PipelineEdge(producer_node=producer, output_type=artifact_type, consumer_node=right)
        if isinstance(raw_edge, Mapping):
            try:
                producer = raw_edge["producer"]
                artifact_type = raw_edge["type"]
                consumer = raw_edge["consumer"]
            except KeyError as exc:
                raise PipelineError(
                    "Edge mappings must include 'producer', 'type', and 'consumer'"
                ) from exc
            if not isinstance(producer, str) or not isinstance(artifact_type, str) or not isinstance(consumer, str):
                raise PipelineError("Edge mapping entries must be strings")
            return PipelineEdge(producer_node=producer, output_type=artifact_type, consumer_node=consumer)
        raise PipelineError(f"Unsupported edge specification: {raw_edge!r}")
