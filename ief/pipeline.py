
"""Pipeline orchestration following the lean-core IE spec."""


from __future__ import annotations

from collections import defaultdict, deque

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence, Tuple

from .artifacts import Artifact
from .exceptions import PipelineError, RegistryError
from .registry import TaskRegistry
from .store import ArtifactStore
from .tasks import RunContext, TaskResult

_EDGE_PATTERN = re.compile(r"^(?P<src>[A-Za-z0-9_\-]+):(?P<atype>[A-Za-z0-9_\-]+)\s*->\s*(?P<dst>[A-Za-z0-9_\-]+)$")


@dataclass
class PipelineNode:
    id: str
    uses: str
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    source: str
    artifact_type: str
    target: str


@dataclass(frozen=True)
class CaptureConfig:
    """Configuration describing which artifacts/renders to persist."""

    store_artifacts: Tuple[str, ...] = ()
    store_renders: bool = False

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "CaptureConfig":
        if not isinstance(raw, Mapping):
            raise PipelineError("capture section must be a mapping")
        store_raw = raw.get("store_artifacts", ())
        if store_raw is None:
            store_artifacts: Tuple[str, ...] = ()
        elif isinstance(store_raw, Mapping) or isinstance(store_raw, (str, bytes, bytearray)):
            raise PipelineError("capture.store_artifacts must be a sequence of artifact type names")
        else:
            store_artifacts = tuple(str(name) for name in store_raw)
        store_renders = bool(raw.get("store_renders", False))
        return cls(store_artifacts=store_artifacts, store_renders=store_renders)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "store_artifacts": list(self.store_artifacts),
            "store_renders": self.store_renders,
        }


@dataclass(frozen=True)
class EvalConfig:
    """Configuration for evaluation datasets and metrics."""

    dataset: str | None = None
    metrics: Tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "EvalConfig":
        if not isinstance(raw, Mapping):
            raise PipelineError("eval section must be a mapping")
        dataset = raw.get("dataset")
        if dataset is not None and not isinstance(dataset, str):
            dataset = str(dataset)
        metrics_raw = raw.get("metrics", ())
        if metrics_raw is None:
            metrics: Tuple[str, ...] = ()
        elif isinstance(metrics_raw, Mapping) or isinstance(metrics_raw, (str, bytes, bytearray)):
            raise PipelineError("eval.metrics must be a sequence of metric identifiers")
        else:
            metrics = tuple(str(metric) for metric in metrics_raw)
        return cls(dataset=dataset, metrics=metrics)

    def as_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        if self.dataset is not None:
            data["dataset"] = self.dataset
        if self.metrics:
            data["metrics"] = list(self.metrics)
        return data


@dataclass(frozen=True)
class RunConfig:
    """Runtime controls applied when executing a pipeline run."""

    seed: int | None = None
    batch_size: int | None = None
    capture: CaptureConfig | None = None
    eval: EvalConfig | None = None
    extras: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Any) -> "RunConfig":
        if raw is None:
            return cls()
        if not isinstance(raw, Mapping):
            raise PipelineError("run section must be a mapping")
        seed = raw.get("seed")
        if seed is not None and not isinstance(seed, int):
            raise PipelineError("run.seed must be an integer")
        batch_size = raw.get("batch_size")
        if batch_size is not None:
            if not isinstance(batch_size, int) or batch_size <= 0:
                raise PipelineError("run.batch_size must be a positive integer")
        capture_raw = raw.get("capture")
        capture = CaptureConfig.from_mapping(capture_raw) if capture_raw is not None else None
        eval_raw = raw.get("eval")
        eval_cfg = EvalConfig.from_mapping(eval_raw) if eval_raw is not None else None
        extras = {
            key: value
            for key, value in raw.items()
            if key not in {"seed", "batch_size", "capture", "eval"}
        }
        return cls(seed=seed, batch_size=batch_size, capture=capture, eval=eval_cfg, extras=dict(extras))

    def as_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        if self.seed is not None:
            data["seed"] = self.seed
        if self.batch_size is not None:
            data["batch_size"] = self.batch_size
        if self.capture is not None:
            data["capture"] = self.capture.as_dict()
        if self.eval is not None:
            data["eval"] = self.eval.as_dict()
        if self.extras:
            data.update(self.extras)
        return data


@dataclass
class PipelineConfig:
    version: str
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    run: RunConfig = field(default_factory=RunConfig)


@dataclass
class NodeRunRecord:
    id: str
    uses: str
    params: Mapping[str, Any]
    config_hash: str
    started_at: datetime
    ended_at: datetime
    metrics: Mapping[str, float]
    model_id: str | None
    warnings: Sequence[str]

    @property
    def latency_ms(self) -> float:
        return (self.ended_at - self.started_at).total_seconds() * 1000.0


@dataclass
class RunManifest:
    run_id: str
    seed: int
    started_at: datetime
    ended_at: datetime | None = None
    nodes: MutableMapping[str, NodeRunRecord] = field(default_factory=dict)
    inputs: Dict[str, List[str]] = field(default_factory=dict)
    environment: Dict[str, Any] = field(default_factory=dict)
    hardware: Dict[str, Any] = field(default_factory=dict)
    batch_size: int | None = None
    capture: CaptureConfig | None = None
    eval: EvalConfig | None = None
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
                    "model_id": record.model_id,
                    "metrics": dict(record.metrics),
                    "warnings": list(record.warnings),
                    "started_at": record.started_at.isoformat(),
                    "ended_at": record.ended_at.isoformat(),
                    "latency_ms": record.latency_ms,
                }
                for node_id, record in self.nodes.items()
            },
            "capture": self.capture.as_dict() if self.capture else None,
            "eval": self.eval.as_dict() if self.eval else None,
            "extras": dict(self.extras),
        }
=======
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Sequence, runtime_checkable


class PipelineError(RuntimeError):
    """Raised when the pipeline definition or execution is invalid."""


@dataclass
class Artifact:
    """A minimal representation of a typed artifact flowing through the pipeline."""

    id: str
    type: str
    doc_id: Optional[str] = None
    data: Any = None
    parents: Sequence[str] = field(default_factory=tuple)
    meta: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    """Result returned by an individual task execution."""

    artifacts: Sequence[Artifact] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    trace: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class Task(Protocol):
    """Minimal protocol describing a runnable task within the pipeline."""

    id: str
    input_types: Sequence[str]
    output_types: Sequence[str]

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: Optional[Any] = None,
    ) -> TaskResult:
        ...


@dataclass
class PipelineNode:
    """A node in the pipeline DAG wrapping a configured task instance."""

    id: str
    task: Task
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineEdge:
    """A typed connection between two pipeline nodes."""

    producer_node: str
    output_type: str
    consumer_node: str



@dataclass
class PipelineRunResult:
    run_id: str
    artifact_store: ArtifactStore
    node_results: Mapping[str, TaskResult]
    manifest: RunManifest

    def get_artifacts(self, artifact_type: str) -> List[Artifact]:
        return self.artifact_store.get_global(artifact_type)

    @property
    def metrics(self) -> Dict[str, float]:
        aggregated: Dict[str, float] = {}
        for node_id, result in self.node_results.items():
            for key, value in result.metrics.items():
                aggregated[f"{node_id}.{key}"] = value
        return aggregated


class Pipeline:
    def __init__(self, config: PipelineConfig, registry: TaskRegistry) -> None:
        self.config = config
        self.registry = registry
        self._nodes: Dict[str, PipelineNode] = {node.id: node for node in config.nodes}
        if len(self._nodes) != len(config.nodes):
            raise PipelineError("Duplicate node identifiers in pipeline configuration")
        self._edges = list(config.edges)
        self._incoming: MutableMapping[str, List[PipelineEdge]] = defaultdict(list)
        self._outgoing: MutableMapping[str, List[PipelineEdge]] = defaultdict(list)
        for edge in self._edges:
            if edge.source not in self._nodes:
                raise PipelineError(f"Edge references unknown source node {edge.source!r}")
            if edge.target not in self._nodes:
                raise PipelineError(f"Edge references unknown target node {edge.target!r}")
            self._incoming[edge.target].append(edge)
            self._outgoing[edge.source].append(edge)
        self._order = self._topological_order()

    @classmethod
    def from_config_dict(cls, raw: Mapping[str, Any], registry: TaskRegistry) -> "Pipeline":
        try:
            version = raw["version"]
            pipeline = raw["pipeline"]
        except KeyError as exc:
            raise PipelineError("Configuration must include 'version' and 'pipeline'") from exc
        nodes = [PipelineNode(id=item["id"], uses=item["uses"], params=item.get("params", {})) for item in pipeline.get("nodes", [])]
        edges = [cls._parse_edge(item) for item in pipeline.get("edges", [])]
        run_cfg = RunConfig.from_mapping(raw.get("run"))
        cfg = PipelineConfig(version=version, nodes=nodes, edges=edges, run=run_cfg)
        return cls(cfg, registry)

    @staticmethod
    def _parse_edge(raw: Any) -> PipelineEdge:
        if isinstance(raw, Mapping):
            return PipelineEdge(source=raw["source"], artifact_type=raw["artifact_type"], target=raw["target"])
        if isinstance(raw, str):
            match = _EDGE_PATTERN.match(raw.strip())
            if not match:
                raise PipelineError(f"Invalid edge specification: {raw!r}")
            return PipelineEdge(
                source=match.group("src"),
                artifact_type=match.group("atype"),
                target=match.group("dst"),
            )
        raise PipelineError(f"Unsupported edge specification: {raw!r}")

    def _topological_order(self) -> List[str]:
        indegree: MutableMapping[str, int] = {node_id: 0 for node_id in self._nodes}
        for edge in self._edges:
            indegree[edge.target] += 1
        queue = deque([node_id for node_id, deg in indegree.items() if deg == 0])
        order: List[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for edge in self._outgoing.get(node, ()):  # type: ignore[arg-type]
                indegree[edge.target] -= 1
                if indegree[edge.target] == 0:
                    queue.append(edge.target)
        if len(order) != len(self._nodes):
            raise PipelineError("Pipeline contains a cycle")
        return order

    def run(
        self,
        initial_artifacts: Sequence[Artifact] | Mapping[str, Sequence[Artifact]] | None = None,
        *,
        seed: int | None = None,
        params_override: Mapping[str, Mapping[str, Any]] | None = None,
        extras: Mapping[str, Any] | None = None,
    ) -> PipelineRunResult:
        config_seed = self.config.run.seed
        base_seed = (
            seed
            if seed is not None
            else (config_seed if config_seed is not None else int(datetime.now(timezone.utc).timestamp()))
        )
        started_at = datetime.now(timezone.utc)
        run_id = hashlib.sha1(f"{base_seed}-{started_at.isoformat()}".encode()).hexdigest()
        manifest = RunManifest(
            run_id=run_id,
            seed=base_seed,
            started_at=started_at,
            batch_size=self.config.run.batch_size,
            capture=replace(self.config.run.capture) if self.config.run.capture else None,
            eval=replace(self.config.run.eval) if self.config.run.eval else None,
        )
        if self.config.run.extras:
            manifest.extras.update(self.config.run.extras)
            environment_cfg = self.config.run.extras.get("environment")
            if isinstance(environment_cfg, Mapping):
                manifest.environment.update(environment_cfg)
            hardware_cfg = self.config.run.extras.get("hardware")
            if isinstance(hardware_cfg, Mapping):
                manifest.hardware.update(hardware_cfg)
        store = ArtifactStore()
        if initial_artifacts:
            if isinstance(initial_artifacts, Mapping):
                for artifacts in initial_artifacts.values():
                    store.add_many(artifacts, producer_id=None)
            else:
                store.add_many(initial_artifacts, producer_id=None)
            snapshot = store.snapshot()
            manifest.inputs = {
                artifact_type: [artifact.id for artifact in artifacts]
                for artifact_type, artifacts in snapshot.items()
            }
        if extras:
            extras_dict = dict(extras)
            manifest.extras.update(extras_dict)
            environment = extras_dict.get("environment")
            if isinstance(environment, Mapping):
                manifest.environment.update(environment)
            hardware = extras_dict.get("hardware")
            if isinstance(hardware, Mapping):
                manifest.hardware.update(hardware)
        results: Dict[str, TaskResult] = {}
        params_override = params_override or {}

        for idx, node_id in enumerate(self._order):
            node = self._nodes[node_id]
            if node.uses not in self.registry:
                raise RegistryError(f"Capability {node.uses!r} not registered")
            task = self.registry.create(node.uses)
            node_params_raw = dict(node.params)
            node_params_raw.update(params_override.get(node_id, {}))
            node_params = task.validate_params(node_params_raw)
            ctx = RunContext(
                run_id=run_id,
                node_id=node_id,
                capability=node.uses,
                seed=base_seed + idx,
                store=store,
                extras=extras,
            )
            ctx.started_at = datetime.now(timezone.utc)
            inputs: Dict[str, List[Artifact]] = {}
            incoming_edges = self._incoming.get(node_id, [])
            if incoming_edges:
                for edge in incoming_edges:
                    inputs.setdefault(edge.artifact_type, []).extend(
                        store.get_from(edge.source, edge.artifact_type)
                    )
            else:
                for artifact_type in task.input_types:
                    inputs.setdefault(artifact_type, []).extend(store.get_global(artifact_type))
            for artifact_type in task.input_types:
                inputs.setdefault(artifact_type, [])
            frozen_inputs = {key: tuple(values) for key, values in inputs.items()}
            result = task.run(inputs=frozen_inputs, params=node_params, ctx=ctx)
            if not isinstance(result, TaskResult):
                raise PipelineError(f"Task {node.uses} returned invalid result type {type(result)!r}")
            result.trace = task.make_trace(ctx, node_params, frozen_inputs, result)
            allowed_outputs = set(task.output_types) if task.output_types else None
            if allowed_outputs:
                for artifact in result.artifacts:
                    if artifact.type not in allowed_outputs:
                        raise PipelineError(
                            f"Task {node.uses} produced unexpected artifact type {artifact.type!r}"
                        )
            for artifact in result.artifacts:
                if artifact.provenance is None:
                    artifact.provenance = result.trace
                store.add(artifact, producer_id=node_id)
            results[node_id] = result
            manifest.nodes[node_id] = NodeRunRecord(
                id=node_id,
                uses=node.uses,
                params=dict(node_params),
                config_hash=result.trace.config_hash,
                started_at=result.trace.started_at,
                ended_at=result.trace.ended_at,
                metrics=dict(result.metrics),
                model_id=result.trace.model_id,
                warnings=list(result.trace.warnings),
            )
        manifest.ended_at = datetime.now(timezone.utc)
        return PipelineRunResult(run_id=run_id, artifact_store=store, node_results=results, manifest=manifest)
