"""Pipeline orchestration following the lean-core IE spec."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence

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


@dataclass
class PipelineConfig:
    version: str
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    run: Mapping[str, Any] = field(default_factory=dict)


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
    extras: Dict[str, Any] = field(default_factory=dict)


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
        run_cfg = raw.get("run", {})
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
        store = ArtifactStore()
        if initial_artifacts:
            if isinstance(initial_artifacts, Mapping):
                for artifacts in initial_artifacts.values():
                    store.add_many(artifacts, producer_id=None)
            else:
                store.add_many(initial_artifacts, producer_id=None)
        base_seed = seed if seed is not None else int(datetime.now(timezone.utc).timestamp())
        run_id = hashlib.sha1(f"{base_seed}-{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()
        manifest = RunManifest(run_id=run_id, seed=base_seed, started_at=datetime.now(timezone.utc))
        if extras:
            manifest.extras.update(dict(extras))
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
