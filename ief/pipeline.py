"""Pipeline orchestration primitives for the lean-core IE reference implementation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from typing import Any, Dict, Mapping, MutableMapping, Protocol, Sequence, Tuple, runtime_checkable

INPUT_NODE_ID = "__input__"


class PipelineError(RuntimeError):
    """Raised when the pipeline definition or execution is invalid."""


@dataclass
class Artifact:
    """Typed payload that flows between pipeline tasks."""

    id: str
    type: str
    doc_id: str | None = None
    data: Any = None
    parents: Sequence[str] = field(default_factory=tuple)
    meta: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class TaskResult:
    """Result returned by a task execution."""

    artifacts: Sequence[Artifact] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.artifacts = tuple(self.artifacts)
        self.metrics = dict(self.metrics)


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
        ctx: Any | None = None,
    ) -> TaskResult:
        ...


@dataclass
class PipelineNode:
    """Concrete task configured as part of the pipeline DAG."""

    id: str
    task: Task
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineEdge:
    """Typed connection between two nodes in the pipeline."""

    producer_node: str
    output_type: str
    consumer_node: str


@dataclass
class PipelineRunResult:
    """Container for artifacts and metrics produced during a pipeline run."""

    run_id: str
    node_results: Mapping[str, TaskResult]
    artifacts_by_node: Mapping[str, Mapping[str, Tuple[Artifact, ...]]]
    started_at: datetime
    ended_at: datetime

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

    def __init__(self, nodes: Sequence[PipelineNode], edges: Sequence[PipelineEdge]) -> None:
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
        params_override: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> PipelineRunResult:
        started_at = datetime.now(timezone.utc)
        run_id = hashlib.sha1(f"{started_at.isoformat()}".encode("utf-8")).hexdigest()

        outputs_by_node: Dict[str, Dict[str, list[Artifact]]] = {}

        def node_outputs(node_id: str) -> Dict[str, list[Artifact]]:
            return outputs_by_node.setdefault(node_id, {})

        if initial_artifacts:
            input_store = node_outputs(self.INPUT_NODE_ID)
            if isinstance(initial_artifacts, Mapping):
                for artifact_type, artifacts in initial_artifacts.items():
                    input_store.setdefault(artifact_type, []).extend(list(artifacts))
            else:
                for artifact in initial_artifacts:
                    input_store.setdefault(artifact.type, []).append(artifact)

        overrides = {node_id: dict(params) for node_id, params in (params_override or {}).items()}
        node_results: Dict[str, TaskResult] = {}

        for node_id in self._order:
            node = self._node_map[node_id]
            node_params = dict(node.params)
            if node_id in overrides:
                node_params.update(overrides[node_id])

            inputs: Dict[str, list[Artifact]] = {}
            incoming_edges = self._incoming.get(node_id, [])
            if incoming_edges:
                for edge in incoming_edges:
                    source_outputs = node_outputs(edge.producer_node)
                    inputs.setdefault(edge.output_type, []).extend(source_outputs.get(edge.output_type, ()))
            for artifact_type in getattr(node.task, "input_types", []):
                inputs.setdefault(artifact_type, [])

            frozen_inputs = {atype: tuple(values) for atype, values in inputs.items()}
            result = self._execute_task(node.task, frozen_inputs, node_params)
            if not isinstance(result, TaskResult):
                raise PipelineError(
                    f"Task {getattr(node.task, 'id', node_id)!r} returned invalid result type {type(result)!r}"
                )

            node_results[node_id] = result
            output_store = node_outputs(node_id)
            for artifact in result.artifacts:
                output_store.setdefault(artifact.type, []).append(artifact)

        ended_at = datetime.now(timezone.utc)

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
        )

    @staticmethod
    def _execute_task(
        task: Task,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
    ) -> TaskResult:
        try:
            return task.run(inputs=inputs, params=params, ctx=None)
        except TypeError as exc:
            if "unexpected keyword argument" in str(exc) and "'ctx'" in str(exc):
                return task.run(inputs=inputs, params=params)
            raise
