"""Core orchestration primitives for the lean IE pipeline."""

from __future__ import annotations

from collections import defaultdict, deque
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
    """Convenience container for artifacts and metadata produced during a run."""

    node_results: Mapping[str, TaskResult]
    artifacts_by_node: Mapping[str, Mapping[str, Sequence[Artifact]]]

    def artifacts_for(self, node_id: str, artifact_type: str) -> Sequence[Artifact]:
        """Return the artifacts produced by *node_id* of the specified *artifact_type*."""

        node_outputs = self.artifacts_by_node.get(node_id, {})
        return tuple(node_outputs.get(artifact_type, ()))


class Pipeline:
    """A simple typed DAG orchestrator for information-extraction tasks."""

    INPUT_NODE_ID = "@input"

    def __init__(self, nodes: Iterable[PipelineNode], edges: Iterable[PipelineEdge]) -> None:
        self._nodes: Dict[str, PipelineNode] = {}
        self._ordered_node_ids: list[str] = []
        for node in nodes:
            if node.id == self.INPUT_NODE_ID:
                raise PipelineError("'@input' is reserved for pipeline inputs")
            if node.id in self._nodes:
                raise PipelineError(f"Duplicate node id '{node.id}'")
            self._nodes[node.id] = node
            self._ordered_node_ids.append(node.id)

        self._edges: tuple[PipelineEdge, ...] = tuple(edges)
        self._incoming_edges: Dict[str, list[PipelineEdge]] = defaultdict(list)
        self._outgoing_edges: Dict[str, list[PipelineEdge]] = defaultdict(list)
        for edge in self._edges:
            if not edge.output_type:
                raise PipelineError("Edge output_type must be a non-empty string")
            if edge.producer_node != self.INPUT_NODE_ID and edge.producer_node not in self._nodes:
                raise PipelineError(
                    f"Edge references unknown producer '{edge.producer_node}'"
                )
            if edge.consumer_node not in self._nodes:
                raise PipelineError(
                    f"Edge references unknown consumer '{edge.consumer_node}'"
                )
            self._incoming_edges[edge.consumer_node].append(edge)
            self._outgoing_edges[edge.producer_node].append(edge)

        self._topological_order: tuple[str, ...] = self._compute_topological_order()

    @property
    def nodes(self) -> tuple[PipelineNode, ...]:
        """Return the pipeline nodes in their original declaration order."""

        return tuple(self._nodes[node_id] for node_id in self._ordered_node_ids)

    @property
    def edges(self) -> tuple[PipelineEdge, ...]:
        """Return the declared pipeline edges."""

        return self._edges

    def get_node(self, node_id: str) -> PipelineNode:
        """Return the pipeline node with *node_id* or raise :class:`PipelineError`."""

        try:
            return self._nodes[node_id]
        except KeyError as exc:  # pragma: no cover - defensive
            raise PipelineError(f"Unknown node '{node_id}'") from exc

    def _compute_topological_order(self) -> tuple[str, ...]:
        """Compute a stable topological order for the pipeline DAG."""

        in_degree: Dict[str, int] = {node_id: 0 for node_id in self._ordered_node_ids}
        for edge in self._edges:
            if edge.producer_node == self.INPUT_NODE_ID:
                continue
            in_degree[edge.consumer_node] += 1

        ready: deque[str] = deque(node_id for node_id in self._ordered_node_ids if in_degree[node_id] == 0)
        order: list[str] = []
        while ready:
            node_id = ready.popleft()
            order.append(node_id)
            for edge in self._outgoing_edges.get(node_id, ()):  # pragma: no cover - fallback
                consumer = edge.consumer_node
                in_degree[consumer] -= 1
                if in_degree[consumer] == 0:
                    ready.append(consumer)

        if len(order) != len(self._ordered_node_ids):
            raise PipelineError("Pipeline graph contains a cycle")
        return tuple(order)

    def run(
        self,
        initial_artifacts: Optional[Mapping[str, Sequence[Artifact]]] = None,
        *,
        ctx: Optional[Any] = None,
    ) -> PipelineRunResult:
        """Execute the pipeline.

        Parameters
        ----------
        initial_artifacts:
            Mapping of artifact type → artifacts that should be injected into the
            graph via ``@input`` edges.
        ctx:
            Optional execution context forwarded to each task.
        """

        artifacts_by_node: Dict[str, Dict[str, list[Artifact]]] = defaultdict(lambda: defaultdict(list))
        if initial_artifacts:
            for artifact_type, artifacts in initial_artifacts.items():
                if not isinstance(artifact_type, str) or not artifact_type:
                    raise PipelineError("Artifact types must be non-empty strings")
                for artifact in artifacts:
                    if not isinstance(artifact, Artifact):
                        raise PipelineError("Initial artifacts must be instances of Artifact")
                    artifacts_by_node[self.INPUT_NODE_ID][artifact_type].append(artifact)
        else:
            artifacts_by_node[self.INPUT_NODE_ID]  # ensure key exists

        node_results: Dict[str, TaskResult] = {}
        for node_id in self._topological_order:
            node = self._nodes[node_id]
            inputs = self._gather_inputs(node_id, artifacts_by_node)
            result = node.task.run(inputs, node.params, ctx)
            if not isinstance(result, TaskResult):
                raise PipelineError(
                    f"Task '{node.task.id}' did not return a TaskResult instance"
                )
            node_results[node_id] = result
            node_outputs: Dict[str, list[Artifact]] = artifacts_by_node[node_id]
            for artifact in result.artifacts:
                if not isinstance(artifact, Artifact):
                    raise PipelineError(
                        f"Task '{node.task.id}' produced a non-Artifact output"
                    )
                if hasattr(node.task, "output_types") and node.task.output_types:
                    if artifact.type not in node.task.output_types:
                        raise PipelineError(
                            f"Task '{node.task.id}' produced unexpected artifact type '{artifact.type}'"
                        )
                node_outputs[artifact.type].append(artifact)

        frozen_outputs: Dict[str, Dict[str, tuple[Artifact, ...]]] = {}
        for node_id, outputs in artifacts_by_node.items():
            frozen_outputs[node_id] = {
                artifact_type: tuple(artifacts)
                for artifact_type, artifacts in outputs.items()
            }

        return PipelineRunResult(node_results=node_results, artifacts_by_node=frozen_outputs)

    def _gather_inputs(
        self,
        node_id: str,
        artifacts_by_node: Mapping[str, Mapping[str, Sequence[Artifact]]],
    ) -> Dict[str, list[Artifact]]:
        incoming = self._incoming_edges.get(node_id, ())
        inputs: Dict[str, list[Artifact]] = defaultdict(list)
        for edge in incoming:
            producer_outputs = artifacts_by_node.get(edge.producer_node, {})
            for artifact in producer_outputs.get(edge.output_type, ()):  # type: ignore[arg-type]
                inputs[edge.output_type].append(artifact)
        return inputs
