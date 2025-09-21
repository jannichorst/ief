"""Task base classes and runtime utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import random
from typing import Any, Dict, Mapping, Sequence

from .artifacts import Artifact
from .exceptions import SchemaValidationError
from .schema import validate_params
from .trace import Trace
from .store import ArtifactStore


@dataclass
class TaskResult:
    """Standard return value from a task run."""

    artifacts: Sequence[Artifact]
    trace: Trace | None = None
    metrics: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.artifacts = tuple(self.artifacts)
        self.metrics = dict(self.metrics)


class RunContext:
    """Runtime information available to tasks."""

    def __init__(
        self,
        run_id: str,
        node_id: str,
        capability: str,
        seed: int,
        store: ArtifactStore,
        extras: Mapping[str, Any] | None = None,
    ) -> None:
        self.run_id = run_id
        self.node_id = node_id
        self.capability = capability
        self.seed = seed
        self.random = random.Random(seed)
        self._store = store
        self._features: Dict[str, Any] = {}
        self._warnings: list[str] = []
        self.extras = dict(extras or {})
        self.started_at = datetime.now(timezone.utc)

    def record_feature(self, key: str, value: Any) -> None:
        self._features[key] = value

    def warn(self, message: str) -> None:
        self._warnings.append(message)

    def get_features(self) -> Dict[str, Any]:
        return dict(self._features)

    def get_warnings(self) -> tuple[str, ...]:
        return tuple(self._warnings)

    def get_artifacts(self, artifact_type: str) -> Sequence[Artifact]:
        return tuple(self._store.get_global(artifact_type))

    def get_artifacts_from(self, producer_id: str | None, artifact_type: str) -> Sequence[Artifact]:
        return tuple(self._store.get_from(producer_id, artifact_type))


class Task:
    """Base class for concrete pipeline tasks."""

    id: str
    input_types: Sequence[str] = ()
    output_types: Sequence[str] = ()
    params_schema: Mapping[str, Any] | None = {"type": "object", "properties": {}, "additionalProperties": True}
    default_model_id: str | None = None

    def validate_params(self, params: Mapping[str, Any] | None) -> Dict[str, Any]:
        try:
            return validate_params(params, self.params_schema)
        except SchemaValidationError as exc:
            raise SchemaValidationError(f"Invalid parameters for task {self.id}: {exc}") from exc

    def config_hash(self, params: Mapping[str, Any]) -> str:
        normalised = json.dumps(params, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(normalised.encode("utf-8")).hexdigest()

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: RunContext,
    ) -> TaskResult:
        raise NotImplementedError

    def make_trace(
        self,
        ctx: RunContext,
        params: Mapping[str, Any],
        inputs: Mapping[str, Sequence[Artifact]],
        result: TaskResult,
    ) -> Trace:
        config_hash = self.config_hash(params)
        parent_ids = tuple(artifact.id for artifacts in inputs.values() for artifact in artifacts)
        finished_at = datetime.now(timezone.utc)
        started_at = getattr(result.trace, "started_at", ctx.started_at)
        features = ctx.get_features()
        warnings = list(ctx.get_warnings())
        extra: Dict[str, Any] = {}
        if result.trace is not None:
            features.update(result.trace.features)
            warnings.extend(result.trace.warnings)
            extra.update(result.trace.extra)
        return Trace(
            run_id=ctx.run_id,
            task_id=ctx.node_id,
            model_id=self.default_model_id,
            config_hash=config_hash,
            started_at=started_at,
            ended_at=finished_at,
            parents=parent_ids,
            features=features,
            warnings=tuple(warnings),
            extra=extra,
        )
