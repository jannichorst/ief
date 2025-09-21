"""Trace data structures capturing provenance for each task invocation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Tuple


@dataclass
class Trace:
    """Execution metadata attached to artifacts produced by a task."""

    run_id: str
    task_id: str
    model_id: str | None
    config_hash: str
    started_at: datetime
    ended_at: datetime
    parents: Tuple[str, ...] = field(default_factory=tuple)
    features: Dict[str, Any] = field(default_factory=dict)
    warnings: Tuple[str, ...] = field(default_factory=tuple)
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        """Return the duration in milliseconds."""

        return (self.ended_at - self.started_at).total_seconds() * 1000.0

    @property
    def time(self) -> Mapping[str, datetime]:
        """Return a dictionary matching the spec ``time`` field."""

        return {"start": self.started_at, "end": self.ended_at}

    def as_dict(self) -> Dict[str, Any]:
        """Serialise the trace into a JSON-serialisable dictionary."""

        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "model_id": self.model_id,
            "config_hash": self.config_hash,
            "time": {
                "start": self.started_at.isoformat(),
                "end": self.ended_at.isoformat(),
            },
            "parents": list(self.parents),
            "features": dict(self.features),
            "warnings": list(self.warnings),
            "extra": dict(self.extra),
        }

    @classmethod
    def now(
        cls,
        run_id: str,
        task_id: str,
        model_id: str | None,
        config_hash: str,
        parents: Tuple[str, ...] = (),
        features: Mapping[str, Any] | None = None,
        warnings: Tuple[str, ...] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> "Trace":
        """Convenience constructor capturing the current time."""

        started = datetime.now(timezone.utc)
        ended = datetime.now(timezone.utc)
        return cls(
            run_id=run_id,
            task_id=task_id,
            model_id=model_id,
            config_hash=config_hash,
            started_at=started,
            ended_at=ended,
            parents=tuple(parents),
            features=dict(features or {}),
            warnings=tuple(warnings or ()),
            extra=dict(extra or {}),
        )
