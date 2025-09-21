"""Storage helpers for managing artifacts during a pipeline run."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, MutableMapping

from .artifacts import Artifact

_INITIAL_PRODUCER = "__initial__"


@dataclass
class ArtifactRecord:
    artifact: Artifact
    producer_id: str


class ArtifactStore:
    """Keep track of artifacts by type and producing node."""

    def __init__(self, artifacts: Iterable[Artifact] | None = None) -> None:
        self._by_type: MutableMapping[str, List[Artifact]] = defaultdict(list)
        self._by_node: MutableMapping[tuple[str | None, str], List[Artifact]] = defaultdict(list)
        if artifacts:
            self.add_many(artifacts, producer_id=None)

    def add_many(self, artifacts: Iterable[Artifact], producer_id: str | None) -> None:
        for artifact in artifacts:
            self.add(artifact, producer_id)

    def add(self, artifact: Artifact, producer_id: str | None) -> None:
        key = (producer_id or _INITIAL_PRODUCER, artifact.type)
        self._by_type[artifact.type].append(artifact)
        self._by_node[key].append(artifact)

    def get_global(self, artifact_type: str) -> List[Artifact]:
        return list(self._by_type.get(artifact_type, ()))

    def get_from(self, producer_id: str | None, artifact_type: str) -> List[Artifact]:
        key = (producer_id or _INITIAL_PRODUCER, artifact_type)
        return list(self._by_node.get(key, ()))

    def snapshot(self) -> Dict[str, List[Artifact]]:
        return {atype: list(items) for atype, items in self._by_type.items()}

    def __contains__(self, artifact_type: str) -> bool:
        return artifact_type in self._by_type
