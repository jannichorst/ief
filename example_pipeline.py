"""Minimal example wiring tasks together via the declarative pipeline loader."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ief.config import load_pipeline_from_dict
from ief.pipeline import Artifact, Pipeline, TaskResult


class EmitTask:
    """Trivial task that emits artifacts declared in its params."""

    def __init__(self, task_id: str, input_types: Sequence[str], output_types: Sequence[str]) -> None:
        self.id = task_id
        self.input_types = list(input_types)
        self.output_types = list(output_types)

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: Any | None = None,
    ) -> TaskResult:
        emit_config = params.get("emit", {}) if isinstance(params, Mapping) else {}
        artifacts: list[Artifact] = []
        for output_type, payloads in emit_config.items():
            if output_type not in self.output_types:
                continue
            for index, payload in enumerate(payloads):
                artifacts.append(
                    Artifact(id=f"{self.id}-{output_type}-{index}", type=output_type, data=payload)
                )
        return TaskResult(artifacts=artifacts)


class PrintTask:
    """Task that prints the artifacts it receives."""

    def __init__(self, task_id: str, input_types: Sequence[str]) -> None:
        self.id = task_id
        self.input_types = list(input_types)
        self.output_types: list[str] = []

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: Any | None = None,
    ) -> TaskResult:
        print(f"[{self.id}] received:")
        for artifact_type, artifacts in inputs.items():
            for artifact in artifacts:
                print(f"  - {artifact_type}: {artifact.data}")
        return TaskResult(artifacts=[])


CONFIG = {
    "version": "0.1",
    "pipeline": {
        "nodes": [
            {
                "id": "ingest",
                "uses": "demo.ingest",
                "params": {"emit": {"TextLayer": ["Hello from config!"]}},
            },
            {"id": "ner", "uses": "demo.ner"},
        ],
        "edges": [
            f"{Pipeline.INPUT_NODE_ID}:Document -> ingest",
            "ingest:TextLayer -> ner",
        ],
    },
}

REGISTRY = {
    "demo.ingest": lambda params: EmitTask("demo.ingest", ["Document"], ["TextLayer"]),
    "demo.ner": lambda params: PrintTask("demo.ner", ["TextLayer"]),
}


def main() -> None:
    loaded = load_pipeline_from_dict(CONFIG, REGISTRY)
    pipeline = loaded.pipeline
    print("Loaded pipeline with nodes:", [node.id for node in pipeline.nodes])
    doc = Artifact(id="doc-1", type="Document", data="Example document payload")
    pipeline.run(initial_artifacts={"Document": [doc]})


if __name__ == "__main__":
    main()
