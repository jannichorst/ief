from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Mapping, Sequence

from ief.config import LoadedPipeline, load_pipeline_from_dict
from ief.pipeline import (
    CaptureConfig,
    Pipeline,
    PipelineConfig,
    PipelineEdge,
    PipelineNode,
    PipelineRunResult,
    RunConfig,
)
from ief.registry import TaskRegistry
from ief.artifacts import Artifact
from ief.tasks import RunContext, Task, TaskResult


@dataclass
class DocumentArtifact(Artifact):
    TYPE: ClassVar[str] = "Document"
    payload: Any = None


@dataclass
class SimpleArtifact(Artifact):
    TYPE: ClassVar[str] = "Simple"
    payload: Any = None


_ARTIFACT_TYPES = {
    "Document": DocumentArtifact,
    "Simple": SimpleArtifact,
}


class RecordingTask(Task):
    """Task implementation that records its inputs via trace features."""

    def __init__(self, task_id: str, input_types: Sequence[str], output_types: Sequence[str]) -> None:
        self.id = task_id
        self.input_types = tuple(input_types)
        self.output_types = tuple(output_types)

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: RunContext,
    ) -> TaskResult:
        ctx.record_feature(
            "inputs",
            {
                artifact_type: [artifact.id for artifact in artifacts]
                for artifact_type, artifacts in inputs.items()
            },
        )
        emit_config = params.get("emit", {}) if isinstance(params, Mapping) else {}
        doc_id = str(params.get("doc_id", "doc"))
        artifacts: list[Artifact] = []
        for output_type, payloads in emit_config.items():
            if output_type not in self.output_types:
                continue
            if isinstance(payloads, Sequence) and not isinstance(payloads, (str, bytes, bytearray)):
                values = list(payloads)
            else:
                values = [payloads]
            artifact_cls = _ARTIFACT_TYPES.get(output_type, SimpleArtifact)
            for index, payload in enumerate(values):
                artifact_id = f"{self.id}-{output_type}-{len(artifacts)}-{index}"
                artifacts.append(
                    artifact_cls(
                        id=artifact_id,
                        doc_id=doc_id,
                        payload=payload,
                    )
                )
        metrics = {"outputs": float(len(artifacts))}
        return TaskResult(artifacts=artifacts, metrics=metrics)


def test_pipeline_edges_limit_artifact_flow() -> None:
    registry = TaskRegistry()
    registry.register("demo.producer", lambda: RecordingTask("producer", [], ["Simple"]))
    registry.register("demo.distractor", lambda: RecordingTask("distractor", [], ["Simple"]))
    registry.register("demo.consumer", lambda: RecordingTask("consumer", ["Simple"], []))

    config = PipelineConfig(
        version="0.1",
        nodes=[
            PipelineNode(
                id="producer",
                uses="demo.producer",
                params={"emit": {"Simple": ["from_producer"]}, "doc_id": "doc-A"},
            ),
            PipelineNode(
                id="distractor",
                uses="demo.distractor",
                params={"emit": {"Simple": ["from_distractor"]}, "doc_id": "doc-B"},
            ),
            PipelineNode(id="consumer", uses="demo.consumer", params={}),
        ],
        edges=[PipelineEdge(source="producer", artifact_type="Simple", target="consumer")],
        run=RunConfig(),
    )
    pipeline = Pipeline(config, registry)

    result = pipeline.run()
    assert isinstance(result, PipelineRunResult)

    consumer_trace = result.node_results["consumer"].trace
    assert consumer_trace.features["inputs"]["Simple"] == ["producer-Simple-0-0"]

    distractor_outputs = result.artifact_store.get_from("distractor", "Simple")
    assert [artifact.payload for artifact in distractor_outputs] == ["from_distractor"]
    assert "distractor-Simple-0-0" not in consumer_trace.features["inputs"]["Simple"]


def test_initial_artifacts_flow_through_input_store() -> None:
    registry = TaskRegistry()
    registry.register("demo.ingest", lambda: RecordingTask("ingest", ["Document"], ["Simple"]))
    registry.register("demo.ner", lambda: RecordingTask("ner", ["Simple"], []))

    config = PipelineConfig(
        version="0.1",
        nodes=[
            PipelineNode(
                id="ingest",
                uses="demo.ingest",
                params={"emit": {"Simple": ["derived_text"]}, "doc_id": "doc-1"},
            ),
            PipelineNode(id="ner", uses="demo.ner", params={}),
        ],
        edges=[PipelineEdge(source="ingest", artifact_type="Simple", target="ner")],
        run=RunConfig(),
    )
    pipeline = Pipeline(config, registry)

    initial_doc = DocumentArtifact(id="doc-1", doc_id="doc-1", payload="raw document")
    result = pipeline.run(initial_artifacts={"Document": [initial_doc]})

    ingest_trace = result.node_results["ingest"].trace
    assert ingest_trace.features["inputs"]["Document"] == ["doc-1"]

    ner_trace = result.node_results["ner"].trace
    assert ner_trace.features["inputs"]["Simple"] == ["ingest-Simple-0-0"]

    produced = result.artifact_store.get_from("ingest", "Simple")
    assert [artifact.payload for artifact in produced] == ["derived_text"]
    assert result.manifest.inputs["Document"] == ["doc-1"]


def test_loader_builds_pipeline_from_config() -> None:
    registry = TaskRegistry()
    registry.register("mock.ingest", lambda: RecordingTask("ingest", ["Document"], ["Simple"]))
    registry.register("mock.ner", lambda: RecordingTask("ner", ["Simple"], []))

    config = {
        "version": "0.1",
        "pipeline": {
            "nodes": [
                {
                    "id": "ingest",
                    "uses": "mock.ingest",
                    "params": {"emit": {"Simple": ["hello"]}, "doc_id": "doc-2"},
                },
                {"id": "ner", "uses": "mock.ner"},
            ],
            "edges": ["ingest:Simple -> ner"],
        },
        "run": {
            "seed": 1337,
            "batch_size": 2,
            "capture": {"store_artifacts": ["Simple"]},
        },
    }

    loaded = load_pipeline_from_dict(config, registry)
    assert isinstance(loaded, LoadedPipeline)
    assert loaded.version == "0.1"
    assert loaded.run.seed == 1337
    assert loaded.run.batch_size == 2
    assert isinstance(loaded.run.capture, CaptureConfig)
    assert loaded.run.capture.store_artifacts == ("Simple",)

    pipeline = loaded.pipeline
    doc = DocumentArtifact(id="doc-2", doc_id="doc-2", payload="text")
    result = pipeline.run(initial_artifacts={"Document": [doc]})

    ingest_trace = result.node_results["ingest"].trace
    assert ingest_trace.features["inputs"]["Document"] == ["doc-2"]

    produced_text = result.artifact_store.get_from("ingest", "Simple")
    assert [artifact.payload for artifact in produced_text] == ["hello"]

    assert "ingest" in result.node_results
    assert "ner" in result.node_results
    assert "ingest.outputs" in result.metrics
