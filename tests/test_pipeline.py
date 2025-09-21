from __future__ import annotations

from typing import Any, Mapping, Sequence

from ief.config import LoadedPipeline, RunConfig, load_pipeline_from_dict
from ief.pipeline import Artifact, Pipeline, PipelineEdge, PipelineNode, PipelineRunResult, TaskResult


class RecordingTask:
    """Simple task implementation that records its inputs and emits configured artifacts."""

    def __init__(self, task_id: str, input_types: Sequence[str], output_types: Sequence[str]) -> None:
        self.id = task_id
        self.input_types = list(input_types)
        self.output_types = list(output_types)
        self.calls: list[Mapping[str, list[Artifact]]] = []

    def run(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        params: Mapping[str, Any],
        ctx: Any | None = None,
    ) -> TaskResult:
        captured = {artifact_type: list(artifacts) for artifact_type, artifacts in inputs.items()}
        self.calls.append(captured)
        artifacts: list[Artifact] = []
        emit_config = params.get("emit", {}) if isinstance(params, Mapping) else {}
        for output_type, payloads in emit_config.items():
            if output_type not in self.output_types:
                continue
            for index, payload in enumerate(payloads):
                artifacts.append(
                    Artifact(
                        id=f"{self.id}-{output_type}-{len(self.calls)}-{index}",
                        type=output_type,
                        data=payload,
                    )
                )
        return TaskResult(artifacts=artifacts)


def test_pipeline_edges_limit_artifact_flow() -> None:
    producer = RecordingTask("producer", input_types=[], output_types=["TextLayer"])
    distractor = RecordingTask("distractor", input_types=[], output_types=["TextLayer"])
    consumer = RecordingTask("consumer", input_types=["TextLayer"], output_types=[])

    pipeline = Pipeline(
        nodes=[
            PipelineNode(id="producer", task=producer, params={"emit": {"TextLayer": ["from_producer"]}}),
            PipelineNode(id="distractor", task=distractor, params={"emit": {"TextLayer": ["from_distractor"]}}),
            PipelineNode(id="consumer", task=consumer, params={}),
        ],
        edges=[PipelineEdge("producer", "TextLayer", "consumer")],
    )

    result = pipeline.run()
    assert isinstance(result, PipelineRunResult)
    assert consumer.calls
    consumer_inputs = consumer.calls[0]
    assert "TextLayer" in consumer_inputs
    received_payloads = [artifact.data for artifact in consumer_inputs["TextLayer"]]
    assert received_payloads == ["from_producer"]


def test_initial_artifacts_flow_through_input_edges() -> None:
    ingest = RecordingTask("ingest", input_types=["Document"], output_types=["TextLayer"])
    downstream = RecordingTask("ner", input_types=["TextLayer"], output_types=[])

    pipeline = Pipeline(
        nodes=[
            PipelineNode(id="ingest", task=ingest, params={"emit": {"TextLayer": ["derived_text"]}}),
            PipelineNode(id="ner", task=downstream, params={}),
        ],
        edges=[
            PipelineEdge(Pipeline.INPUT_NODE_ID, "Document", "ingest"),
            PipelineEdge("ingest", "TextLayer", "ner"),
        ],
    )

    doc_artifact = Artifact(id="doc-1", type="Document", data="raw document")
    pipeline.run(initial_artifacts={"Document": [doc_artifact]})

    assert ingest.calls
    ingest_inputs = ingest.calls[0]
    assert "Document" in ingest_inputs
    assert ingest_inputs["Document"][0] is doc_artifact


def test_loader_builds_pipeline_from_config() -> None:
    registry = {
        "mock.ingest": lambda params: RecordingTask("mock.ingest", [], ["TextLayer"]),
        "mock.ner": lambda params: RecordingTask("mock.ner", ["TextLayer"], []),
    }

    config = {
        "version": "0.1",
        "pipeline": {
            "nodes": [
                {
                    "id": "ingest",
                    "uses": "mock.ingest",
                    "params": {"emit": {"TextLayer": ["hello"]}},
                },
                {"id": "ner", "uses": "mock.ner"},
            ],
            "edges": [
                f"{Pipeline.INPUT_NODE_ID}:Document -> ingest",
                "ingest:TextLayer -> ner",
            ],
        },
        "run": {
            "seed": 1337,
            "batch_size": 2,
            "capture": {"store_artifacts": ["TextLayer"]},
        },
    }

    loaded = load_pipeline_from_dict(config, registry)
    assert isinstance(loaded, LoadedPipeline)
    assert isinstance(loaded.run, RunConfig)
    assert loaded.version == "0.1"
    assert loaded.run.seed == 1337
    assert loaded.run.batch_size == 2
    assert loaded.run.capture["store_artifacts"] == ["TextLayer"]

    pipeline = loaded.pipeline
    result = pipeline.run(initial_artifacts={"Document": [Artifact(id="doc", type="Document", data="text")]})

    ner_node = pipeline.get_node("ner")
    assert isinstance(ner_node.task, RecordingTask)
    assert ner_node.task.calls
    ner_inputs = ner_node.task.calls[0]
    assert "TextLayer" in ner_inputs
    assert [artifact.data for artifact in ner_inputs["TextLayer"]] == ["hello"]

    assert "ingest" in result.artifacts_by_node
    produced_text = result.artifacts_for("ingest", "TextLayer")
    assert [artifact.data for artifact in produced_text] == ["hello"]
