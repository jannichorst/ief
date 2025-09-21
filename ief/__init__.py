"""Core runtime components for the Lean-Core Information Extraction pipeline.

This package provides Python building blocks that follow the contracts laid out
in ``spec_v_0.md``.  The goal is to make it straightforward to wire together
typed tasks inside a DAG, while keeping artifacts and provenance traceable.
"""

from .artifacts import (
    Artifact,
    BoundingBox,
    Document,
    DocumentSource,
    EntityMention,
    KVPair,
    LayoutElement,
    LayoutLayer,
    PageBox,
    PageGeometry,
    Relation,
    RelationArgument,
    SpanRef,
    TextLayer,
    TextToken,
)
from .pipeline import (
    CaptureConfig,
    EvalConfig,
    NodeRunRecord,
    Pipeline,
    PipelineConfig,
    PipelineEdge,
    PipelineNode,
    PipelineRunResult,
    RunConfig,
    RunManifest,
)
from .registry import TaskRegistry
from .tasks import RunContext, Task, TaskResult
from .trace import Trace

__all__ = [
    "Artifact",
    "BoundingBox",
    "Document",
    "DocumentSource",
    "EntityMention",
    "KVPair",
    "LayoutElement",
    "LayoutLayer",
    "PageBox",
    "PageGeometry",
    "Pipeline",
    "PipelineConfig",
    "PipelineEdge",
    "PipelineNode",
    "PipelineRunResult",
    "RunConfig",
    "RunManifest",
    "CaptureConfig",
    "EvalConfig",
    "NodeRunRecord",
    "RunContext",
    "SpanRef",
    "Task",
    "TaskRegistry",
    "TaskResult",
    "TextLayer",
    "TextToken",
    "Trace",
]

