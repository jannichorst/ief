"""Core runtime components for the Lean-Core Information Extraction pipeline."""

from __future__ import annotations

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
    INPUT_NODE_ID,
    Pipeline,
    PipelineEdge,
    PipelineError,
    PipelineNode,
    PipelineRunResult,
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
    "INPUT_NODE_ID",
    "KVPair",
    "LayoutElement",
    "LayoutLayer",
    "PageBox",
    "PageGeometry",
    "Pipeline",
    "PipelineEdge",
    "PipelineError",
    "PipelineNode",
    "PipelineRunResult",
    "RunContext",
    "SpanRef",
    "Task",
    "TaskRegistry",
    "TaskResult",
    "TextLayer",
    "TextToken",
    "Trace",
]
