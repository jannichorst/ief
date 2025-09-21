"""Core package for the lean information extraction pipeline prototype."""

from .pipeline import (
    Artifact,
    Pipeline,
    PipelineEdge,
    PipelineNode,
    PipelineRunResult,
    Task,
    TaskResult,
)

__all__ = [
    "Artifact",
    "Pipeline",
    "PipelineEdge",
    "PipelineNode",
    "PipelineRunResult",
    "Task",
    "TaskResult",
]