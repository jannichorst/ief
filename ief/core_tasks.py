"""Definitions for the core pipeline tasks used across the project."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Tuple

from .utils import MetricKeys, coverage_per_field


@dataclass(frozen=True)
class CoreTask:
    """Lightweight description of a core pipeline task."""

    id: str
    capability: str
    summary: str
    input_types: Tuple[str, ...]
    output_types: Tuple[str, ...]
    metrics: Mapping[str, str]


CORE_TASKS: Dict[str, CoreTask] = {
    "ingest_text": CoreTask(
        id="ingest_text",
        capability="ie.ingest.text",
        summary=(
            "Convert document sources into text and layout layers. "
            "Includes OCR where necessary."
        ),
        input_types=("Document",),
        output_types=("TextLayer", "LayoutLayer"),
        metrics={
            MetricKeys.OCR_CER: "Character error rate of the OCR subsystem.",
            MetricKeys.OCR_WER: "Word error rate of the OCR subsystem.",
            MetricKeys.LAYOUT_TOKEN_BOX_ALIGNMENT: (
                "Alignment between recognised tokens and layout geometry."
            ),
            coverage_per_field("text"): (
                "Coverage of textual content captured in the TextLayer."
            ),
            coverage_per_field("tokens"): (
                "Coverage of tokenised content relative to the source."
            ),
        },
    ),
    "structure": CoreTask(
        id="structure",
        capability="ie.structure",
        summary="Augment the layout layer with structured blocks and lines.",
        input_types=("TextLayer", "LayoutLayer"),
        output_types=("LayoutLayer",),
        metrics={
            coverage_per_field("blocks"): (
                "IoU-style overlap for detected layout blocks."
            ),
            coverage_per_field("lines"): (
                "IoU-style overlap for detected layout lines."
            ),
            MetricKeys.LAYOUT_TOKEN_BOX_ALIGNMENT: (
                "Post-structuring token-to-box alignment quality."
            ),
        },
    ),
    "ner": CoreTask(
        id="ner",
        capability="ie.ner",
        summary="Detect entity mentions from the textual representation.",
        input_types=("TextLayer", "LayoutLayer"),
        output_types=("EntityMention",),
        metrics={
            MetricKeys.NER_SPAN_F1: "Span-level F1 against reference mentions.",
            MetricKeys.NER_PRECISION: "Precision for detected entity mentions.",
            MetricKeys.NER_RECALL: "Recall for detected entity mentions.",
            coverage_per_field("entity_mentions"): (
                "Coverage of required entity mention categories."
            ),
        },
    ),
    "key_value": CoreTask(
        id="key_value",
        capability="ie.kv",
        summary="Extract structured key-value pairs from the document.",
        input_types=("TextLayer", "LayoutLayer"),
        output_types=("KVPair",),
        metrics={
            MetricKeys.KV_EM: (
                "Exact match score aggregated across canonical fields."
            ),
            MetricKeys.KV_F1: (
                "Field-level F1 aggregated across canonical fields."
            ),
            MetricKeys.KV_COVERAGE: (
                "Fraction of canonical fields populated with a value."
            ),
            coverage_per_field("field"): (
                "Use per-field coverage for individual canonical slots."
            ),
        },
    ),
    "relation": CoreTask(
        id="relation",
        capability="ie.rel",
        summary="Infer relations among entity mentions.",
        input_types=("TextLayer", "EntityMention"),
        output_types=("Relation",),
        metrics={
            MetricKeys.REL_F1: "Micro-averaged F1 over relation predictions.",
            MetricKeys.REL_PRECISION: "Precision for relation extraction.",
            MetricKeys.REL_RECALL: "Recall for relation extraction.",
            coverage_per_field("relations"): (
                "Coverage of required relation schemas or slots."
            ),
        },
    ),
    "normalize_validate": CoreTask(
        id="normalize_validate",
        capability="ie.post.normalize_validate",
        summary="Normalise extracted artifacts and run validation checks.",
        input_types=("EntityMention", "KVPair", "Relation"),
        output_types=("EntityMention", "KVPair", "Relation"),
        metrics={
            MetricKeys.NORM_SUCCESS_RATE: (
                "Rate of successful normalization over processed artifacts."
            ),
            MetricKeys.DQ_VIOLATIONS: (
                "Count of data-quality violations detected during validation."
            ),
        },
    ),
}


def get_core_task_ids() -> Tuple[str, ...]:
    """Return the stable ordering of core task identifiers."""

    return tuple(CORE_TASKS.keys())
