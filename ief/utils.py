"""Utility helpers for working with standardized metric keys.

The project spec (see :mod:`spec_v_0`) defines a compact vocabulary of
metric names under §8.  To reduce drift across modules, helpers in this
module provide the canonical strings and sanitisation routines used by
the core tasks.
"""

from __future__ import annotations

import re
from typing import Final

__all__ = [
    "MetricKeys",
    "coverage_per_field",
    "latency_for",
    "cost_for",
]

_FIELD_SANITISER: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")


def _normalise_suffix(value: str) -> str:
    """Return a lowercase identifier fragment suitable for metric keys."""

    normalised = _FIELD_SANITISER.sub(".", value.strip().lower())
    return normalised.strip(".") or value.strip().lower()


class MetricKeys:
    """Namespace of frequently used metric keys.

    Keeping the values centralised ensures that call sites cannot
    inadvertently emit non-standard keys (e.g. ``kv.candidates``) and
    allows static analysis or linting to spot typos more easily.
    """

    OCR_CER: Final[str] = "ocr.cer"
    OCR_WER: Final[str] = "ocr.wer"
    LAYOUT_TOKEN_BOX_ALIGNMENT: Final[str] = "layout.token_box_alignment"

    NER_SPAN_F1: Final[str] = "ner.span_f1"
    NER_PRECISION: Final[str] = "ner.precision"
    NER_RECALL: Final[str] = "ner.recall"

    KV_EM: Final[str] = "kv.em"
    KV_F1: Final[str] = "kv.f1"
    KV_COVERAGE: Final[str] = "kv.coverage"

    REL_F1: Final[str] = "rel.f1"
    REL_PRECISION: Final[str] = "rel.precision"
    REL_RECALL: Final[str] = "rel.recall"

    NORM_SUCCESS_RATE: Final[str] = "norm.success_rate"
    DQ_VIOLATIONS: Final[str] = "dq.violations"

    @staticmethod
    def coverage(field: str) -> str:
        """Shortcut for :func:`coverage_per_field`."""

        return coverage_per_field(field)


def coverage_per_field(field: str) -> str:
    """Return the canonical coverage key for ``field``.

    The helper mirrors the naming convention from §8: values are emitted
    under ``coverage.per_field.{field}`` with the field converted to a
    dotted lowercase identifier (e.g. ``"Invoice Number"`` →
    ``"coverage.per_field.invoice.number"``).
    """

    suffix = _normalise_suffix(field)
    return f"coverage.per_field.{suffix}"


def latency_for(node_id: str) -> str:
    """Return the latency metric key for a pipeline node."""

    suffix = _normalise_suffix(node_id)
    return f"latency.{suffix}"


def cost_for(node_id: str) -> str:
    """Return the cost metric key for a pipeline node."""

    suffix = _normalise_suffix(node_id)
    return f"cost.{suffix}"
