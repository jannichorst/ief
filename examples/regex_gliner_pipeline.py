"""Demo pipeline combining regex, GLiNER-style heuristics, and GPT-5-nano bundling."""

from __future__ import annotations

from collections import defaultdict
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from ief.config import LoadedPipeline, load_pipeline_from_dict
from ief.pipeline import Artifact as PipelineArtifact
from ief.pipeline import Pipeline, PipelineRunResult
from ief.registry import TaskRegistry
from ief.tasks import RunContext, Task, TaskResult

DEMO_DATA_PATH = Path(__file__).with_name("ner_demo_samples.json")
_TOKEN_PATTERN = re.compile(r"\b\w[\w'.-]*\b")


def load_demo_documents(path: Path | None = None) -> List[Dict[str, str]]:
    """Load the bundled demo documents used for the extraction showcase."""

    data_path = path or DEMO_DATA_PATH
    with data_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _basic_tokenise(text: str) -> List[Dict[str, Any]]:
    """Return lightweight token metadata for *text* suitable for demos."""

    tokens: List[Dict[str, Any]] = []
    for index, match in enumerate(_TOKEN_PATTERN.finditer(text)):
        tokens.append(
            {
                "i": index,
                "text": match.group(0),
                "start": match.start(),
                "end": match.end(),
            }
        )
    return tokens


class StaticTextIngestTask(Task):
    """Turns initial `Document` artifacts into `TextLayer` payloads."""

    def __init__(self) -> None:
        self.id = "demo.ingest.text/static"
        self.input_types = ("Document",)
        self.output_types = ("TextLayer",)

    def run(
        self,
        inputs: Mapping[str, Sequence[PipelineArtifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        documents = inputs.get("Document", ())
        text_layers: List[PipelineArtifact] = []
        for index, document in enumerate(documents):
            payload = document.data
            text = ""
            if isinstance(payload, Mapping):
                text = str(payload.get("text", ""))
            elif payload is not None:
                text = str(payload)
            doc_id = document.doc_id or document.id
            layer_id = f"{self.id}-text-{index}"
            text_layers.append(
                PipelineArtifact(
                    id=layer_id,
                    type="TextLayer",
                    doc_id=doc_id,
                    data={
                        "doc_id": doc_id,
                        "text": text,
                        "tokens": _basic_tokenise(text),
                    },
                    parents=(document.id,),
                    meta={
                        "method": {
                            "type": "ingest",
                            "name": "static-demo",
                            "provider": "demo",
                        }
                    },
                )
            )
        if ctx is not None:
            ctx.record_feature("documents", [doc.doc_id or doc.id for doc in documents])
        metrics = {"text_layers": float(len(text_layers))}
        return TaskResult(artifacts=text_layers, metrics=metrics)


class RegexNerTask(Task):
    """Regex-based detector for email addresses and credit card numbers."""

    EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
    CREDIT_CARD_PATTERN = re.compile(r"\b(?:\d{4}[- ]?){3}\d{4}\b")

    def __init__(self) -> None:
        self.id = "demo.ner.regex"
        self.input_types = ("TextLayer",)
        self.output_types = ("EntityMention",)

    def run(
        self,
        inputs: Mapping[str, Sequence[PipelineArtifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        text_layers = inputs.get("TextLayer", ())
        mentions: List[PipelineArtifact] = []
        per_doc_counts: MutableMapping[str, Dict[str, int]] = defaultdict(lambda: {"EMAIL": 0, "CREDIT_CARD": 0})

        for layer in text_layers:
            data = layer.data if isinstance(layer.data, Mapping) else {"text": layer.data}
            text = str(data.get("text", ""))
            doc_id = layer.doc_id or str(data.get("doc_id", ""))
            for match in self.EMAIL_PATTERN.finditer(text):
                mentions.append(
                    self._make_mention(
                        layer_id=layer.id,
                        doc_id=doc_id,
                        label="EMAIL",
                        surface=match.group(0),
                        start=match.start(),
                        end=match.end(),
                        score=1.0,
                    )
                )
                per_doc_counts[doc_id]["EMAIL"] += 1
            for match in self.CREDIT_CARD_PATTERN.finditer(text):
                mentions.append(
                    self._make_mention(
                        layer_id=layer.id,
                        doc_id=doc_id,
                        label="CREDIT_CARD",
                        surface=match.group(0),
                        start=match.start(),
                        end=match.end(),
                        score=0.95,
                    )
                )
                per_doc_counts[doc_id]["CREDIT_CARD"] += 1

        if ctx is not None:
            ctx.record_feature("regex.mention_counts", dict(per_doc_counts))
        metrics = {"mentions": float(len(mentions))}
        return TaskResult(artifacts=mentions, metrics=metrics)

    def _make_mention(
        self,
        *,
        layer_id: str,
        doc_id: str,
        label: str,
        surface: str,
        start: int,
        end: int,
        score: float,
    ) -> PipelineArtifact:
        artifact_id = f"{self.id}-{doc_id}-{label.lower()}-{start}"
        return PipelineArtifact(
            id=artifact_id,
            type="EntityMention",
            doc_id=doc_id,
            data={
                "label": label,
                "text": surface,
                "span": {"start": start, "end": end},
                "score": score,
                "source": "regex",
            },
            parents=(layer_id,),
            meta={
                "method": {
                    "type": "regex",
                    "name": "email-creditcard-rules",
                    "provider": "demo",
                }
            },
        )


class PseudoGlinerTask(Task):
    """Heuristic GLiNER-style recogniser for person names plus fuzzy entities."""

    PERSON_PATTERNS = [
        re.compile(r"\bAlice\s+Johnson\b", re.IGNORECASE),
        re.compile(r"\bBob\s+Rivera\b", re.IGNORECASE),
    ]
    EMAIL_PATTERN = RegexNerTask.EMAIL_PATTERN
    CREDIT_CARD_PATTERN = RegexNerTask.CREDIT_CARD_PATTERN

    def __init__(self) -> None:
        self.id = "demo.ner.gliner"
        self.input_types = ("TextLayer",)
        self.output_types = ("EntityMention",)

    def run(
        self,
        inputs: Mapping[str, Sequence[PipelineArtifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        text_layers = inputs.get("TextLayer", ())
        mentions: List[PipelineArtifact] = []
        for layer in text_layers:
            data = layer.data if isinstance(layer.data, Mapping) else {"text": layer.data}
            text = str(data.get("text", ""))
            doc_id = layer.doc_id or str(data.get("doc_id", ""))

            for pattern in self.PERSON_PATTERNS:
                for match in pattern.finditer(text):
                    mentions.append(
                        self._make_mention(
                            layer_id=layer.id,
                            doc_id=doc_id,
                            label="PERSON",
                            surface=match.group(0),
                            start=match.start(),
                            end=match.end(),
                            score=0.92,
                        )
                    )
            # GLiNER is generalist; we include softer email/card predictions to show ensembling.
            for match in self.EMAIL_PATTERN.finditer(text):
                mentions.append(
                    self._make_mention(
                        layer_id=layer.id,
                        doc_id=doc_id,
                        label="EMAIL",
                        surface=match.group(0),
                        start=match.start(),
                        end=match.end(),
                        score=0.78,
                    )
                )
            for match in self.CREDIT_CARD_PATTERN.finditer(text):
                mentions.append(
                    self._make_mention(
                        layer_id=layer.id,
                        doc_id=doc_id,
                        label="CREDIT_CARD",
                        surface=match.group(0),
                        start=match.start(),
                        end=match.end(),
                        score=0.7,
                    )
                )

        metrics = {"mentions": float(len(mentions))}
        return TaskResult(artifacts=mentions, metrics=metrics)

    def _make_mention(
        self,
        *,
        layer_id: str,
        doc_id: str,
        label: str,
        surface: str,
        start: int,
        end: int,
        score: float,
    ) -> PipelineArtifact:
        artifact_id = f"{self.id}-{doc_id}-{label.lower()}-{start}"
        return PipelineArtifact(
            id=artifact_id,
            type="EntityMention",
            doc_id=doc_id,
            data={
                "label": label,
                "text": surface,
                "span": {"start": start, "end": end},
                "score": score,
                "source": "gliner",
            },
            parents=(layer_id,),
            meta={
                "method": {
                    "type": "ml",
                    "name": "gliner-demo",
                    "provider": "mock",
                }
            },
        )


class Gpt5NanoEntityBundler(Task):
    """Deduplicate mentions and bundle contact entities via a GPT-5-nano simulation."""

    def __init__(self) -> None:
        self.id = "demo.bundle.gpt5nano"
        self.input_types = ("EntityMention",)
        self.output_types = ("EntityCluster",)
        self.default_model_id = "gpt-5-nano"

    def run(
        self,
        inputs: Mapping[str, Sequence[PipelineArtifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        mention_artifacts = list(inputs.get("EntityMention", ()))
        grouped: Dict[tuple[str, str, str], Dict[str, Any]] = {}

        for artifact in mention_artifacts:
            data = artifact.data if isinstance(artifact.data, Mapping) else {}
            label = str(data.get("label", ""))
            text = str(data.get("text", ""))
            span = data.get("span") if isinstance(data.get("span"), Mapping) else {}
            start = int(span.get("start", -1))
            end = int(span.get("end", -1))
            doc_id = artifact.doc_id or ""
            normalised = self._normalise_value(label, text)
            source = data.get("source") or self._source_from_meta(artifact.meta)
            key = (doc_id, label, normalised)
            bucket = grouped.setdefault(
                key,
                {
                    "doc_id": doc_id,
                    "label": label,
                    "normalised": normalised,
                    "text": text,
                    "mentions": [],
                    "sources": set(),
                    "starts": [],
                    "ends": [],
                },
            )
            bucket["mentions"].append(
                {
                    "artifact": artifact,
                    "start": start,
                    "end": end,
                    "source": source,
                    "score": data.get("score"),
                    "text": text,
                }
            )
            bucket["sources"].add(source)
            if start >= 0:
                bucket["starts"].append(start)
            if end >= 0:
                bucket["ends"].append(end)

        doc_groupings: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for (_doc_id, label, _), info in grouped.items():
            info["start"] = min(info["starts"]) if info["starts"] else -1
            info["end"] = max(info["ends"]) if info["ends"] else -1
            info["sources"] = set(info["sources"])
            doc_groupings[info["doc_id"]][label].append(info)

        clusters: List[PipelineArtifact] = []
        dedupe_summary: Dict[str, Dict[str, Dict[str, int]]] = {}

        for doc_id, label_map in doc_groupings.items():
            per_label_summary: Dict[str, Dict[str, int]] = {}
            for label, entries in label_map.items():
                label_summary: Dict[str, int] = {}
                for entry in entries:
                    if len(entry["mentions"]) > 1:
                        label_summary[entry["normalised"]] = len(entry["mentions"])
                if label_summary:
                    per_label_summary[label] = label_summary
            if per_label_summary:
                dedupe_summary[doc_id] = per_label_summary

            person_entries = sorted(label_map.get("PERSON", []), key=lambda item: item["start"])
            email_entries = sorted(label_map.get("EMAIL", []), key=lambda item: item["start"])
            card_entries = sorted(label_map.get("CREDIT_CARD", []), key=lambda item: item["start"])

            used_emails: set[str] = set()
            used_cards: set[str] = set()

            for index, person in enumerate(person_entries):
                next_start = (
                    person_entries[index + 1]["start"]
                    if index + 1 < len(person_entries)
                    else None
                )
                assigned_emails = self._select_within_window(
                    email_entries, used_emails, person["end"], next_start
                )
                assigned_cards = self._select_within_window(
                    card_entries, used_cards, person["end"], next_start
                )
                clusters.append(
                    self._build_cluster(doc_id, person, assigned_emails, assigned_cards, index)
                )

            leftover_emails = [entry for entry in email_entries if entry["normalised"] not in used_emails]
            for entry in leftover_emails:
                clusters.append(self._build_standalone_cluster(doc_id, entry, "EMAIL"))
            leftover_cards = [entry for entry in card_entries if entry["normalised"] not in used_cards]
            for entry in leftover_cards:
                clusters.append(self._build_standalone_cluster(doc_id, entry, "CREDIT_CARD"))

        if ctx is not None and dedupe_summary:
            ctx.record_feature("gpt5nano.deduplicated", dedupe_summary)
        metrics = {
            "clusters": float(len(clusters)),
            "mentions_in": float(len(mention_artifacts)),
        }
        return TaskResult(artifacts=clusters, metrics=metrics)

    @staticmethod
    def _source_from_meta(meta: Mapping[str, Any] | None) -> str:
        if not meta:
            return "unknown"
        method = meta.get("method")
        if isinstance(method, Mapping):
            return str(method.get("name", "unknown"))
        return "unknown"

    @staticmethod
    def _normalise_value(label: str, value: str) -> str:
        text = value.strip()
        if label.upper() == "EMAIL":
            return text.lower()
        if label.upper() == "CREDIT_CARD":
            return re.sub(r"\D", "", text)
        return text.lower()

    def _select_within_window(
        self,
        entries: Sequence[Dict[str, Any]],
        used: set[str],
        start_bound: int,
        end_bound: int | None,
    ) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        for entry in entries:
            key = entry["normalised"]
            if key in used:
                continue
            entry_start = entry.get("start", -1)
            if entry_start < 0:
                continue
            if entry_start < start_bound:
                continue
            if end_bound is not None and entry_start > end_bound:
                continue
            selected.append(entry)
            used.add(key)
        return selected

    def _build_cluster(
        self,
        doc_id: str,
        person: Dict[str, Any],
        emails: Sequence[Dict[str, Any]],
        cards: Sequence[Dict[str, Any]],
        index: int,
    ) -> PipelineArtifact:
        mention_ids = [m["artifact"].id for m in person["mentions"]]
        method_sources = set(person.get("sources", set()))
        email_payloads = []
        for email in emails:
            mention_ids.extend(m["artifact"].id for m in email["mentions"])
            method_sources.update(email.get("sources", set()))
            email_payloads.append(
                {
                    "text": email["text"],
                    "normalized": email["normalised"],
                    "sources": sorted(email.get("sources", set())),
                    "mention_ids": [m["artifact"].id for m in email["mentions"]],
                }
            )
        card_payloads = []
        for card in cards:
            mention_ids.extend(m["artifact"].id for m in card["mentions"])
            method_sources.update(card.get("sources", set()))
            card_payloads.append(
                {
                    "text": card["text"],
                    "normalized": card["normalised"],
                    "sources": sorted(card.get("sources", set())),
                    "mention_ids": [m["artifact"].id for m in card["mentions"]],
                }
            )

        entity_span = {
            "start": min((m.get("start", -1) for m in person["mentions"] if m.get("start", -1) >= 0), default=-1),
            "end": max((m.get("end", -1) for m in person["mentions"] if m.get("end", -1) >= 0), default=-1),
        }

        reasoning = (
            f"Grouped {len(email_payloads)} email(s) and {len(card_payloads)} card(s) with "
            f"{person['text']} based on textual proximity."
        )
        artifact_id = f"{self.id}-{doc_id}-entity-{index}"
        return PipelineArtifact(
            id=artifact_id,
            type="EntityCluster",
            doc_id=doc_id,
            data={
                "entity": {
                    "label": "PERSON",
                    "name": person["text"],
                    "span": entity_span,
                },
                "emails": email_payloads,
                "credit_cards": card_payloads,
                "methods_used": sorted(method_sources),
            },
            parents=tuple(mention_ids),
            meta={
                "method": {
                    "type": "llm",
                    "name": "gpt-5-nano",
                    "provider": "demo",
                },
                "reasoning": reasoning,
            },
        )

    def _build_standalone_cluster(
        self,
        doc_id: str,
        entry: Dict[str, Any],
        label: str,
    ) -> PipelineArtifact:
        mention_ids = [m["artifact"].id for m in entry["mentions"]]
        reasoning = f"No nearby person found; emitting standalone {label.lower()} cluster."
        artifact_id = f"{self.id}-{doc_id}-{label.lower()}-{entry['normalised']}"
        payload_key = "emails" if label == "EMAIL" else "credit_cards"
        payload_value = [
            {
                "text": entry["text"],
                "normalized": entry["normalised"],
                "sources": sorted(entry.get("sources", set())),
                "mention_ids": [m["artifact"].id for m in entry["mentions"]],
            }
        ]
        return PipelineArtifact(
            id=artifact_id,
            type="EntityCluster",
            doc_id=doc_id,
            data={
                "entity": {
                    "label": label,
                    "name": entry["text"],
                },
                payload_key: payload_value,
                "methods_used": sorted(entry.get("sources", set())),
            },
            parents=tuple(mention_ids),
            meta={
                "method": {
                    "type": "llm",
                    "name": "gpt-5-nano",
                    "provider": "demo",
                },
                "reasoning": reasoning,
            },
        )


class ConsoleReporterTask(Task):
    """Collects bundles and stores a lightweight summary on the trace."""

    def __init__(self) -> None:
        self.id = "demo.report.console"
        self.input_types = ("EntityCluster",)
        self.output_types: tuple[str, ...] = ()

    def run(
        self,
        inputs: Mapping[str, Sequence[PipelineArtifact]],
        params: Mapping[str, Any],
        ctx: RunContext | None,
    ) -> TaskResult:
        clusters = inputs.get("EntityCluster", ())
        summary: List[Dict[str, Any]] = []
        for cluster in clusters:
            data = cluster.data if isinstance(cluster.data, Mapping) else {}
            entity = data.get("entity", {}) if isinstance(data, Mapping) else {}
            summary.append(
                {
                    "doc_id": cluster.doc_id,
                    "entity": entity,
                    "emails": data.get("emails", []),
                    "credit_cards": data.get("credit_cards", []),
                }
            )
        if ctx is not None:
            ctx.record_feature("report.summary", summary)
        return TaskResult(artifacts=[], metrics={"clusters_logged": float(len(clusters))})


PIPELINE_CONFIG: Dict[str, Any] = {
    "version": "0.1",
    "pipeline": {
        "nodes": [
            {"id": "ingest", "uses": "demo.ingest.text"},
            {"id": "regex_ner", "uses": "demo.ner.regex"},
            {"id": "gliner_ner", "uses": "demo.ner.gliner"},
            {"id": "bundle", "uses": "demo.bundle.gpt5nano"},
            {"id": "report", "uses": "demo.report.console"},
        ],
        "edges": [
            f"{Pipeline.INPUT_NODE_ID}:Document -> ingest",
            "ingest:TextLayer -> regex_ner",
            "ingest:TextLayer -> gliner_ner",
            "regex_ner:EntityMention -> bundle",
            "gliner_ner:EntityMention -> bundle",
            "bundle:EntityCluster -> report",
        ],
    },
    "run": {
        "seed": 42,
        "capture": {"store_artifacts": ["EntityMention", "EntityCluster"]},
    },
}

REGISTRY_FACTORIES = {
    "demo.ingest.text": StaticTextIngestTask,
    "demo.ner.regex": RegexNerTask,
    "demo.ner.gliner": PseudoGlinerTask,
    "demo.bundle.gpt5nano": Gpt5NanoEntityBundler,
    "demo.report.console": ConsoleReporterTask,
}


def build_demo_registry() -> TaskRegistry:
    registry = TaskRegistry()
    for capability, factory in REGISTRY_FACTORIES.items():
        registry.register(capability, factory)
    return registry


def build_demo_pipeline() -> LoadedPipeline:
    registry = build_demo_registry()
    return load_pipeline_from_dict(PIPELINE_CONFIG, registry)


def make_document_artifacts(documents: Iterable[Mapping[str, Any]]) -> List[PipelineArtifact]:
    artifacts: List[PipelineArtifact] = []
    for document in documents:
        doc_id = str(document.get("id"))
        text = str(document.get("text", ""))
        artifacts.append(
            PipelineArtifact(
                id=doc_id,
                type="Document",
                doc_id=doc_id,
                data={"text": text},
            )
        )
    return artifacts


def run_demo(documents: Sequence[Mapping[str, Any]] | None = None) -> PipelineRunResult:
    loaded = build_demo_pipeline()
    pipeline = loaded.pipeline
    docs = list(documents) if documents is not None else load_demo_documents()
    initial_artifacts = {"Document": make_document_artifacts(docs)}
    extras = {"environment": {"ief_demo": "regex+gliner+gpt5nano"}}
    run_kwargs: Dict[str, Any] = {}
    if getattr(loaded.run, "seed", None) is not None:
        run_kwargs["seed"] = loaded.run.seed
    return pipeline.run(initial_artifacts=initial_artifacts, extras=extras, **run_kwargs)


if __name__ == "__main__":
    result = run_demo()
    bundle_outputs = result.artifacts_for("bundle", "EntityCluster")
    for artifact in bundle_outputs:
        print(json.dumps(artifact.data, indent=2))
