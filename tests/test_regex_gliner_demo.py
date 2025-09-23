from __future__ import annotations

from typing import Any, Dict

from examples.regex_gliner_pipeline import (
    build_demo_pipeline,
    load_demo_documents,
    make_document_artifacts,
    run_demo,
)


def _cluster_map(clusters):
    mapping: Dict[str, Any] = {}
    for artifact in clusters:
        data = artifact.data if isinstance(artifact.data, dict) else {}
        entity = data.get("entity", {})
        name = entity.get("name")
        if name:
            mapping[name] = artifact
    return mapping


def test_demo_pipeline_clusters_contacts() -> None:
    documents = load_demo_documents()
    loaded = build_demo_pipeline()
    pipeline = loaded.pipeline
    initial_docs = make_document_artifacts(documents)

    result = pipeline.run(initial_artifacts={"Document": initial_docs})

    clusters = result.artifacts_for("bundle", "EntityCluster")
    assert len(clusters) == len(documents)

    cluster_by_name = _cluster_map(clusters)
    assert set(cluster_by_name) == {"Alice Johnson", "Bob Rivera"}

    alice = cluster_by_name["Alice Johnson"]
    alice_data = alice.data
    assert {email["normalized"] for email in alice_data["emails"]} == {"alice.johnson@example.com"}
    assert alice_data["credit_cards"][0]["normalized"] == "4111111111111111"
    assert alice.meta["method"]["name"] == "gpt-5-nano"
    assert set(alice_data["methods_used"]) == {"gliner", "regex"}

    bob = cluster_by_name["Bob Rivera"]
    bob_data = bob.data
    assert {email["normalized"] for email in bob_data["emails"]} == {
        "bob@example.org",
        "bob.rivera@example.org",
    }
    assert bob_data["credit_cards"][0]["normalized"] == "5500000000000004"

    bundle_trace = result.node_results["bundle"].trace
    assert bundle_trace is not None
    dedupe_summary = bundle_trace.features["gpt5nano.deduplicated"]
    assert dedupe_summary["doc-001"]["EMAIL"]["alice.johnson@example.com"] >= 2
    assert dedupe_summary["doc-002"]["EMAIL"]["bob@example.org"] >= 2


def test_run_demo_helper_uses_samples() -> None:
    result = run_demo()
    clusters = result.artifacts_for("bundle", "EntityCluster")
    assert any(cluster.data["entity"]["name"] == "Alice Johnson" for cluster in clusters)
