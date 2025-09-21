# ief

Runtime components and data structures that implement the Lean-Core Information Extraction (IE) pipeline spec.  See `spec_v_0.md` for the authoritative contract; the Python package in `ief/` provides:

- Typed artifact models (documents, text/layout layers, spans, mentions, key-value pairs, relations).
- Task base classes with schema-aware parameter validation and provenance recording.
- A capability-based registry plus a DAG pipeline orchestrator with typed ports.
- Run configuration helpers that surface capture/eval directives and a manifest capturing seeds, inputs, and per-node metrics.

```python
from ief import Pipeline, TaskRegistry

registry = TaskRegistry()
# register concrete tasks ...

config = {
    "version": "0.1",
    "pipeline": {
        "nodes": [
            {"id": "ingest", "uses": "ie.ingest.text/mock"},
        ],
        "edges": [],
    },
    "run": {
        "seed": 1337,
        "capture": {"store_artifacts": ["TextLayer"], "store_renders": False},
        "eval": {"dataset": "path/to/gold.jsonl", "metrics": ["span_f1"]},
    },
}

pipeline = Pipeline.from_config_dict(config, registry)
run = pipeline.run(extras={"environment": {"ief_version": "0.1"}})
print(run.metrics)
print(run.manifest.as_dict()["seed"])
```
