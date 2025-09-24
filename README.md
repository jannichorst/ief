# ief

Runtime components and data structures that implement the Lean-Core Information Extraction (IE) pipeline spec. See `spec_v_0.md` for the authoritative contract; the Python package in `ief/` provides:

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

This repository also provides lightweight task metadata (`ief.core_tasks`) and helpers for standardised metric naming (`ief.utils`). The goal is to keep the specification and reference code in sync so downstream components consistently emit metrics that align with §8 of `spec_v_0.md`.

## Visualising the data model

An interactive overview of the core artifact data model is available via `examples/data_model_overview.py`. The script inspects the `ief.artifacts` and `ief.trace` dataclasses and builds a navigable network diagram showing inheritance and field-level relationships.

```bash
python examples/data_model_overview.py
# => writes visualizations/data_model_overview.html
```

Open the generated HTML file in a browser to explore the graph—hover over any node to see the dataclass fields and drag nodes to rearrange the layout. The viewer loads the [force-graph](https://github.com/vasturiano/force-graph) library from a CDN, so an internet connection is required for the interactive rendering.

## Metrics naming helpers

All metric keys emitted by the core tasks come from the shared constants in `ief.utils.MetricKeys` or from helper constructors such as `ief.utils.coverage_per_field`. This ensures downstream collectors can rely on familiar names like `kv.coverage` instead of ad-hoc entries.

```python
from ief.core_tasks import CORE_TASKS

kv_task = CORE_TASKS["key_value"]
for metric_key, description in kv_task.metrics.items():
    print(metric_key, "-", description)
```

Running the snippet produces output similar to:

```
kv.em - Exact match score aggregated across canonical fields.
kv.f1 - Field-level F1 aggregated across canonical fields.
kv.coverage - Fraction of canonical fields populated with a value.
coverage.per_field.field - Use per-field coverage for individual canonical slots.
```

Refer to the spec for the complete vocabulary of standard metric keys and for a broader discussion of the core tasks.
