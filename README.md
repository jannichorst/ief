# ief

Runtime components and data structures that implement the Lean-Core Information Extraction (IE) pipeline spec.  See `spec_v_0.md` for the authoritative contract; the Python package in `ief/` provides:

- Typed artifact models (documents, text/layout layers, spans, mentions, key-value pairs, relations).
- Task base classes with schema-aware parameter validation and provenance recording.
- A capability-based registry plus a DAG pipeline orchestrator with typed ports.

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
}

pipeline = Pipeline.from_config_dict(config, registry)
run = pipeline.run()
print(run.metrics)
```
