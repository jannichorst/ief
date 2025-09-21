# ief

This repository explores a lean core for information extraction (IE)
pipelines.  The `ief` Python package contains lightweight task metadata
(`ief.core_tasks`) and helpers for standardised metric naming
(`ief.utils`).  The goal is to keep the specification and reference code
in sync so downstream components consistently emit metrics that align
with §8 of `spec_v_0.md`.

## Metrics naming helpers

All metric keys emitted by the core tasks come from the shared constants
in `ief.utils.MetricKeys` or from helper constructors such as
`ief.utils.coverage_per_field`.  This ensures downstream collectors can
rely on familiar names like `kv.coverage` instead of ad-hoc entries.

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

Refer to the spec for the complete vocabulary of standard metric keys
and for a broader discussion of the core tasks.
