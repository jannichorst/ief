# Lean‑Core Information Extraction Pipeline — Spec (v0.1)

**Intent:** A small, stable core for end‑to‑end IE with text+layout grounding, strict provenance, and interchangeable methods. Everything else is pluggable optional modules that conform to the same task contracts.

---

## 1) Principles & Scope
- **Typed artifacts** only; tasks read/write known types.
- **DAG orchestration** with typed ports (no hardcoded sequences).
- **Dual grounding:** character offsets *and* page geometry flow through the pipeline.
- **Provenance-first:** every output is explainable and reproducible (model+config hash).
- **Method agnostic:** rules, ML, LLM/VLM all behind the same interface.
- **Evaluation built-in:** stage and end‑to‑end metrics produced on every run.

---

## 2) Core Artifact Types (lean set)
> All artifacts carry `id`, `type`, `doc_id`, `parents[]`, `meta{}` (artifact-level metadata).

### 2.0 Artifact Metadata Conventions
- `meta.method?`: `{type, name, version, provider, config_ref?}` for describing the producing method (e.g., `llm`, `regex`, `crf`).
- `meta.misc?`: free-form object for additional contextual details (prompts, checkpoints, pipeline tags).
- `meta.probability?`: float in `[0, 1]`; populated when the method produces a calibrated prediction probability (regex/rule systems may omit).
- `meta.reasoning?`: string or structured object with model reasoning or task rationale. Generative methods SHOULD populate this to explain extractions or task framing.

Extraction artifacts that already expose `score` retain it; populate `meta.probability` (and optionally mirror to a top-level `probability?`) when a calibrated probability is available so `score` semantics stay method-specific.

### 2.1 Document
- `doc_id`: string
- `source`: {mime, bytes_ref|path, pages:int}
- `meta`: {ingest_time, checksum}

### 2.2 TextLayer
- `text`: full concatenated text (UTF‑8)
- `tokens[]`: {i, start_char, end_char, text, page, bbox?}
- `reading_order[]`: array of token indices in read sequence

### 2.3 LayoutLayer
- `pages[]`: {page, width, height, dpi}
- `blocks[]/lines[]/words[]`: geometric groups with `bbox|poly`, token refs

### 2.4 SpanRef
- `start_char`, `end_char`, `token_span: [i0, i1)`
- `page_boxes[]`: {page, bbox|poly}

### 2.5 EntityMention (core)
- `label`: string (schema-agnostic type)
- `text`: raw surface string
- `span: SpanRef`
- `score`: float
- `probability?`: float `[0, 1]` (optional calibrated probability; prefer `meta.probability` when carrying both score and probability)
- `reasoning?`: string|list|dict (alias for `meta.reasoning` when surfaced at top level by generative methods)
- `evidence[]`: SpanRef|feature keys

### 2.6 KVPair (core)
- `key_span?`, `value_span: SpanRef`
- `field`: canonical field name
- `normalized_value?`
- `score`
- `probability?`: float `[0, 1]`
- `reasoning?`: string|list|dict (generative explanation optional)

### 2.7 Relation (core)
- `type`: string
- `args[]`: {role, mention_id}
- `score`, `evidence[]`
- `probability?`: float `[0, 1]`
- `reasoning?`: string|list|dict (generative explanation optional)

### 2.8 Trace / Provenance (core)
- `run_id`, `task_id`, `model_id`, `config_hash`, `time{start,end}`
- `parents[]`: input artifact ids
- `features{}`: optional features used for decisions
- `warnings[]`

> **Optional artifact types** (declared but not required by core): `EntityCluster`, `LinkedEntity`, `Table`, `Form`, `Barcode`, `Event`, `Quantity`, `Address`, `Identifier`, `Assertion`, `TemporalScope` (see §7).

---

## 3) Core Task Interfaces
All tasks implement the same minimal interface.

```python
class Task:
    id: str                      # unique within registry
    input_types: list[str]       # accepted artifact types
    output_types: list[str]      # produced artifact types
    params_schema: dict          # JSON Schema for config validation

    def run(self, inputs: dict[str, list[Artifact]],
            params: dict, ctx: RunContext) -> TaskResult: ...

class TaskResult:
    artifacts: list[Artifact]
    trace: Trace                 # one per task invocation
    metrics: dict[str, float]    # stage-local, standardized keys
```

### Core task set (the truly minimal graph)
1. **IngestText**  
   *Input:* `Document`  
   *Output:* `TextLayer`, `LayoutLayer`  
   *Metrics:* coverage%, OCR CER/WER (if OCR), tokenization rate

2. **Structure**  
   *Input:* `TextLayer`, `LayoutLayer`  
   *Output:* enriched `LayoutLayer` (blocks/lines/words), optional `reading_order`  
   *Metrics:* block/line IoU (if refs), token→box alignment%

3. **NER**  
   *Input:* `TextLayer` (+optional `LayoutLayer`)  
   *Output:* `EntityMention[]`  
   *Metrics:* span‑F1 (if gold), coverage%

4. **KeyValueExtraction**  
   *Input:* `TextLayer`, `LayoutLayer`  
   *Output:* `KVPair[]`  
   *Metrics:* EM/F1 per field

5. **RelationExtraction**  
   *Input:* `TextLayer`, `EntityMention[]`  
   *Output:* `Relation[]`  
   *Metrics:* micro/macro F1

6. **NormalizeValidate (Post‑proc)**  
   *Input:* `EntityMention[]`, `KVPair[]`, `Relation[]`  
   *Output:* same types with normalized values + validation flags  
   *Metrics:* normalization success%, constraint violations

> Everything else is optional modules that plug into the DAG and/or extend artifact types.

---

## 4) Orchestration (DAG) & Config
- **Registry:** each task implementation registers under a capability (e.g., `ie.ingest.text/tesseract`, `ie.ner/crf`, `ie.ner/bert`, `ie.ner/llm`).
- **Typed ports:** edges connect `producer.output_type → consumer.input_type`.
- **Config-as-data:** pipeline is a YAML/JSON document validated against a schema.

### Example pipeline config (lean + a few optional modules)
```yaml
version: 0.1
pipeline:
  nodes:
    - id: ingest
      uses: ie.ingest.text/tesseract
      params: {lang: "eng", psm: 6}
    - id: structure
      uses: ie.structure/layout_fast
    - id: ner
      uses: ie.ner/bert
      params: {labels: [PERSON, ORG, LOC, DATE]}
    - id: kv
      uses: ie.kv/rules_tables
    - id: rel
      uses: ie.rel/patterns
    - id: normalize
      uses: ie.post/normalize_validate
    - id: timex        # optional module
      uses: ie.ex.timex/heider_time
    - id: quantities   # optional module
      uses: ie.ex.quant/ud_units
  edges:
    - ingest:TextLayer    -> structure
    - ingest:LayoutLayer  -> structure
    - structure:TextLayer -> ner
    - structure:TextLayer -> kv
    - structure:LayoutLayer -> kv
    - ner:EntityMention   -> rel
    - structure:TextLayer -> rel
    - ner:EntityMention   -> normalize
    - kv:KVPair           -> normalize
    - rel:Relation        -> normalize
    - structure:TextLayer -> timex
    - timex:Temporal      -> normalize
    - structure:TextLayer -> quantities
    - quantities:Quantity -> normalize
run:
  seed: 1337
  batch_size: 4
  capture:
    store_artifacts: [TextLayer, LayoutLayer, EntityMention, KVPair, Relation]
    store_renders: true
  eval:
    dataset: path/to/gold
    metrics: [span_f1, field_em, rel_f1, coverage, latency]
```

---

## 5) Geometry/Text Co‑Propagation Rules
- **Token as the join key:** every decision references token indices; geometry derives from tokens.
- **Span→geometry:** union token boxes; prefer polygons if available; keep page breaks explicit.
- **Transform safety:** page rotations/scales are tracked in `LayoutLayer.pages[]` and applied on render.
- **De‑hyphenation & line‑wrap:** update `TextLayer.tokens` and keep a `token_map` so upstream ids remain traceable.

---

## 6) Provenance & Reproducibility
- Each artifact embeds `provenance` with `(run_id, task_id, model_id, config_hash, parent_ids)`.
- **Run manifest** (JSON): inputs, env (library+model versions), seeds, hardware, cost/latency per node.
- **Determinism:** seed surfaces in config; LLM/VLM adapters expose `temperature`, `nucleus`, retry policy.

---

## 7) Optional Modules Catalog (capabilities)
> Each module follows the core `Task` interface. Inputs/outputs listed for wiring.

### Pre‑Processing
- **ImageCleanup**: de‑skew, dewarp, denoise → improved `LayoutLayer`; metrics: skew°, blur score
- **HybridPDFParsing**: detect embedded text vs raster; selective OCR → `TextLayer`, `LayoutLayer`
- **HandwritingRecognition**: HTR model → `TextLayer.tokens` augmentation
- **BarcodeQR**: decode → `Identifier[]`
- **DocBoundarySplitMerge**: multi‑doc detection & reordering → `Document[]`
- **TokenAlignmentFix**: hyphenation correction → updated `TextLayer` + `token_map`

### Classification
- **TemplateDetection**: layout clustering/ID → `LayoutTemplate`
- **ScriptDirection**: script + LTR/RTL per block → enriched `LayoutLayer`
- **DocClass/PageClass**: labels → `ClassLabel[]`

### Extraction
- **TIMEX**: temporal expressions → `Temporal[]`
- **QuantitiesUnits**: numbers + units → `Quantity[]` (normalized SI)
- **Identifiers**: VAT/IBAN/… with checksum → `Identifier[]`
- **AddressesPhones**: parsed + canonical → `Address[]`, `Phone[]`
- **EventExtraction (light)**: trigger+roles → `Event[]`
- **TableRefinement**: header/stub detection, multi‑page stitching → enriched `Table`
- **TableExtraction** (richer): table cells to `KVPair[]` or `Relation[]`

### Entity Refinement
- **Coreference** (intra‑doc) → `EntityCluster` (mentions→cluster)
- **CrossDocCoref** → corpus‑level `EntityCluster`
- **EntityLinking** → `LinkedEntity` (cluster→KB ids)
- **AssertionNegation** → attach factuality to mentions/fields
- **TemporalScoping** → valid-from/to on attributes/relations
- **EvidenceConsolidation** → per cluster, merged evidence spans

### Data Quality / Post
- **Canonicalize** (names, phones, addresses, dates) → normalized values
- **ConstraintCheck**: cross‑field rules; checksum validators → violation reports
- **EnsemblingAdjudication**: reconcile multiple extractors → final `KVPair[]`/`EntityMention[]`
- **UncertaintyCalibration**: temperature scaling/Platt → calibrated scores
- **Deduplication**: exact/fuzzy doc/page/entity de‑dupe → `EntityCluster` updates

### Ops & Evaluation
- **CoverageCompleteness**: per field coverage → metrics
- **CostLatencyTracker**: compute & $ per node → metrics
- **DriftDetection**: layout/content drift → alerts, version suggestions

---

## 8) Metrics (standard keys)
- `ocr.cer`, `ocr.wer`, `layout.token_box_alignment`
- `ner.span_f1`, `ner.precision`, `ner.recall`
- `kv.em`, `kv.f1`, `kv.coverage`
- `rel.f1`, `rel.precision`, `rel.recall`
- `norm.success_rate`, `dq.violations`
- `coverage.per_field.{FIELD}`
- `latency.{node_id}`, `cost.{node_id}`

---

## 9) Rendering & UX Hooks (non‑blocking)
- `render(artifact, page)` returns overlay shapes (polys) + labels for UI
- `explain(artifact)` exposes features used (e.g., key window tokens, template id)

---

## 10) Registry & Packaging
- **Entry points** (Python): `ie.ingest.text/*`, `ie.structure/*`, `ie.ner/*`, `ie.kv/*`, `ie.rel/*`, `ie.post/*`, `ie.ex.*`, `ie.ops.*`
- **Versioning:** `model_id = {name, version, sha}`; `config_hash = sha256(compiled_params)`
- **Compatibility:** task declares `requires_artifacts` and `produces_artifacts` with versions.

---

## 11) Dataset & Gold Format (for eval)
- **Docs:** raw files + `doc.json` meta
- **Gold artifacts:** JSONL for mentions, kvpairs, relations with `SpanRef` (char + token + boxes)
- **Splits:** train/dev/test; schema version stamped in `dataset.json`

---

## 12) Minimal Compliance Checklist (to ship a method)
- [ ] Implements `Task` interface and validates `params` against schema
- [ ] Emits artifacts with `doc_id`, `parents[]`, and `provenance`
- [ ] Provides `metrics{}` with standard keys
- [ ] Supplies small fixture + golden for CI

---

## 13) Example: Tiny End‑to‑End (text‑only)
1. `IngestText(text_only)` → `TextLayer`
2. `NER(bert)` → `EntityMention[]`
3. `KeyValueExtraction(rules)` → `KVPair[]`
4. `NormalizeValidate` → normalized outputs
5. Eval against gold → span‑F1, EM

> Geometry‑aware runs add `LayoutLayer` and optional modules without changing the core contracts.

---

**This spec keeps the core small while making headroom for everything listed via optional capabilities.** Modify the module catalog or the standard metrics list as needed; the interfaces remain unchanged.

