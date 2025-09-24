#!/usr/bin/env python
"""Generate an interactive visual overview of the core data model."""

from __future__ import annotations

import argparse
import html
import inspect
import json
import sys
import types
from collections import defaultdict
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, MutableMapping, Tuple, Type, get_args, get_origin, get_type_hints

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ief import artifacts
from ief import trace as trace_module

MODULES = (artifacts, trace_module)


def _collect_dataclasses() -> Dict[Type[Any], str]:
    classes: Dict[Type[Any], str] = {}
    for module in MODULES:
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            if not is_dataclass(obj):
                continue
            classes[obj] = name
    return classes


def _format_annotation(annotation: Any) -> str:
    origin = get_origin(annotation)
    if origin is None:
        if annotation is type(None):
            return "None"
        if isinstance(annotation, type):
            return annotation.__name__
        text = str(annotation)
        for prefix in ("typing.", "ief.artifacts.", "ief.trace.", "collections.abc."):
            text = text.replace(prefix, "")
        return text

    args = [
        _format_annotation(arg)
        for arg in get_args(annotation)
        if arg is not Ellipsis
    ]

    union_type = getattr(sys.modules["typing"], "Union", None)
    if origin in {types.UnionType, union_type}:
        return " | ".join(args)

    name = getattr(origin, "__name__", str(origin))
    for prefix in ("typing.", "collections.abc."):
        name = name.replace(prefix, "")
    if not args:
        return name
    return f"{name}[{', '.join(args)}]"


def _iter_related(annotation: Any, dataclasses: MutableMapping[Type[Any], str]) -> Iterable[Type[Any]]:
    origin = get_origin(annotation)
    if origin is None:
        if isinstance(annotation, type) and annotation in dataclasses:
            yield annotation
        return

    for arg in get_args(annotation):
        if arg is Ellipsis or arg is type(None):
            continue
        if isinstance(arg, type) and arg in dataclasses:
            yield arg
        else:
            yield from _iter_related(arg, dataclasses)


def build_graph_data() -> Dict[str, Any]:
    dataclasses = _collect_dataclasses()
    artifact_base = artifacts.Artifact

    field_text: Dict[str, list[dict[str, str]]] = {}
    edges: Dict[Tuple[str, str], set[str]] = defaultdict(set)

    for cls, name in dataclasses.items():
        module = sys.modules[cls.__module__]
        type_hints = get_type_hints(cls, globalns=vars(module))
        summaries: list[dict[str, str]] = []
        for field in fields(cls):
            annotation = type_hints.get(field.name, field.type)
            type_text = _format_annotation(annotation)
            summaries.append({
                "name": field.name,
                "type": type_text,
                "type_html": html.escape(type_text),
            })
            for related in _iter_related(annotation, dataclasses):
                edges[(name, dataclasses[related])].add(field.name)
        field_text[name] = summaries

        base = cls.__mro__[1]
        if base in dataclasses and base is not cls:
            edges[(name, dataclasses[base])].add("inherits")

    nodes = []
    for cls, name in dataclasses.items():
        raw_doc = inspect.getdoc(cls) or ""
        doc_html = html.escape(raw_doc)
        tooltip_lines = []
        if raw_doc:
            tooltip_lines.append(raw_doc)
        tooltip_lines.extend(
            f"{entry['name']}: {entry['type']}" for entry in field_text[name]
        )
        group: str
        if cls is artifact_base:
            group = "artifact_base"
        elif issubclass(cls, artifact_base):
            group = "artifact"
        elif cls.__module__ == trace_module.__name__:
            group = "trace"
        else:
            group = "support"
        nodes.append({
            "id": name,
            "group": group,
            "doc": raw_doc,
            "doc_html": doc_html,
            "fields": field_text[name],
            "tooltip": "\n".join(tooltip_lines),
        })

    links = []
    for (source, target), names in edges.items():
        links.append({
            "source": source,
            "target": target,
            "fields": sorted(names),
        })

    return {"nodes": nodes, "links": links}


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>IEF Data Model Overview</title>
  <style>
    body { margin: 0; font-family: 'Inter', 'Segoe UI', Helvetica, Arial, sans-serif; background-color: #0b0c10; color: #f8f9fa; }
    #graph { width: 100vw; height: 100vh; }
    #info { position: fixed; top: 16px; left: 16px; max-width: 360px; padding: 12px 16px; background: rgba(0, 0, 0, 0.65); border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.15); overflow-y: auto; max-height: 80vh; }
    #info h1 { margin: 0 0 12px; font-size: 1.25rem; }
    #info h2 { margin: 8px 0 4px; font-size: 1.1rem; }
    #info ul { padding-left: 20px; margin: 4px 0 0; }
    #info li { margin-bottom: 4px; }
    code { background: rgba(255, 255, 255, 0.08); padding: 1px 4px; border-radius: 4px; }
    a { color: #7fc7ff; }
  </style>
</head>
<body>
  <div id=\"graph\"></div>
  <div id=\"info\">
    <h1>IEF Data Model</h1>
    <p>Drag nodes to explore relationships between artifacts, supporting structures, and provenance traces. Click a node for field details.</p>
    <p>Edge tooltips list the field names that connect two dataclasses. Inheritance edges are labelled <em>inherits</em>.</p>
  </div>
  <script src=\"https://unpkg.com/force-graph@1.45.0/dist/force-graph.js\"></script>
  <script>
    const DATA = __DATA__;
    const graphElem = document.getElementById('graph');
    const infoElem = document.getElementById('info');

    const Graph = ForceGraph()(graphElem)
      .graphData(DATA)
      .nodeId('id')
      .nodeLabel(node => node.tooltip)
      .nodeAutoColorBy('group')
      .nodeVal(node => node.group === 'artifact_base' ? 10 : node.group === 'artifact' ? 6 : node.group === 'trace' ? 5 : 4)
      .linkDirectionalArrowLength(8)
      .linkDirectionalArrowRelPos(1)
      .linkCurvature(0.2)
      .linkLabel(link => `${link.source.id} → ${link.target.id}: ${link.fields.join(', ')}`)
      .onNodeClick(node => renderDetails(node))
      .onNodeHover(node => graphElem.style.cursor = node ? 'pointer' : 'default');

    Graph.d3Force('charge').strength(-220);

    function renderDetails(node) {
      const fieldItems = node.fields
        .map(entry => `<li><code>${entry.name}</code>: ${entry.type_html}</li>`)
        .join('');
      const doc = node.doc_html ? `<p>${node.doc_html}</p>` : '';
      infoElem.innerHTML = `
        <h1>${node.id}</h1>
        ${doc}
        <h2>Fields</h2>
        <ul>${fieldItems}</ul>
      `;
    }

    window.addEventListener('resize', () => {
      Graph.width(window.innerWidth);
      Graph.height(window.innerHeight);
    });
  </script>
</body>
</html>
"""


def write_html(graph_data: Dict[str, Any], output_path: Path) -> Path:
    json_data = json.dumps(graph_data, indent=2)
    safe_json = json_data.replace('</', '<\/')
    html_content = HTML_TEMPLATE.replace('__DATA__', safe_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content, encoding='utf-8')
    return output_path


def main() -> None:
    default_output = Path(__file__).resolve().parents[1] / "visualizations" / "data_model_overview.html"
    parser = argparse.ArgumentParser(description="Generate interactive data model visualisation.")
    parser.add_argument("--output", type=Path, default=default_output, help="Destination HTML file.")
    args = parser.parse_args()

    graph_data = build_graph_data()
    output = write_html(graph_data, args.output)
    print(f"Wrote interactive data model overview to {output}")


if __name__ == "__main__":
    main()
