"""Artifact data structures used throughout the IE pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Iterable, List, Mapping, MutableMapping, Tuple

from .trace import Trace


@dataclass(frozen=True)
class BoundingBox:
    """Axis-aligned bounding box."""

    x0: float
    y0: float
    x1: float
    y1: float

    def width(self) -> float:
        return max(0.0, self.x1 - self.x0)

    def height(self) -> float:
        return max(0.0, self.y1 - self.y0)

    def area(self) -> float:
        return self.width() * self.height()


@dataclass(frozen=True)
class PageGeometry:
    """Page dimensions and resolution."""

    page: int
    width: float
    height: float
    dpi: float | None = None


@dataclass(frozen=True)
class PageBox:
    """A span of geometry on a page."""

    page: int
    bbox: BoundingBox | None = None
    polygon: Tuple[Tuple[float, float], ...] | None = None

    def __post_init__(self) -> None:
        if self.bbox is None and self.polygon is None:
            raise ValueError("PageBox requires either bbox or polygon")


@dataclass
class Artifact:
    """Base class for all typed artifacts."""

    id: str
    doc_id: str
    parents: Tuple[str, ...] = field(default_factory=tuple)
    meta: MutableMapping[str, Any] = field(default_factory=dict)
    provenance: Trace | None = None

    TYPE: ClassVar[str] = "Artifact"

    def __post_init__(self) -> None:
        object.__setattr__(self, "parents", tuple(self.parents))

    @property
    def type(self) -> str:
        return self.TYPE


@dataclass
class DocumentSource:
    mime: str
    bytes_ref: str | None = None
    path: str | None = None
    pages: int | None = None


@dataclass
class Document(Artifact):
    """Represents an ingested document."""

    TYPE: ClassVar[str] = "Document"
    source: DocumentSource | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.source is None:
            raise ValueError("Document requires a source description")


@dataclass
class TextToken:
    i: int
    start_char: int
    end_char: int
    text: str
    page: int | None = None
    bbox: BoundingBox | None = None


@dataclass
class TextLayer(Artifact):
    """Full document text and tokenisation."""

    TYPE: ClassVar[str] = "TextLayer"
    text: str = ""
    tokens: List[TextToken] = field(default_factory=list)
    reading_order: List[int] | None = None

    def token_by_index(self, index: int) -> TextToken:
        return self.tokens[index]


@dataclass
class LayoutElement:
    """Layout grouping with optional bounding boxes and token references."""

    id: str
    bbox: BoundingBox | None = None
    polygon: Tuple[Tuple[float, float], ...] | None = None
    token_indices: Tuple[int, ...] = ()

    def __post_init__(self) -> None:
        self.token_indices = tuple(self.token_indices)


@dataclass
class LayoutLayer(Artifact):
    """Captures document geometry and grouping."""

    TYPE: ClassVar[str] = "LayoutLayer"
    pages: List[PageGeometry] = field(default_factory=list)
    blocks: List[LayoutElement] = field(default_factory=list)
    lines: List[LayoutElement] = field(default_factory=list)
    words: List[LayoutElement] = field(default_factory=list)
    token_map: Dict[int, int] | None = None


@dataclass
class SpanRef(Artifact):
    """A span linking character offsets to layout geometry."""

    TYPE: ClassVar[str] = "SpanRef"
    start_char: int = 0
    end_char: int = 0
    token_span: Tuple[int, int] = (0, 0)
    page_boxes: Tuple[PageBox, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.page_boxes = tuple(self.page_boxes)


@dataclass
class EntityMention(Artifact):
    """Surface mention detected by NER."""

    TYPE: ClassVar[str] = "EntityMention"
    label: str = ""
    text: str = ""
    span: SpanRef | None = None
    score: float | None = None
    evidence: Tuple[SpanRef | str | Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.evidence = tuple(self.evidence)


@dataclass
class KVPair(Artifact):
    """Key-value pair extracted from the document."""

    TYPE: ClassVar[str] = "KVPair"
    field: str = ""
    value_span: SpanRef | None = None
    key_span: SpanRef | None = None
    normalized_value: Any | None = None
    score: float | None = None


@dataclass
class RelationArgument:
    role: str
    mention_id: str


@dataclass
class Relation(Artifact):
    """Relation between entity mentions."""

    TYPE: ClassVar[str] = "Relation"
    type: str = ""
    args: Tuple[RelationArgument, ...] = ()
    score: float | None = None
    evidence: Tuple[SpanRef | str | Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        self.args = tuple(self.args)
        self.evidence = tuple(self.evidence)


ArtifactLike = Artifact
ArtifactIterable = Iterable[ArtifactLike]
