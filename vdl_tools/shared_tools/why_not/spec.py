"""Declarations a pipeline makes so the why_not engine can explain its drops.

A pipeline (the CFT, the LFT, ...) describes itself with a ``PipelineSpec``:
where each stage's output lives (``Source``), how to recognise its ids, the
ordered stage rules to walk, what the serving schema looks like, and which live
sources to consult when an org was never pulled. Everything that names a file,
a prompt, a threshold or a stage order belongs in the spec; the engine only
knows how to build an index from sources, resolve a query, walk stages and
render a report.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

PASS, FAIL, HIDDEN, SKIP, UNKNOWN, NOTE = "pass", "fail", "hidden", "skip", "unknown", "note"
STATUS_WORD = {PASS: "PASS", FAIL: "FAIL", HIDDEN: "HIDDEN", SKIP: "skip", UNKNOWN: "unknown", NOTE: "note"}


@dataclass
class Stage:
    key: str
    label: str
    status: str
    detail: list[str] = field(default_factory=list)


@dataclass
class Ctx:
    """What a stage rule sees: the org's index row, the run lineage, prior stages."""
    row: pd.Series
    lineage: dict
    stages: list[Stage]
    spec: PipelineSpec

    def v(self, col: str):
        """Cell value or None (pd.NA / NaN / missing column all -> None)."""
        if col not in self.row.index:
            return None
        val = self.row[col]
        if val is None or val is pd.NA:
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        return val

    def flag(self, col: str) -> bool:
        return bool(self.v(col))

    def prev(self, key: str) -> Stage | None:
        return next((s for s in self.stages if s.key == key), None)

    def passed(self, key: str) -> bool:
        st = self.prev(key)
        return st is not None and st.status == PASS


StageRule = Callable[[Ctx], Stage]


@dataclass
class Source:
    """One stage artifact and how to fold it into the index.

    kind
      ``frame``: a parquet / json-records table with a uid column. ``columns``
      maps source columns to index columns (the uid column must map to
      ``uid``); ``membership`` names the boolean flag set for every uid present
      (``in_pull`` ...); ``snip`` lists index columns to keep as a snippet plus
      a ``<col>_len`` count.
      ``uid_set``: an xlsx / json whose uids become the boolean ``flag`` column.
      For xlsx, ``uid_col`` names the id column and ``row_filter`` can keep a
      subset (e.g. ``remove == 1``); for json dicts, ``json_value`` keeps the
      keys with that value.
      ``uid_map``: a json dict mapped onto ``column`` by uid.
      ``relevance_cache``: the shared prompt cache; ``prompt_name`` + ``model``
      select the pipeline's classifier. Yields ``relevance_*`` columns and the
      ``membership`` flag (``has_relevance_text`` by default).
      ``custom``: ``load()`` returns a frame / set / dict handled like the
      kinds above (``as_kind`` says which); ``fingerprint()`` says when it
      changed.
    ``uri`` is read first, ``fallback`` when that fails. ``required`` sources
    abort the build when neither can be read.
    """
    key: str
    label: str
    kind: str
    uri: str | None = None
    fallback: str | None = None
    required: bool = False
    # frame
    columns: dict[str, str] | None = None
    uid_col: str = "uid"
    membership: str | None = None
    snip: tuple[str, ...] = ()
    # uid_set
    flag: str | None = None
    row_filter: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    json_value: Any = None
    # uid_map
    column: str | None = None
    value_map: Callable[[Any], Any] | None = None
    # relevance_cache
    prompt_name: str | None = None
    model: str | None = None
    # custom
    load: Callable[[], Any] | None = None
    as_kind: str = "frame"
    fingerprint: Callable[[], list] | None = None


@dataclass
class ServingSpec:
    """How to read the app's serving schema for a live check (``--schema``)."""
    default_schema: str | None = None
    table: str = "mv_org_table"
    columns: tuple[str, ...] = ("uid", "name", "data_source", "operating_status", "total_funding", "last_year_funded")


@dataclass
class ClassifySpec:
    """The pipeline's own relevance classifier, for ``--classify``."""
    model: str
    prompt_name: str
    system_prompt: str | None = None
    prompt_format: str | None = None


@dataclass
class ProbeSpec:
    """Live lookups for an org the pipeline never pulled."""
    sources: tuple[str, ...] = ("crunchbase", "gt")
    company_terms: Callable[[], list[str]] | None = None    # tested against CB / NZI descriptions
    nonprofit_terms: Callable[[], list[str]] | None = None  # tested against 990 text
    pull_min_funding_year: int = 2010
    cb_requires_active: bool = False
    cb_eligibility: Callable[[dict], tuple[bool, list[str]]] | None = None  # override the default replay
    nonprofit_floors: dict[str, int] = field(default_factory=dict)         # label -> dollars, reported on GT hits
    classify: ClassifySpec | None = None


@dataclass
class PipelineSpec:
    name: str                      # short id: "cft", "lft"
    title: str                     # "US Climate Finance Tracker"
    repo_root: Path
    index_dir: Path
    version: int                   # bump when sources / columns change; forces a rebuild
    sources: Callable[[], list[Source]]
    stages: list[StageRule]
    uid_from_ein: Callable[[str], list[str]]           # "NN-NNNNNNN" -> candidate uids
    uid_from_uuid: Callable[[str], list[str]] = lambda u: [u]
    name_columns: tuple[str, ...] = ("name", "profile_name")
    website_columns: tuple[str, ...] = ("website",)
    linkedin_columns: tuple[str, ...] = ("linkedin",)
    int_columns: tuple[str, ...] = ("year_last_funded", "relevance_pred", "relevance_override", "relevance_text_len")
    float_columns: tuple[str, ...] = ("total_funding", "relevance_prob")
    finalize: Callable[[pd.DataFrame, dict], pd.DataFrame] | None = None   # (index, lineage_by_key) -> index
    lineage: Callable[[dict], dict] | None = None                           # lineage_by_key -> summary
    counts: Callable[[pd.DataFrame], dict] | None = None
    summary: Callable[[Ctx], dict] | None = None                            # org summary for the report
    hints: Callable[[dict], list[str]] | None = None                        # trace -> next steps
    header_notes: Callable[[dict], list[str]] | None = None                 # index meta -> notes
    in_tracker_stage: str = "network"                                       # the stage whose PASS means "in the tracker"
    serving: ServingSpec = field(default_factory=ServingSpec)
    probe: ProbeSpec = field(default_factory=ProbeSpec)
    constants: dict[str, Any] = field(default_factory=dict)

    @property
    def index_path(self) -> Path:
        return self.index_dir / "index.parquet"

    @property
    def index_meta_path(self) -> Path:
        return self.index_dir / "index.meta.json"
