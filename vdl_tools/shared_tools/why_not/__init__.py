"""why_not: explain why an organization is, or is not, in a VDL tracker.

A pipeline declares a ``PipelineSpec`` (where each stage's output lives, how
its ids look, the ordered stage rules, the serving schema, the live sources)
and gets a command line that joins those sources into one per-org index,
resolves a name / uid / EIN / URL, walks the stages, and, when the org was
never pulled, looks it up live. See ``spec.py`` for the declarations and
``stages.py`` for the reusable stage rules.
"""
from vdl_tools.shared_tools.why_not.cli import main, run
from vdl_tools.shared_tools.why_not.index import build_index, default_lineage_summary, load_index
from vdl_tools.shared_tools.why_not.spec import (
    FAIL,
    HIDDEN,
    NOTE,
    PASS,
    SKIP,
    UNKNOWN,
    ClassifySpec,
    Ctx,
    PipelineSpec,
    ProbeSpec,
    ServingSpec,
    Source,
    Stage,
)
from vdl_tools.shared_tools.why_not.trace import classify_query, resolve, trace_uid
