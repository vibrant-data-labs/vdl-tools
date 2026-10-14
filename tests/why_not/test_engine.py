"""why_not engine: resolution and the stage walk on a synthetic index (no network, no files)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from vdl_tools.shared_tools.why_not import (
    FAIL,
    HIDDEN,
    NOTE,
    PASS,
    SKIP,
    UNKNOWN,
    Ctx,
    PipelineSpec,
    classify_query,
    resolve,
    trace_uid,
)
from vdl_tools.shared_tools.why_not import stages as st
from vdl_tools.shared_tools.why_not.loaders import normalize_domain, normalize_name, parse_verdict


def _spec(stages, **kw) -> PipelineSpec:
    return PipelineSpec(name="t", title="Test Tracker", repo_root=Path("."), index_dir=Path("."), version=1,
                        sources=lambda: [], stages=stages, uid_from_ein=lambda e: [f"gt_{e}", e], **kw)


STAGES = [
    st.pull_stage("pulled", downstream_flags=("in_meta", "in_network"), not_pulled_detail="never pulled"),
    st.text_stage("had text"),
    st.relevance_stage("relevant", override_source="overrides.json"),
    st.texts_stage("texts survived", requires=("relevance",)),
    st.taxonomy_stage("taxonomy", levels={"Tax": ("tax0",)}, nomatch=("No Match",)),
    st.rules_stage("cleaning", reasons=[
        lambda ctx: "too small" if (ctx.v("total_funding") or 0) < 100 else None,
        lambda ctx: "not US" if ctx.v("country") not in (None, "United States") else None,
    ]),
    st.membership_stage("published", key="network", flag="in_network",
                        pass_detail=lambda ctx: f"node {ctx.v('network_name')}"),
    st.visible_stage("visible", visible=lambda ctx: (ctx.v("operating_status") == "active", "status")),
]
SPEC = _spec(STAGES)


def _index() -> pd.DataFrame:
    rows = [
        # uid, name, in_pull, has_text, pred, override, in_meta, tax0, total, country, in_network, status
        ("a", "Oklo Inc", True, True, 1, None, True, "Energy", 500, "United States", True, "active"),
        ("b", "Kalshi", True, True, 0, None, False, None, 500, None, False, "active"),
        ("c", "Tiny Audubon Society", True, True, 1, None, True, "Nature", 50, "United States", False, None),
        ("d", "Ghost Co", False, False, None, None, False, None, None, None, False, None),
        ("e", "Closed Corp", True, True, 1, None, True, "No Match", 900, "United States", True, "closed"),
        ("f", "Overridden Org", True, True, 1, 0, False, None, 900, None, False, None),
        ("g", "Mass Audubon", True, True, 1, None, True, "Nature", 900, "United States", True, "active"),
    ]
    cols = ["uid", "name", "in_pull", "has_relevance_text", "relevance_pred", "relevance_override", "in_meta",
            "tax0", "total_funding", "country", "in_network", "operating_status"]
    df = pd.DataFrame(rows, columns=cols)
    df["name_norm"] = df["name"].map(normalize_name)
    df["alt_norm"] = ""
    df["domain"] = ""
    df["linkedin_slug"] = ""
    df["network_name"] = df["name"].where(df["in_network"], None)
    df["description_len"] = 200
    df["relevance_text"] = "some text"
    df["relevance_text_len"] = 9
    df["relevance_text_source"] = "description"
    df["source"] = "Crunchbase"
    return df


@pytest.fixture
def index():
    return _index()


def test_normalizers():
    assert normalize_name("The Nature Conservancy, Inc.") == "nature conservancy"
    assert normalize_domain("https://www.Example.org/path?x=1") == "example.org"
    assert normalize_domain("Oklo Inc") == ""
    assert parse_verdict("1 ") == 1 and parse_verdict("0-proof") is None


def test_classify_query():
    assert classify_query("121234567") == ("ein", "12-1234567")
    assert classify_query("https://www.linkedin.com/company/oklo/") == ("linkedin", "oklo")
    assert classify_query("oklo.com") == ("url", "oklo.com")
    assert classify_query("Oklo")[0] == "name"


def test_exact_name_resolves_and_traces(index):
    res = resolve(index, "Oklo", SPEC)
    assert res["found"] and res["auto"] and res["candidates"][0]["uid"] == "a"
    tr = trace_uid(index, "a", SPEC)
    assert [s["status"] for s in tr["stages"]] == [PASS, PASS, PASS, PASS, PASS, PASS, PASS, PASS]
    assert tr["exit"]["summary"] == "in the tracker"


def test_partial_name_is_listed_not_traced(index):
    res = resolve(index, "Audubon", SPEC)
    assert not res["found"] and res["partial"]
    assert {c["uid"] for c in res["candidates"]} == {"c", "g"}


def test_lookalike_is_not_found(index):
    res = resolve(index, "Climeworks", SPEC)
    assert not res["found"] and not res["partial"]


def test_ein_maps_through_spec(index):
    index.loc[index["uid"] == "g", "uid"] = "gt_12-3456789"
    res = resolve(index, "123456789", SPEC)
    assert res["found"] and res["candidates"][0]["uid"] == "gt_12-3456789"


def test_relevance_drop(index):
    tr = trace_uid(index, "b", SPEC)
    assert tr["exit"]["stage"] == "relevance"
    assert tr["stages"][2]["status"] == FAIL and "NOT relevant" in tr["stages"][2]["detail"][0]
    assert tr["stages"][3]["status"] == SKIP


def test_override_wins_over_classifier(index):
    tr = trace_uid(index, "f", SPEC)
    assert tr["exit"]["stage"] == "relevance"
    assert "overrides.json" in tr["stages"][2]["detail"][0]


def test_cleaning_reason_and_network_follow(index):
    tr = trace_uid(index, "c", SPEC)
    assert tr["exit"]["stage"] == "cleaning" and tr["stages"][5]["detail"] == ["too small"]
    assert tr["stages"][6]["status"] == FAIL and "removed at 'cleaning'" in tr["stages"][6]["detail"][0]


def test_hidden_and_no_match_note(index):
    tr = trace_uid(index, "e", SPEC)
    assert tr["stages"][4]["status"] == NOTE
    assert tr["stages"][7]["status"] == HIDDEN and tr["exit"]["status"] == HIDDEN


def test_never_pulled(index):
    tr = trace_uid(index, "d", SPEC)
    assert tr["stages"][0]["status"] == FAIL and tr["exit"]["stage"] == "pull"
    assert all(s["status"] == SKIP for s in tr["stages"][1:])


def test_pull_newer_than_enrichment_is_unknown_not_fail(index):
    index.loc[index["uid"] == "b", ["has_relevance_text", "relevance_pred"]] = [False, None]
    meta = {"lineage": {"enrichment_behind_pull": True, "pull_created_at": "2026-09-21", "meta_created_at": "2026-08-05"}}
    tr = trace_uid(index, "b", SPEC, meta)
    assert tr["stages"][1]["status"] == UNKNOWN and "not classified yet" in tr["stages"][1]["detail"][0]
    assert tr["exit"]["status"] == UNKNOWN


def test_merged_into_detail(index):
    spec = _spec([st.membership_stage("clean", key="clean", flag="in_clean")], in_tracker_stage="clean")
    index["in_clean"] = index["in_network"]
    index["merged_into"] = None
    index.loc[index["uid"] == "c", "merged_into"] = "g"
    tr = trace_uid(index, "c", spec)
    assert "merged into `g`" in tr["stages"][0]["detail"][0]


def test_ctx_helpers(index):
    ctx = Ctx(row=index.iloc[0], lineage={}, stages=[], spec=SPEC)
    assert ctx.v("missing_column") is None and ctx.flag("in_pull") is True


def test_exact_uid_resolves_even_with_a_prefix(index):
    index.loc[index["uid"] == "g", "uid"] = "givingtuesday_12-3456789"
    res = resolve(index, "givingtuesday_12-3456789", SPEC)
    assert res["found"] and res["auto"] and res["kind"] == "uid"


def test_taxonomy_drop_when_missing(index):
    spec = _spec([st.taxonomy_stage("tax", levels={"L0": ("tax0",), "L1": ("tax1",)},
                                    drop_when_missing={"L0": "no pillar", "L1": "pillar but no sub-pillar"})])
    index["tax1"] = None
    tr = trace_uid(index, "a", spec)                      # pillar, no sub-pillar
    assert tr["stages"][0]["status"] == FAIL and tr["stages"][0]["detail"][0] == "pillar but no sub-pillar"
    index.loc[index["uid"] == "a", "tax1"] = "Sub"
    assert trace_uid(index, "a", spec)["stages"][0]["status"] == PASS
    index.loc[index["uid"] == "a", ["tax0", "tax1"]] = [None, None]
    assert trace_uid(index, "a", spec)["stages"][0]["detail"][0] == "no pillar"
