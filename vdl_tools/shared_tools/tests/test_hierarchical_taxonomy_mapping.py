"""Regression tests for the hierarchical walk's NoMatch refusal capture.

The walk historically dropped the model's refusal output at the first
level: an entity refused at Pillar fell through to the all-null fallback
row with ``reason=None``, losing exactly the rationale post-hoc NoMatch
analysis needs. The response schemas now carry ``no_match_reason`` and
``classify_entities`` persists it on the NoMatch row's ``reason``.
"""

import json

import pandas as pd
import pytest
from pydantic import BaseModel

import vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping as htm
from vdl_tools.shared_tools.taxonomy_mapping.oe_hierarchical_taxonomy_mapping import (
    oneearth_match_schema,
)


LEVELS = [
    {"idx": 0, "name": "Pillar", "sheet": "Pillars",
     "key_col": "Pillar", "output_col": "Pillar"},
    {"idx": 1, "name": "SubPillar", "sheet": "SubPillars",
     "key_col": "Sub-Pillar", "output_col": "Sub-Pillar"},
]

TABLES = {
    0: pd.DataFrame([
        {"Pillar": "Energy", "Definition": "Clean energy work."},
    ]),
    1: pd.DataFrame([
        {"Pillar": "Energy", "Sub-Pillar": "Solar",
         "Definition": "Solar power."},
    ]),
}


class _FakeSession:
    def commit(self):
        pass


class _FakeCache:
    """Stands in for TaxonomyMatchCache; serves canned response JSON."""

    def __init__(self, responses_by_given_id):
        self.responses = responses_by_given_id

    def bulk_get_cache_or_run(self, given_ids_texts, **_kwargs):
        return {
            gid: {"response_text": self.responses[gid]}
            for gid, _ in given_ids_texts
            if gid in self.responses
        }


def _classify(monkeypatch, responses, entities, **kwargs):
    monkeypatch.setattr(
        htm, "TaxonomyMatchCache", lambda **_kw: _FakeCache(responses)
    )
    return htm.classify_entities(
        session=_FakeSession(),
        tables=TABLES,
        levels=LEVELS,
        system_prompt="test prompt",
        entities=entities,
        id_col="id",
        name_col="name",
        text_col="description",
        model="gpt-4.1",
        **kwargs,
    )


def test_pillar_refusal_persists_reason_on_nomatch_row(monkeypatch):
    """A top-level refusal lands the model's no_match_reason in ``reason``."""
    refusal = "A bakery selling bread performs no climate activity."
    entities = pd.DataFrame([
        {"id": "in", "name": "SolarCo", "description": "Builds solar farms."},
        {"id": "out", "name": "Bakery", "description": "Sells bread."},
        {"id": "silent", "name": "Mystery", "description": "???"},
    ])
    responses = {
        "in|Pillar|": json.dumps({
            "matches": [{"index": 1, "evidence": "solar farms",
                         "reason": "builds solar"}],
            "no_match_reason": "",
        }),
        # Refused below Pillar: the walk keeps the Pillar leaf and its
        # match reason — the sub-level refusal must not clobber it.
        "in|SubPillar|Pillar=Energy": json.dumps({
            "matches": [],
            "no_match_reason": "No specific sub-pillar named.",
        }),
        "out|Pillar|": json.dumps({
            "matches": [], "no_match_reason": refusal,
        }),
        # Model refused but gave no rationale: reason stays null.
        "silent|Pillar|": json.dumps({
            "matches": [], "no_match_reason": "",
        }),
    }

    out = _classify(monkeypatch, responses, entities).set_index("id")

    assert out.loc["out", "Pillar"] is None
    assert out.loc["out", "reason"] == refusal

    assert out.loc["silent", "reason"] is None

    assert out.loc["in", "Pillar"] == "Energy"
    assert out.loc["in", "reason"] == "builds solar"


def test_schema_without_no_match_reason_still_walks(monkeypatch):
    """Custom match schemas lacking the field keep the old null behavior."""

    class _BareMatch(BaseModel):
        index: int
        evidence: str = ""
        reason: str = ""

    class _BareResponse(BaseModel):
        matches: list[_BareMatch] = []

    entities = pd.DataFrame([
        {"id": "out", "name": "Bakery", "description": "Sells bread."},
    ])
    responses = {"out|Pillar|": json.dumps({"matches": []})}

    out = _classify(
        monkeypatch, responses, entities, match_schema=_BareResponse,
    ).set_index("id")

    assert out.loc["out", "Pillar"] is None
    assert out.loc["out", "reason"] is None


@pytest.mark.parametrize("schema_kwargs", [
    {},
    {"include_confidence": True},
    {"research": True},
])
def test_oe_schemas_carry_no_match_reason(schema_kwargs):
    """Every OE response variant parses and exposes the refusal field."""
    schema = oneearth_match_schema(**schema_kwargs)
    parsed = schema.model_validate_json(
        '{"matches": [], "no_match_reason": "out of scope"}'
    )
    assert parsed.no_match_reason == "out of scope"
