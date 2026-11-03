import json

import pandas as pd
import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import (
    MatchPolicy,
    map_subjects,
    question_sets,
    select_matches,
)
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.exports import export_extraction
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.subjects import build_activity_subjects

from .conftest import COLUMNS, STEWARDSHIP_SECTOR, fake_jev, map_and_export, score_activities


def test_exclusions_and_full_path_identity(taxonomy):
    tax = taxonomy(duplicate_names=True)
    repeated = tax.nodes[tax.nodes.name == "Repeated"]
    assert len(repeated) == 2 and repeated.node_id.nunique() == 2
    assert len(taxonomy(excluded=True).nodes) == 3
    assert set(tax.nodes.depth) == {0, 1, 2}


def test_lower_tier_only_applies_to_empty_activities(taxonomy, extraction):
    tax = taxonomy(duplicate_names=True)
    p = {r.node_id: 0.1 for r in tax.nodes.itertuples()}
    p[tax.nodes[(tax.nodes.Pillar == "Energy") & (tax.nodes.depth == 0)].node_id.iloc[0]] = 0.8
    p[tax.nodes[(tax.nodes.Pillar == "Nature") & (tax.nodes.depth == 2)].node_id.iloc[0]] = 0.6
    scores = score_activities(extraction, tax, request_runner=fake_jev(p))
    paths = select_matches(scores, tax, MatchPolicy())
    assert set(paths.Pillar) == {"Energy"}
    assert set(paths.depth) == {0}
    changed = select_matches(scores, tax, MatchPolicy((0.5,)))
    assert set(changed.Pillar) == {"Energy", "Nature"}


def test_parent_names_do_not_collide(taxonomy, extraction):
    tax = taxonomy(duplicate_names=True)
    p = {r.node_id: 0.1 for r in tax.nodes.itertuples()}
    p[tax.nodes[(tax.nodes.Pillar == "Energy") & (tax.nodes.depth == 2)].node_id.iloc[0]] = 0.9
    p[tax.nodes[(tax.nodes.Pillar == "Nature") & (tax.nodes.depth == 1)].node_id.iloc[0]] = 0.8
    scored = score_activities(extraction, tax, request_runner=fake_jev(p))
    paths = select_matches(scored, tax, MatchPolicy())
    assert len(paths) == 4
    assert set(paths.depth) == {1, 2}
    leaf = paths[paths.depth == 2].iloc[0]
    assert leaf.ancestor_matches[0]["assignment"] == "implied"
    assert leaf.ancestor_matches[-1]["assignment"] == "own"


def test_drawdown_descent_adds_child_and_grandchild(taxonomy, extraction):
    tax = taxonomy("drawdown")
    p = {r.node_id: (0.8 if r.depth == 0 else 0.65) for r in tax.nodes.itertuples()}
    scored = score_activities(extraction, tax, request_runner=fake_jev(p))
    assert set(select_matches(scored, tax, MatchPolicy()).depth) == {0}
    paths = select_matches(scored, tax, MatchPolicy(descend=0.6))
    assert set(paths.depth) == {2} and set(paths.match_origin) == {"descend"}


def test_drawdown_stewardship_criteria_attach_to_the_named_nodes(taxonomy):
    tax = taxonomy("drawdown")
    with_criteria = tax.nodes[tax.nodes.question.map(lambda q: "criteria" in q)]
    assert set(with_criteria.name) == {
        "Nature-Based Carbon Removal",
        "Protect, Manage, & Restore Ecosystems",
    }


def test_partial_request_never_becomes_a_match(taxonomy, extraction):
    tax = taxonomy()

    def broken(questions, pairs, **kwargs):
        out = fake_jev()(questions, pairs)
        first = next(iter(out))
        answers = json.loads(out[first]["response_text"])
        answers.pop(next(iter(answers)))
        out[first]["response_text"] = json.dumps(answers)
        return out

    scores = score_activities(extraction, tax, request_runner=broken)
    paths = select_matches(scores, tax, MatchPolicy())
    assert scores.usage.error.notna().sum() == 1
    assert paths.subject_id.nunique() == 1


@pytest.mark.parametrize("bad", [True, None, "0.9", float("nan"), -0.1, 1.1])
def test_invalid_probabilities_are_failed_requests(taxonomy, extraction, bad):
    tax = taxonomy()
    bad_scores = score_activities(
        extraction, tax, request_runner=fake_jev({tax.nodes.node_id.iloc[0]: bad})
    )
    assert bad_scores.usage.error.notna().all()
    assert select_matches(bad_scores, tax, MatchPolicy()).empty


def test_question_groups_are_bounded_and_cover_each_node_once(taxonomy):
    import tiktoken

    tax = taxonomy()
    # Taxonomy wording may contain a tokenizer's special-token spelling as plain text.
    tax.nodes.at[0, "question"] = {"type": "noul", "instructions": "Does <|endoftext|> apply?"}
    groups = question_sets(tax, max_tokens=90)
    assert len(groups) > 1
    assert [node for g in groups.values() for node in g] == tax.nodes.node_id.tolist()
    encoding = tiktoken.get_encoding("cl100k_base")
    assert all(
        len(encoding.encode(json.dumps(group, sort_keys=True), disallowed_special=())) <= 90
        for group in groups.values()
    )
    with pytest.raises(ValueError, match="exceeds request budget"):
        question_sets(tax, max_tokens=1)


@pytest.mark.parametrize("name", ["one_earth", "drawdown"])
def test_union_export_evidence_and_funding(taxonomy, extraction, organizations, name):
    tax = taxonomy(name)
    mapped = map_and_export(organizations, extraction, tax, request_runner=fake_jev())
    assert len(mapped.organizations) == len(organizations)
    assert mapped.organizations.id.tolist() == organizations.id.tolist()
    assert mapped.organizations.Sector.tolist() == organizations.Sector.tolist()
    assert len(mapped.activity_paths) == 4  # Both core and minor reach both branches.
    hit = mapped.paths[mapped.paths.id == "1"]
    assert hit.n_activities.tolist() == [2, 2]
    assert all(len(v) == 2 for v in hit.supporting_activity_ids)
    assert all(len(v) == 2 for v in hit.activity_evidence)
    assert mapped.funding.groupby("id").FundingFrac.sum().to_dict() == {"1": 1.0}
    assert mapped.funding.FundingFrac.tolist() == [0.5, 0.5]
    assert "level3" not in mapped.funding
    assert mapped.organizations[f"all_level3_{name}_category"].tolist() == [[], [], [], []]
    assert set(mapped.paths.mapping_status) == {
        "matched",
        "missing_text",
        "request_failed",
        "empty",
    }


def test_all_unmatched_cohort(taxonomy, extraction, organizations):
    tax = taxonomy()
    p = dict.fromkeys(tax.nodes.node_id, 0.1)
    result = map_and_export(organizations, extraction, tax, request_runner=fake_jev(p))
    assert len(result.paths) == 4 and result.funding.empty
    assert set(result.paths.deepest_match) == {"NoMatch"}


def test_stored_scores_cannot_be_applied_to_changed_taxonomy(taxonomy, extraction):
    from dataclasses import replace

    tax = taxonomy()
    scored = score_activities(extraction, tax, request_runner=fake_jev())
    with pytest.raises(ValueError, match="different taxonomy"):
        select_matches(scored, replace(tax, sha256="changed"), MatchPolicy())


def test_serving_explanations_keep_supporting_activities(taxonomy, extraction, organizations):
    from vdl_tools.shared_tools.taxonomy_mapping.mapping_evidence import assignment_explanations

    tax = taxonomy()
    result = map_and_export(organizations, extraction, tax, request_runner=fake_jev())
    mappings = result.paths.rename(columns={c: f"level{i}" for i, c in enumerate(tax.columns)})
    assignments = result.funding.rename(columns={"id": "org_uid"})
    explanations = assignment_explanations(assignments, mappings, source="fixture.json")
    payload = json.loads(explanations.iloc[0])[0]
    assert len(payload["supporting_activity_ids"]) == 2
    assert len(payload["activity_evidence"]) == 2
    assert payload["policy_sha256"] == result.lineage["policy_sha256"]
    assert payload["activity_evidence"][0]["ancestor_matches"]


def test_core_first_rollup_keeps_core_matches_and_falls_back(taxonomy, extraction, organizations):
    tax = taxonomy("drawdown")
    energy = set(tax.nodes.loc[tax.nodes.Sector == "Energy", "node_id"])

    def gateway(core_hits):
        """Org 1's core activity ("solar work 0") scores high only on ``core_hits``; its minor one on every node."""

        def run(questions, pairs, **kwargs):
            return {
                key: {
                    "response_text": json.dumps(
                        {
                            node: {"noul": 0.8 if "solar work 1" in state or node in core_hits else 0.1}
                            for node in questions
                        }
                    )
                }
                for key, state in pairs
            }

        return run

    def export(core_hits, rollup):
        mapped = map_subjects(
            build_activity_subjects(extraction.activities), tax, policy=MatchPolicy(), request_runner=gateway(core_hits)
        )
        return export_extraction(
            organizations, extraction, tax, mapped, **COLUMNS, mapping_name="dd", rollup=rollup
        )

    union = export(energy, "union")
    core = export(energy, "core_first")
    org = lambda result: result.paths[result.paths.id == "1"]  # noqa: E731
    assert set(org(union).Sector) == {"Energy", STEWARDSHIP_SECTOR}
    assert set(org(core).Sector) == {"Energy"}  # the minor activity's other Sector is dropped
    assert org(core).n_activities.tolist() == [1] * len(org(core))
    assert set(core.funding.level0) == {"Energy"}
    assert core.funding.FundingFrac.sum() == pytest.approx(1)
    pd.testing.assert_frame_equal(core.activity_paths, union.activity_paths)  # per-activity results keep every match
    assert core.organizations.drawdown_mapping_status.tolist() == union.organizations.drawdown_mapping_status.tolist()
    assert (union.lineage["rollup"], core.lineage["rollup"]) == ("union", "core_first")
    # When the core activity matches nothing, the next prominence group (here the minor activity) is used.
    fallback = export(set(), "core_first")
    assert set(org(fallback).Sector) == {"Energy", STEWARDSHIP_SECTOR}
    with pytest.raises(ValueError, match="Unknown rollup"):
        export(energy, "core_secondary")
