import json

import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import (
    MatchPolicy,
    question_sets,
    select_matches,
)

from .conftest import fake_jev, map_and_export, score_activities


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
    tax = taxonomy()
    groups = question_sets(tax, max_tokens=90)
    assert len(groups) > 1
    assert [node for g in groups.values() for node in g] == tax.nodes.node_id.tolist()
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
