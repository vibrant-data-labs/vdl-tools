"""Generality and loader isolation without inference or database access."""

import json
import os
import subprocess
import sys
from dataclasses import replace

import pandas as pd
import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import MatchPolicy, select_matches
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.pipeline import (
    group_matches,
    map_subjects,
)
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.taxonomies import load_workbook

from .conftest import fake_jev


@pytest.fixture
def custom_taxonomy(tmp_path):
    def make(depth):
        columns = tuple(f"Education stage {i}" for i in range(depth))
        sheets = tuple(f"Sheet {i}" for i in range(depth))
        path = tmp_path / f"education-{depth}.xlsx"
        with pd.ExcelWriter(path) as writer:
            for i, sheet in enumerate(sheets):
                pd.DataFrame(
                    [
                        {
                            **{c: f"Branch {j}" for j, c in enumerate(columns[: i + 1])},
                            "Definition": f"Work at stage {i}",
                        }
                    ]
                ).to_excel(writer, sheet_name=sheet, index=False)

        def questions(nodes, columns, variant):
            return nodes.assign(
                question=[
                    {"type": "noul", "instructions": f"Does this organization do {r.name}?"}
                    for r in nodes.itertuples()
                ]
            )

        return load_workbook(
            "education",
            path,
            columns=columns,
            sheets=sheets,
            question_builder=questions,
            subject_kind="organization",
        )

    return make


@pytest.mark.parametrize("depth", [1, 4])
def test_direct_entity_and_new_taxonomy_need_no_engine_changes(custom_taxonomy, depth):
    tax = custom_taxonomy(depth)
    subjects = pd.DataFrame(
        [
            {
                "subject_id": "org-1",
                "entity_id": "org-1",
                "state": "We run a school.",
                "kind": "organization",
                "evidence": "We run a school.",
            }
        ]
    )
    probabilities = {r.node_id: 0.8 if r.depth == 0 else 0.65 for r in tax.nodes.itertuples()}
    result = map_subjects(
        subjects, tax, policy=MatchPolicy(descend=0.6), request_runner=fake_jev(probabilities)
    )
    assert result.subject_paths.depth.tolist() == [depth - 1]
    assert len(result.subject_paths.iloc[0].ancestor_matches) == depth
    grouped = group_matches(result.subject_paths, subjects, tax)
    assert grouped.supporting_subject_ids.tolist() == [["org-1"]]
    assert grouped.subject_evidence.iloc[0][0]["evidence"] == "We run a school."
    assert not hasattr(result, "funding") and not hasattr(result, "organizations")


def test_changed_question_rejects_saved_probabilities(custom_taxonomy):
    tax = custom_taxonomy(1)
    subjects = pd.DataFrame([{"subject_id": "a", "state": "school"}])
    result = map_subjects(subjects, tax, policy=MatchPolicy(), request_runner=fake_jev())
    nodes = tax.nodes.copy()
    nodes["question"] = [{"type": "noul", "instructions": "A changed meaning"}]
    with pytest.raises(ValueError, match="different taxonomy questions"):
        select_matches(result, replace(tax, nodes=nodes), MatchPolicy())


def test_old_group_hash_manifests_remain_usable_offline(custom_taxonomy):
    tax = custom_taxonomy(1)
    subjects = pd.DataFrame([{"subject_id": "a", "state": "school"}])
    result = map_subjects(subjects, tax, policy=MatchPolicy(), request_runner=fake_jev())
    result.lineage.pop("questions_sha256")
    result.lineage.pop("subject_kind")
    assert len(select_matches(result, tax, MatchPolicy())) == 1
    nodes = tax.nodes.copy()
    nodes["question"] = [{"type": "noul", "instructions": "Changed meaning"}]
    with pytest.raises(ValueError, match="different taxonomy questions"):
        select_matches(result, replace(tax, nodes=nodes), MatchPolicy())


def test_subject_kind_and_namespace_are_explicit(custom_taxonomy):
    tax = custom_taxonomy(1)
    subjects = pd.DataFrame([{"subject_id": "a", "state": "school", "kind": "activity"}])
    with pytest.raises(ValueError, match="Subject kind"):
        map_subjects(subjects, tax, policy=MatchPolicy(), request_runner=fake_jev())
    subjects["kind"] = "organization"
    keys = []

    def capture(questions, pairs, **kwargs):
        keys.append(pairs[0][0])
        return fake_jev()(questions, pairs, **kwargs)

    for namespace in ("project-a", "project-b"):
        map_subjects(
            subjects, tax, policy=MatchPolicy(), namespace=namespace, request_runner=capture
        )
    assert keys[0] != keys[1]


@pytest.mark.parametrize("name,other", [("one_earth", "drawdown"), ("drawdown", "one_earth")])
def test_selected_loader_imports_no_sibling_question_module(taxonomy, name, other):
    tax = taxonomy(name)
    blocked = (
        ("one_earth", "oneearth_questions")
        if other == "one_earth"
        else ("drawdown", "drawdown_questions")
    )
    code = f"""
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.rsplit('.', 1)[-1] in {blocked!r}:
            raise AssertionError('Sibling imported: ' + fullname)
sys.meta_path.insert(0, Block())
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.taxonomies import load_taxonomy
result = load_taxonomy({name!r}, {tax.path!r}, variant='base')
assert len(result.nodes) == 6
assert not any('activity_extraction' in key for key in sys.modules)
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PYTHONPATH": str(__import__("pathlib").Path(__file__).resolve().parents[2]),
        },
    )
    assert completed.returncode == 0, completed.stderr


def test_extraction_snapshot_round_trip_and_integrity(extraction, tmp_path):
    directory = tmp_path / "extraction"
    extraction.save(directory)
    loaded = type(extraction).load(directory)
    assert loaded.activities.activity_id.tolist() == extraction.activities.activity_id.tolist()
    assert loaded.lineage == extraction.lineage
    with pytest.raises(FileExistsError):
        extraction.save(directory)
    (directory / "activity_extraction_orgs.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum mismatch"):
        type(extraction).load(directory)


def test_generic_cli_scores_and_reselects(custom_taxonomy, tmp_path, monkeypatch):
    from vdl_tools.shared_tools.taxonomy_mapping.activity_jev import __main__ as cli

    tax = custom_taxonomy(4)
    spec = tmp_path / "spec.json"
    spec.write_text(
        json.dumps(
            {
                "name": tax.name,
                "columns": tax.columns,
                "nodes": tax.nodes.to_dict("records"),
                "subject_kind": "organization",
            }
        )
    )
    subjects = tmp_path / "subjects.parquet"
    pd.DataFrame([{"subject_id": "a", "state": "school"}]).to_parquet(subjects)
    original = cli.score_subjects
    monkeypatch.setattr(
        cli, "score_subjects", lambda *a, **kw: original(*a, **kw, request_runner=fake_jev())
    )
    scored = tmp_path / "scored"
    cli.main(
        ["--subjects", str(subjects), "--taxonomy-spec", str(spec), "--output-dir", str(scored)]
    )
    monkeypatch.setattr(
        cli, "score_subjects", lambda *a, **kw: pytest.fail("Offline reselection scored")
    )
    selected = tmp_path / "selected"
    cli.main(
        ["--stored-run", str(scored), "--taxonomy-spec", str(spec), "--output-dir", str(selected)]
    )
    assert json.loads((selected / "selection_manifest.json").read_text())["max_level"] == 3
    assert (selected / "subject_paths.parquet").exists()
