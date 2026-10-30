"""Generality and loader isolation without inference or database access."""

import os
import subprocess
import sys
from dataclasses import replace

import pandas as pd
import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import (
    MatchPolicy,
    map_subjects,
    select_matches,
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

        def questions(nodes, columns):
            return nodes.assign(
                question=[
                    {"type": "noul", "instructions": f"Does this organization do {r.name}?"}
                    for r in nodes.itertuples()
                ]
            )

        return load_workbook(
            "education", path, columns=columns, sheets=sheets, question_builder=questions
        )

    return make


@pytest.mark.parametrize("depth", [1, 4])
def test_direct_entity_and_new_taxonomy_need_no_engine_changes(custom_taxonomy, depth):
    tax = custom_taxonomy(depth)
    subjects = pd.DataFrame([{"subject_id": "org-1", "state": "We run a school."}])
    probabilities = {r.node_id: 0.8 if r.depth == 0 else 0.65 for r in tax.nodes.itertuples()}
    result = map_subjects(
        subjects, tax, policy=MatchPolicy(descend=0.6), request_runner=fake_jev(probabilities)
    )
    assert result.subject_paths.depth.tolist() == [depth - 1]
    assert len(result.subject_paths.iloc[0].ancestor_matches) == depth
    assert result.lineage["max_mapping_level"] == depth - 1


def test_changed_question_rejects_saved_probabilities(custom_taxonomy):
    tax = custom_taxonomy(1)
    subjects = pd.DataFrame([{"subject_id": "a", "state": "school"}])
    result = map_subjects(subjects, tax, policy=MatchPolicy(), request_runner=fake_jev())
    nodes = tax.nodes.copy()
    nodes["question"] = [{"type": "noul", "instructions": "A changed meaning"}]
    with pytest.raises(ValueError, match="different taxonomy or question set"):
        select_matches(result, replace(tax, nodes=nodes), MatchPolicy())


@pytest.mark.parametrize("name,other", [("one_earth", "drawdown"), ("drawdown", "one_earth")])
def test_selected_loader_imports_no_sibling_package(taxonomy, name, other):
    tax = taxonomy(name)
    code = f"""
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.startswith('vdl_tools.shared_tools.taxonomy_mapping.{other}'):
            raise AssertionError('Sibling imported: ' + fullname)
sys.meta_path.insert(0, Block())
from vdl_tools.shared_tools.taxonomy_mapping.{name} import load_taxonomy
result = load_taxonomy({tax.path!r})
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


def test_extraction_snapshot_round_trips_through_parquet_cache(extraction, tmp_path):
    directory = tmp_path / "extraction"
    extraction.save(directory)
    loaded = type(extraction).load(directory)
    assert loaded.activities.activity_id.tolist() == extraction.activities.activity_id.tolist()
    assert (
        loaded.organizations.extraction_status.tolist()
        == extraction.organizations.extraction_status.tolist()
    )
    assert {k: loaded.lineage[k] for k in extraction.lineage} == extraction.lineage
    assert "created_at" in loaded.lineage  # written by parquet_cache into the footer
