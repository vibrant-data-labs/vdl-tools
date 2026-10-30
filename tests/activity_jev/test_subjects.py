"""JEV input preparation is downstream of independent activity extraction."""

import pandas as pd
import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.subjects import build_activity_subjects
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_sha256


def test_subjects_preserve_existing_jev_states_and_cache_keys(extraction):
    subjects = build_activity_subjects(extraction.activities)
    expected = (
        "Activity: solar work 0\nWhat happens: Installs rooftop solar panels."
        "\nWho carries it out: Own crews\nAim: Clean energy"
        "\nSector: electricity\nMethods: rooftop solar"
    )
    assert subjects.state.iloc[0] == expected
    assert subjects.cache_id.iloc[0] == create_deterministic_sha256(["1", expected])
    assert subjects.subject_id.tolist() == extraction.activities.activity_id.tolist()
    assert "state" not in extraction.activities and "cache_id" not in extraction.activities


def test_run_labels_and_metadata_do_not_change_jev_cache_identity(extraction):
    original = build_activity_subjects(extraction.activities)
    changed = extraction.activities.assign(activity_id=["new-1", "new-2"], extraction_run_id="new", prominence="minor")
    rebuilt = build_activity_subjects(changed)
    assert rebuilt.cache_id.tolist() == original.cache_id.tolist()
    pd.testing.assert_series_equal(rebuilt.state, original.state)
    changed.loc[0, "what_happens"] = "A different activity"
    assert build_activity_subjects(changed).cache_id.iloc[0] != original.cache_id.iloc[0]


def test_empty_activities_produce_an_empty_subject_frame(extraction):
    subjects = build_activity_subjects(extraction.activities.iloc[:0])
    assert subjects.empty
    assert subjects.columns.tolist() == ["subject_id", "state", "cache_id"]


def test_jev_requires_its_own_input_fields(extraction):
    with pytest.raises(ValueError, match="what_happens"):
        build_activity_subjects(extraction.activities.drop(columns="what_happens"))
