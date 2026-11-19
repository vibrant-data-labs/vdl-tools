"""The activity layer: subjects from activities, the shared runner (one extraction, several
taxonomy jobs, files with lineage) and assembly by ID."""

import pandas as pd
import pytest

from vdl_tools.shared_tools.parquet_cache import get_lineage, read_dataframe
from vdl_tools.shared_tools.taxonomy_mapping import drawdown, one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.activities import (
    MappingJob,
    assemble_mapping_runs,
    build_activity_subjects,
    run_activity_mapping,
)
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_sha256

from .conftest import fake_jev, profile, website_texts

# Deliberately not the Climate Landscape names: nothing may assume id/Organization/Website.
COLUMNS = {"id_col": "org_key", "name_col": "org_title", "url_col": "homepage"}
PROPERTIES = {"SPEED OF ACTION": "speed", "CLIMATE POLLUTANTS": "pollutants"}


@pytest.fixture
def orgs(organizations):
    return organizations.rename(columns={"id": "org_key", "Organization": "org_title", "Website": "homepage"})


@pytest.fixture
def jobs(taxonomy):
    return {
        "one_earth": MappingJob(taxonomy("one_earth"), one_earth.DEFAULT_POLICY, "oe_cat"),
        "drawdown": MappingJob(
            taxonomy("drawdown"), drawdown.DEFAULT_POLICY, "dd_cat",
            attribute_columns=PROPERTIES, pad_levels=4, funding_level=1,
        ),
    }


@pytest.fixture(autouse=True)
def extraction_calls(use_extraction, use_jev):
    """Offline extraction (counting calls) and Jev answers for every test here."""
    calls = []
    use_extraction(lambda *a, **kw: calls.append(1) or {"1": profile(), "4": profile(0)})
    use_jev(fake_jev())
    return calls


def run(orgs, jobs, directory, **options):
    texts = website_texts(orgs.rename(columns={"org_key": "id", "homepage": "Website"}))
    return run_activity_mapping(orgs, jobs, run_id="run-1", directory=directory, **COLUMNS, website_texts=texts, **options)


def test_one_extraction_maps_every_job_and_writes_each_table(orgs, jobs, tmp_path, extraction_calls):
    result = run(orgs, jobs, tmp_path, file_names={"drawdown_paths": "dd_rows.parquet"})
    assert len(extraction_calls) == 1
    mapped = result.organizations
    assert mapped.org_key.tolist() == orgs.org_key.tolist()
    assert mapped.one_earth_mapping_status.tolist() == ["matched", "missing_text", "request_failed", "empty"]
    assert mapped.drawdown_mapping_status.tolist() == mapped.one_earth_mapping_status.tolist()
    assert mapped.activity_extraction_status.tolist() == ["success", "missing_text", "request_failed", "empty"]
    # No padding unless asked: One Earth has three levels, Drawdown asked for four.
    assert "all_level2_oe_cat" in mapped and "all_level3_oe_cat" not in mapped
    assert mapped.all_level3_dd_cat.tolist() == [[], [], [], []]
    # Attribute lists align with the deepest level's matches.
    assert mapped.speed.iloc[0] == ["Fast"] * len(mapped.all_level2_dd_cat.iloc[0])
    assert mapped.pollutants.iloc[1] == []
    names = {p.name for p in tmp_path.glob("*.parquet")}
    assert names == {
        "organizations.parquet", "one_earth_paths.parquet", "one_earth_funding.parquet", "dd_rows.parquet",
        "drawdown_funding.parquet", *(f"{job}_{t}.parquet" for job in jobs for t in ("scores", "usage", "activity_paths")),
    }
    rows = read_dataframe(tmp_path / "dd_rows.parquet")
    assert {"org_key", "org_title", "SPEED OF ACTION"} <= set(rows.columns)
    funding = read_dataframe(tmp_path / "drawdown_funding.parquet")
    # Funding stops at level 1: one row per organization and level-1 path, splitting 1.0.
    assert len(funding) == len(funding[["org_key", "level0", "level1"]].drop_duplicates())
    assert funding.groupby("org_key").FundingFrac.sum().to_dict() == {"1": 1.0}
    assert result.lineage["taxonomies"]["drawdown"]["max_funding_level"] == 1
    assert result.lineage["taxonomies"]["one_earth"]["max_funding_level"] == 2
    lineage = get_lineage(tmp_path / "organizations.parquet")
    assert lineage["columns"] == {"id": "org_key", "name": "org_title", "url": "homepage"}
    assert lineage["run_id"] == "run-1" and lineage["n_orgs"] == 4
    assert set(lineage["taxonomies"]) == {"one_earth", "drawdown"}
    assert get_lineage(tmp_path / "dd_rows.parquet")["export_columns"][-2:] == ["speed", "pollutants"]


def test_a_failed_job_keeps_finished_files_and_writes_no_organizations_file(orgs, jobs, tmp_path, use_jev):
    drawdown_nodes = set(jobs["drawdown"].taxonomy.nodes.node_id)
    replies = fake_jev()

    def gateway(questions, pairs, **kwargs):
        if drawdown_nodes & set(questions):
            raise RuntimeError("gateway down")
        return replies(questions, pairs, **kwargs)

    use_jev(gateway)
    with pytest.raises(RuntimeError, match="gateway down"):
        run(orgs, jobs, tmp_path)
    assert (tmp_path / "one_earth_paths.parquet").exists()
    assert not (tmp_path / "drawdown_paths.parquet").exists()
    assert not (tmp_path / "organizations.parquet").exists()


def test_assembly_copies_selected_jobs_by_id(orgs, jobs, tmp_path):
    first = run(orgs, {"one_earth": jobs["one_earth"]}, tmp_path / "oe")
    second = run(orgs, {"drawdown": jobs["drawdown"]}, tmp_path / "dd")
    legacy = orgs.iloc[::-1].assign(dd_cat=["Approved legacy", None, None, None])
    combined = assemble_mapping_runs(legacy, {"one_earth": str(tmp_path / "oe" / "organizations.parquet")}, id_col="org_key")
    assert combined.org_key.tolist() == ["4", "3", "2", "1"]
    assert combined.dd_cat.tolist() == legacy.dd_cat.tolist()
    assert combined.one_earth_mapping_status.tolist() == first.organizations.one_earth_mapping_status.tolist()[::-1]
    both = assemble_mapping_runs(
        orgs,
        {"one_earth": str(tmp_path / "oe" / "organizations.parquet"), "drawdown": str(tmp_path / "dd" / "organizations.parquet")},
        id_col="org_key",
    )
    pd.testing.assert_series_equal(both.speed, second.organizations.speed)
    with pytest.raises(ValueError, match="different organization cohort"):
        assemble_mapping_runs(orgs.iloc[:1], {"one_earth": str(tmp_path / "oe" / "organizations.parquet")}, id_col="org_key")


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
