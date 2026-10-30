"""The shared runner: one extraction, several taxonomy jobs, files with lineage, assembly by ID."""

import pandas as pd
import pytest

from vdl_tools.shared_tools.parquet_cache import get_lineage, read_dataframe
from vdl_tools.shared_tools.taxonomy_mapping import drawdown, one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.runs import (
    MappingJob,
    assemble_mapping_runs,
    run_activity_mapping,
)

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


def run(orgs, jobs, directory, calls=None, **options):
    texts = website_texts(orgs.rename(columns={"org_key": "id", "homepage": "Website"}))

    def extract(*args, **kwargs):
        if calls is not None:
            calls.append(1)
        return {"1": profile(), "4": profile(0)}

    return run_activity_mapping(
        orgs, jobs, run_id="run-1", directory=directory, **COLUMNS, website_texts=texts,
        extraction_runner=extract, request_runner=options.pop("request_runner", fake_jev()), **options,
    )


def test_one_extraction_maps_every_job_and_writes_each_table(orgs, jobs, tmp_path):
    calls = []
    result = run(orgs, jobs, tmp_path, calls, file_names={"drawdown_paths": "dd_rows.parquet"})
    assert len(calls) == 1
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


def test_a_failed_job_keeps_finished_files_and_writes_no_organizations_file(orgs, jobs, tmp_path):
    drawdown_nodes = set(jobs["drawdown"].taxonomy.nodes.node_id)
    replies = fake_jev()

    def gateway(questions, pairs, **kwargs):
        if drawdown_nodes & set(questions):
            raise RuntimeError("gateway down")
        return replies(questions, pairs, **kwargs)

    with pytest.raises(RuntimeError, match="gateway down"):
        run(orgs, jobs, tmp_path, request_runner=gateway)
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
