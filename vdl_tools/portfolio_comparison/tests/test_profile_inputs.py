import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.intake.profile_inputs import (
    map_dispositions,
    profile_file,
    propose_column_mapping,
)


def test_map_dispositions_with_engagement_overrides():
    s = pd.Series(["Yes", "No", "In progress", "TOTAL", None])
    overrides = {"in progress": "passed", "total": "exclude", "": "passed"}
    assert map_dispositions(s, overrides).tolist() == [
        "invested", "passed", "passed", "exclude", "passed",
    ]


def test_map_dispositions_rejects_bad_override_targets():
    with pytest.raises(ValueError, match="targets"):
        map_dispositions(pd.Series(["x"]), {"x": "maybe"})


def test_overrides_clear_disposition_blocking():
    df = pd.DataFrame({
        "Name": ["A", "B", "C"],
        "Decision": ["Invested", "Pipeline (Interested)", None],
    })
    blocked = profile_file(df, "f.xlsx", default_entity_type="nonprofit")
    assert blocked["disposition"]["blocking"]
    cleared = profile_file(
        df, "f.xlsx", default_entity_type="nonprofit",
        disposition_overrides={"pipeline (interested)": "passed", "": "passed"},
    )
    assert not cleared["disposition"]["blocking"]
    assert cleared["disposition"]["n_passed"] == 2


def test_token_fallback_maps_compound_headers_but_not_sponsor_columns():
    # Real OSP grants file shape: the project-name header only matches via
    # token fallback, and "Fiscal Sponsor Name" must NOT claim `name`.
    mapping = propose_column_mapping([
        "EIN", "Organization / Project Name", "Website",
        "Fiscal Sponsor Name", "Fiscal Sponsor EIN", "Invested or Passed",
    ])
    assert mapping["Organization / Project Name"] == "name"
    assert mapping["EIN"] == "ein"
    assert mapping["Website"] == "url"
    assert mapping["Invested or Passed"] == "disposition"
    assert mapping["Fiscal Sponsor Name"] == "passthrough"
    assert mapping["Fiscal Sponsor EIN"] == "passthrough"


def test_propose_column_mapping_recognizes_customer_headers():
    mapping = propose_column_mapping(
        ["Company Name", "Website", "Invest Decision", "Check Size", "Fund"]
    )
    assert mapping["Company Name"] == "name"
    assert mapping["Website"] == "url"
    assert mapping["Invest Decision"] == "disposition"
    assert mapping["Check Size"] == "passthrough"
    assert mapping["Fund"] == "passthrough"


def test_profile_flags_partial_disposition_as_blocking():
    df = pd.DataFrame({
        "Name": ["A", "B", "C"],
        "URL": ["a.com", "b.com", "c.com"],
        "Decision": ["Invested", None, "Passed"],
    })
    profile = profile_file(df, "companies.xlsx", default_entity_type="for_profit")
    dispo = profile["disposition"]
    assert dispo["present"]
    assert dispo["n_invested"] == 1
    assert dispo["n_passed"] == 1
    assert dispo["n_blank"] == 1
    assert dispo["blocking"]  # partial coverage is ambiguous — never guessed


def test_profile_detects_duplicates_and_eins():
    df = pd.DataFrame({
        "Organization": ["River Trust", "River Trust", "Ocean Fund"],
        "Website": ["rivertrust.org", "rivertrust.org", "oceanfund.org"],
        "EIN": ["12-3456789", "12-3456789", None],
    })
    profile = profile_file(df, "nonprofits.xlsx", default_entity_type="nonprofit")
    assert profile["n_duplicate_rows"] == 2
    assert profile["n_valid_ein"] == 2
    assert profile["disposition"] == {
        "present": False, "note": "all rows default to invested",
    }


def test_profile_without_name_column_blocks():
    df = pd.DataFrame({"Website": ["a.com"]})
    profile = profile_file(df, "companies.xlsx", default_entity_type="for_profit")
    assert "blocking" in profile
