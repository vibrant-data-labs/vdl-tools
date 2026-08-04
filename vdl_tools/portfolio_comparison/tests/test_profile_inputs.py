import pandas as pd

from vdl_tools.portfolio_comparison.intake.profile_inputs import (
    profile_file,
    propose_column_mapping,
)


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
