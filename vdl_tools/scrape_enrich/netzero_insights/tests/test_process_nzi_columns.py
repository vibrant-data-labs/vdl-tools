"""Lenient-but-loud column selection shared by the process_nzi processors."""

import logging

import pandas as pd
import pytest

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.columns import (
    missing_columns,
    select_and_rename,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi import company, funding_round, investor
from vdl_tools.shared_tools.tools.logger import logger as vdl_logger


@pytest.fixture(autouse=True)
def _propagate_vdl_logger():
    # The vdl_tools logger sets propagate=False, so pytest's caplog (which
    # listens on the root logger) never sees it unless we open the gate.
    previous = vdl_logger.propagate
    vdl_logger.propagate = True
    yield
    vdl_logger.propagate = previous


def test_present_columns_are_renamed_and_suffixed_extras_kept():
    df = pd.DataFrame({"clientID": [1], "lastRoundType": ["Seed"], "trl_parsed_nzi": ["9"], "dropme": [0]})

    out = select_and_rename(df, ["clientID", "lastRoundType"], "_nzi", entity="company")

    assert list(out.columns) == ["client_id_nzi", "last_round_type_nzi", "trl_parsed_nzi"]
    assert out.loc[0, "last_round_type_nzi"] == "Seed"


def test_missing_columns_become_na_and_are_logged(caplog):
    # v2 records lack e.g. eutopiaScore; the old `df[cols]` raised KeyError here.
    df = pd.DataFrame({"clientID": [1, 2]})

    with caplog.at_level(logging.WARNING):
        out = select_and_rename(df, ["clientID", "eutopiaScore", "directURL"], "_nzi", entity="company")

    assert list(out.columns) == ["client_id_nzi", "eutopia_score_nzi", "direct_url_nzi"]
    assert out["eutopia_score_nzi"].isna().all()
    assert len(out) == 2
    assert any("company records lack 2 expected column(s)" in r.message and "eutopiaScore" in r.message
               for r in caplog.records)


def test_no_warning_when_nothing_is_missing(caplog):
    df = pd.DataFrame({"a": [1]})
    with caplog.at_level(logging.WARNING):
        select_and_rename(df, ["a"], "_nzi", entity="x")
    assert not [r for r in caplog.records if "lack" in r.message]


def test_missing_columns_helper():
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert missing_columns(df, ["b", "c", "a", "d"]) == ["c", "d"]


def test_listed_column_that_already_carries_the_suffix_is_not_duplicated():
    # A column that is both listed and already suffixed must be kept once.
    df = pd.DataFrame({"x_nzi": [1]})
    out = select_and_rename(df, ["x_nzi"], "_nzi", entity="x")
    assert len(out.columns) == 1


@pytest.mark.parametrize("module, columns, entity", [
    (company, company.ORIGINAL_COMPANY_DETAILS_COLUMNS, "company"),
    (funding_round, funding_round.FUNDING_ROUND_COLUMNS, "funding-round"),
    (investor, investor.ORIGINAL_INVESTOR_DETAILS_COLUMNS, "investor"),
])
def test_each_processor_tolerates_a_record_missing_listed_fields(module, columns, entity, caplog):
    # A record carrying only the first listed field must not crash the selector.
    df = pd.DataFrame({columns[0]: [1]})
    with caplog.at_level(logging.WARNING):
        out = module.filter_format_columns(df, keep_suffix="_nzi")
    assert len(out.columns) == len(columns)
    assert any(f"{entity} records lack" in r.message for r in caplog.records)
