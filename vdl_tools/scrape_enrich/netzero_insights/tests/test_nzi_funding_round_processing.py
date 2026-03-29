import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_funding_round_processing import (
    SPLIT_AFTER_LAST_EARLY_ROUND,
    SPLIT_ON_FIRST_LATE_ROUND,
    divide_funding_rows,
)


def make_company_funding_rows(rounds):
    return pd.DataFrame(rounds)


def test_legacy_split_keeps_debt_before_series_b_in_early_bucket():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-03-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
    ])

    early_rows, later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early_rows["round_type_nzi"].tolist() == ["Series A", "Debt"]
    assert later_rows["round_type_nzi"].tolist() == ["Series B"]


def test_new_split_moves_debt_after_last_early_round_into_later_bucket():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-03-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
    ])

    early_rows, later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early_rows["round_type_nzi"].tolist() == ["Series A"]
    assert later_rows["round_type_nzi"].tolist() == ["Debt", "Series B"]


def test_new_split_allows_later_bucket_without_series_b():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
    ])

    early_rows, later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early_rows["round_type_nzi"].tolist() == ["Series A"]
    assert later_rows["round_type_nzi"].tolist() == ["Debt"]


def test_split_still_caps_later_bucket_before_series_c():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-03-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-04-01"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
    ])

    legacy_early_rows, legacy_later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )
    new_early_rows, new_later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert legacy_early_rows["round_type_nzi"].tolist() == ["Series A", "Debt"]
    assert legacy_later_rows["round_type_nzi"].tolist() == ["Series B"]
    assert new_early_rows["round_type_nzi"].tolist() == ["Series A"]
    assert new_later_rows["round_type_nzi"].tolist() == ["Debt", "Series B"]


def test_new_split_uses_last_early_round_as_boundary():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-03-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-04-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-05-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
    ])

    early_rows, later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early_rows["round_type_nzi"].tolist() == ["Early VC", "Debt", "Series A"]
    assert later_rows["round_type_nzi"].tolist() == ["Debt", "Series B"]


def test_new_split_returns_none_without_series_a_or_early_vc_anchor():
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
    ])

    early_rows, later_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early_rows is None
    assert later_rows is None
