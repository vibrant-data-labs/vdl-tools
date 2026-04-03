import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
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

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A", "Debt"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late is None
    assert exit_rows is None


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

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A"]
    assert middle["round_type_nzi"].tolist() == ["Debt", "Series B"]
    assert late is None
    assert exit_rows is None


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

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A"]
    assert middle["round_type_nzi"].tolist() == ["Debt"]
    assert late is None
    assert exit_rows is None


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

    legacy_early, legacy_middle, legacy_late, legacy_post = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )
    new_early, new_middle, new_late, new_post = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert legacy_early["round_type_nzi"].tolist() == ["Series A", "Debt"]
    assert legacy_middle["round_type_nzi"].tolist() == ["Series B"]
    assert legacy_late["round_type_nzi"].tolist() == ["Series C"]
    assert legacy_post is None

    assert new_early["round_type_nzi"].tolist() == ["Series A"]
    assert new_middle["round_type_nzi"].tolist() == ["Debt", "Series B"]
    assert new_late["round_type_nzi"].tolist() == ["Series C"]
    assert new_post is None


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

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Early VC", "Debt", "Series A"]
    assert middle["round_type_nzi"].tolist() == ["Debt", "Series B"]
    assert late is None
    assert exit_rows is None


def test_seed_is_early_stage():
    """Seed rounds are recognized as early-stage equity ventures."""
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

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Seed", "Debt"]
    assert middle is None
    assert late is None
    assert exit_rows is None


def test_company_starting_at_series_b():
    """Companies with no early rounds should still get middle/late buckets."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2018-10-18"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-06-11"),
            "round_type_nzi": "Late VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-04-15"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-12-24"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-06-30"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early is None
    assert middle["round_type_nzi"].tolist() == ["Series B", "Late VC", "Debt", "Series B"]
    assert late["round_type_nzi"].tolist() == ["Series C"]
    assert exit_rows is None


def test_full_lifecycle():
    """Company going through all stages: early -> middle -> late -> exit."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2015-01-01"),
            "round_type_nzi": "Pre-Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2016-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2017-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2018-01-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-01-01"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "IPO",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Post IPO - Equity",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Pre-Seed", "Seed", "Series A"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == ["Series C"]
    assert exit_rows["round_type_nzi"].tolist() == ["IPO", "Post IPO - Equity"]


def test_debt_only_returns_all_none():
    """Companies with only debt rounds should return all None."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early is None
    assert middle is None
    assert late is None
    assert exit_rows is None


def test_non_venture_absorbed_chronologically():
    """Non-venture round types (Grant, Debt) are absorbed into their chronological bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2019-01-01"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-06-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-06-01"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-01-01"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Grant", "Series A", "Debt"]
    assert middle["round_type_nzi"].tolist() == ["Series B", "Grant"]
    assert late["round_type_nzi"].tolist() == ["Series C"]
    assert exit_rows is None


def test_growth_equity_maps_to_late():
    """Growth equity is classified as late stage."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-01-01"),
            "round_type_nzi": "Growth equity",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Seed"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == ["Growth equity"]
    assert exit_rows is None


def test_grant_only_company_with_equity_flag():
    """Companies with only Grant financing type should be processed."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Grant", "Seed"]
    assert middle is None
    assert late is None
    assert exit_rows is None


# ── Tests based on real company funding patterns ──────────────────────────


def test_early_stage_acquired_company():
    """Based on Company 68: Early VC rounds followed by acquisition.
    Acquisition triggers the exit bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2015-11-16"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2016-02-01"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-01-13"),
            "round_type_nzi": "Acquisition",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Early VC", "Early VC"]
    assert middle is None
    assert late is None
    assert exit_rows["round_type_nzi"].tolist() == ["Acquisition"]


def test_series_a_with_debt_and_buyout():
    """Based on Company 16441: Series A rounds, grants, debt, then buyout and project finance.
    Buyout triggers exit bucket; Project Finance after buyout is absorbed into exit."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2017-07-25"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2018-08-07"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2018-10-26"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-12-23"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-10-08"),
            "round_type_nzi": "Buyout",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2025-03-17"),
            "round_type_nzi": "Project Finance",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == [
        "Series A", "Series A", "Grant", "Debt",
    ]
    assert middle is None
    assert late is None
    assert exit_rows["round_type_nzi"].tolist() == ["Buyout", "Project Finance"]


def test_full_lifecycle_with_spac_and_post_ipo():
    """Based on Company 275: Series A through Series E, SPAC, PIPE, Post IPO.
    Grants and debt absorbed into their chronological buckets."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2007-04-24"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2010-07-12"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2012-01-23"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2012-10-03"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2014-03-26"),
            "round_type_nzi": "Series D",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-08-06"),
            "round_type_nzi": "Series E",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-06-02"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2023-02-09"),
            "round_type_nzi": "SPAC",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2023-02-09"),
            "round_type_nzi": "PIPE",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2024-08-08"),
            "round_type_nzi": "Post IPO",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == [
        "Series C", "Debt", "Series D", "Series E", "Grant",
    ]
    assert exit_rows["round_type_nzi"].tolist() == ["SPAC", "PIPE", "Post IPO"]


def test_early_to_spac_skipping_middle_and_late():
    """Based on Company 65697: Early VC + Accelerator then straight to SPAC.
    No middle or late stages — jumps from early to exit."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-08-18"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-10-05"),
            "round_type_nzi": "Accelerator/incubator",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-02-22"),
            "round_type_nzi": "SPAC",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-02-22"),
            "round_type_nzi": "PIPE",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Early VC", "Accelerator/incubator"]
    assert middle is None
    assert late is None
    assert exit_rows["round_type_nzi"].tolist() == ["SPAC", "PIPE"]


def test_early_skips_to_late_with_series_c():
    """Based on Company 50: Seed/Series A/Early VC then jumps to Series C and SPAC.
    No Series B means no middle bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2018-02-14"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-05-21"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-01"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-09-16"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-02-01"),
            "round_type_nzi": "PIPE",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-02-01"),
            "round_type_nzi": "SPAC",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Seed", "Series A", "Early VC"]
    assert middle is None
    assert late["round_type_nzi"].tolist() == ["Series C", "PIPE"]
    assert exit_rows["round_type_nzi"].tolist() == ["SPAC"]


def test_ipo_only_company_with_post_ipo_and_acquisition():
    """Based on Company 7: IPO -> Post IPO rounds -> Acquisition -> Grants.
    No early/middle/late since there are no venture rounds."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2016-10-26"),
            "round_type_nzi": "IPO",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2017-10-23"),
            "round_type_nzi": "Post IPO",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-12-21"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-07-07"),
            "round_type_nzi": "Acquisition",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early is None
    assert middle is None
    assert late is None
    assert exit_rows["round_type_nzi"].tolist() == [
        "IPO", "Post IPO", "Grant", "Acquisition",
    ]


def test_full_lifecycle_seed_to_ipo_with_merger():
    """Based on Company 186: Seed through Series C, Late VC, IPO, then Merger.
    Merger is non-boundary, absorbed into the exit bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2015-01-01"),
            "round_type_nzi": "Accelerator/incubator",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2015-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2017-07-19"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2018-11-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-02-25"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-07-06"),
            "round_type_nzi": "Late VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-11-24"),
            "round_type_nzi": "IPO",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2023-01-12"),
            "round_type_nzi": "Merger",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == [
        "Accelerator/incubator", "Seed", "Series A",
    ]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == ["Series C", "Late VC"]
    assert exit_rows["round_type_nzi"].tolist() == ["IPO", "Merger"]


def test_series_a_to_late_vc_with_acquisition():
    """Based on Company 16398: Series A, Early VC rounds, then Late VC, then Acquisition.
    Late VC triggers middle; Acquisition triggers exit."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2014-07-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2015-05-18"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2016-05-27"),
            "round_type_nzi": "Early VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2016-09-28"),
            "round_type_nzi": "Late VC",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-05-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-11-30"),
            "round_type_nzi": "Acquisition",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A", "Early VC", "Early VC"]
    assert middle["round_type_nzi"].tolist() == ["Late VC", "Debt"]
    assert late is None
    assert exit_rows["round_type_nzi"].tolist() == ["Acquisition"]


def test_series_a_through_spac_with_post_ipo_and_buyout():
    """Based on Company 205: Series A, late rounds, SPAC, Post IPO, then Buyout.
    Buyout and Grant absorbed into exit bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2013-08-09"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2015-07-22"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2017-09-13"),
            "round_type_nzi": "Series D",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-04-22"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-08-10"),
            "round_type_nzi": "SPAC",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-12-01"),
            "round_type_nzi": "Post IPO",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2023-07-24"),
            "round_type_nzi": "Buyout",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2024-03-15"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A"]
    assert middle is None
    assert late["round_type_nzi"].tolist() == ["Series C", "Series D", "Debt"]
    assert exit_rows["round_type_nzi"].tolist() == [
        "SPAC", "Post IPO", "Buyout", "Grant",
    ]


def test_grants_heavy_company_through_full_lifecycle():
    """Based on Company 218: Many grants early, then Series A-E, SPAC, more grants.
    Grants before Series A stay in early; grants after SPAC go to exit."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2009-01-01"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2013-01-01"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2015-03-18"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2015-07-23"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2016-06-08"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2017-12-14"),
            "round_type_nzi": "Series D",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-09-19"),
            "round_type_nzi": "Series E",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-09-30"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-09-17"),
            "round_type_nzi": "SPAC",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-08-16"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Grant", "Grant", "Series A"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == [
        "Series C", "Series D", "Series E", "Grant",
    ]
    assert exit_rows["round_type_nzi"].tolist() == ["SPAC", "Grant"]


def test_middle_stage_buyout_after_series_b_and_c():
    """Based on Company 16444: Series A, Series B, multiple Series C rounds, then Buyout.
    Buyout triggers exit bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2020-02-25"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-04-28"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-11-17"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-01-13"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-10-18"),
            "round_type_nzi": "Series C",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2026-02-09"),
            "round_type_nzi": "Buyout",
            "financing_type_nzi": "Other",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Series A", "Debt"]
    assert middle["round_type_nzi"].tolist() == ["Series B"]
    assert late["round_type_nzi"].tolist() == ["Series C", "Series C"]
    assert exit_rows["round_type_nzi"].tolist() == ["Buyout"]
