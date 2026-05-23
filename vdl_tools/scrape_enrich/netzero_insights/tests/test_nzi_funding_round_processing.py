import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    divide_funding_rows,
    divided_funding_rows_and_flatten,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.stage_constants import (
    SPLIT_AFTER_LAST_EARLY_ROUND,
    SPLIT_ON_FIRST_LATE_ROUND,
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
    """Companies with no early rounds should still get b_to_late/late_to_exit buckets."""
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
    No Series B means no b_to_late bucket."""
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


def test_no_boundary_rounds_defaults_to_early():
    """Based on Company 15: Equity crowdfunding, Grants, Accelerators, Awards only.
    No standard venture round types, but has equity financing.
    All rounds should default to the early bucket."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2012-12-01"),
            "round_type_nzi": "Accelerator/Incubator",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2018-05-20"),
            "round_type_nzi": "Product crowdfunding",
            "financing_type_nzi": "Other",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-05-11"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-05-06"),
            "round_type_nzi": "Equity crowdfunding",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-12-01"),
            "round_type_nzi": "Award/Prize",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2024-07-03"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert early["round_type_nzi"].tolist() == [
        "Accelerator/Incubator", "Product crowdfunding", "Grant",
        "Equity crowdfunding", "Award/Prize", "Grant",
    ]
    assert middle is None
    assert late is None
    assert exit_rows is None


def test_no_boundary_rounds_defaults_to_early_with_split_after_last():
    """Same as above but with SPLIT_AFTER_LAST_EARLY_ROUND strategy."""
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2021-05-11"),
            "round_type_nzi": "Grant",
            "financing_type_nzi": "Grant",
        },
        {
            "round_date_nzi": pd.Timestamp("2022-05-06"),
            "round_type_nzi": "Equity crowdfunding",
            "financing_type_nzi": "Equity",
        },
    ])

    early, middle, late, exit_rows = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_AFTER_LAST_EARLY_ROUND,
    )

    assert early["round_type_nzi"].tolist() == ["Grant", "Equity crowdfunding"]
    assert middle is None
    assert late is None
    assert exit_rows is None


# ── Flatten output: equity / non-equity split per stage ──────────────────


def _row(client_id, date, round_type, financing_type, amount):
    return {
        "client_id_nzi": client_id,
        "round_date_nzi": pd.Timestamp(date),
        "round_type_nzi": round_type,
        "financing_type_nzi": financing_type,
        "round_amount_usd_nzi": amount,
    }


def test_flatten_equity_nonequity_split_per_stage():
    # Company A: pre-B journey with grants/debt mixed in across stages.
    # Company B: starts at Series A directly (no up_to_a; tests None vs 0.0).
    rounds = pd.DataFrame([
        # ── Company A ──
        _row("A", "2018-01-01", "Pre-Seed", "Equity", 1_000_000),  # up_to_a equity
        _row("A", "2018-06-01", "Grant",    "Grant",    250_000),  # up_to_a non-equity
        _row("A", "2019-01-01", "Series A", "Equity", 5_000_000),  # a_to_b equity
        _row("A", "2019-06-01", "Series A", "Equity", 2_000_000),  # a_to_b equity (secondary A)
        _row("A", "2019-09-01", "Debt",     "Debt",     500_000),  # a_to_b non-equity
        _row("A", "2020-01-01", "Series B", "Equity", 10_000_000), # middle equity (no non-equity)
        # ── Company B ──
        _row("B", "2021-01-01", "Series A", "Equity", 3_000_000),  # a_to_b equity only
    ])

    out = divided_funding_rows_and_flatten(rounds, id_col="client_id_nzi")
    out_a = out[out["client_id_nzi"] == "A"].iloc[0]
    out_b = out[out["client_id_nzi"] == "B"].iloc[0]

    # Company A: up_to_a — Pre-Seed (equity) + Grant (non-equity)
    assert out_a["up_to_a_equity_raised"] == 1_000_000
    assert out_a["up_to_a_nonequity_raised"] == 250_000
    assert out_a["up_to_a_n_rounds_nonequity"] == 1
    assert out_a["up_to_a_nonequity_types"] == ["Grant"]
    assert (
        out_a["up_to_a_equity_raised"] + out_a["up_to_a_nonequity_raised"]
        == out_a["up_to_a_amount_raised"]
    )

    # Company A: a_to_b — two Series A (equity, summed) + Debt (non-equity)
    assert out_a["a_to_b_equity_raised"] == 7_000_000
    assert out_a["a_to_b_nonequity_raised"] == 500_000
    assert out_a["a_to_b_n_rounds_nonequity"] == 1
    assert out_a["a_to_b_nonequity_types"] == ["Debt"]
    assert (
        out_a["a_to_b_equity_raised"] + out_a["a_to_b_nonequity_raised"]
        == out_a["a_to_b_amount_raised"]
    )

    # Company A: b_to_late — only Series B equity, no non-equity rounds.
    assert out_a["b_to_late_equity_raised"] == 10_000_000
    assert out_a["b_to_late_nonequity_raised"] == 0.0
    assert out_a["b_to_late_n_rounds_nonequity"] == 0
    assert out_a["b_to_late_nonequity_types"] == []

    # Company B: no up_to_a bucket. Numeric columns become NaN through the
    # DataFrame conversion (matches existing pattern for `amount_raised`);
    # the list column stays as None.
    assert pd.isna(out_b["up_to_a_equity_raised"])
    assert pd.isna(out_b["up_to_a_nonequity_raised"])
    assert pd.isna(out_b["up_to_a_n_rounds_nonequity"])
    assert pd.isna(out_b["up_to_a_amount_raised"])  # sanity: same convention
    assert out_b["up_to_a_nonequity_types"] is None

    # Company B: a_to_b has only the Series A equity round.
    assert out_b["a_to_b_equity_raised"] == 3_000_000
    assert out_b["a_to_b_nonequity_raised"] == 0.0
    assert out_b["a_to_b_n_rounds_nonequity"] == 0
    assert out_b["a_to_b_nonequity_types"] == []

    # late_to_exit / exit buckets do NOT get the equity-split columns.
    for absent in ("late_to_exit_equity_raised", "exit_equity_raised",
                   "late_to_exit_nonequity_raised", "exit_nonequity_raised",
                   "late_to_exit_nonequity_types", "exit_nonequity_types"):
        assert absent not in out.columns


def test_flatten_per_stage_investor_lists():
    # One company with two rounds in `up_to_a` (investors overlap across
    # them — dedupe should kick in), one round in `a_to_b`, none in `middle`.
    rounds = pd.DataFrame([
        {
            "client_id_nzi": "A",
            "round_date_nzi": pd.Timestamp("2018-01-01"),
            "round_type_nzi": "Pre-Seed",
            "financing_type_nzi": "Equity",
            "round_amount_usd_nzi": 500_000,
            "round_investor_ids_nzi": [1, 2],
        },
        {
            "client_id_nzi": "A",
            "round_date_nzi": pd.Timestamp("2018-06-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
            "round_amount_usd_nzi": 1_000_000,
            "round_investor_ids_nzi": [2, 3],  # 2 already seen
        },
        {
            "client_id_nzi": "A",
            "round_date_nzi": pd.Timestamp("2019-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
            "round_amount_usd_nzi": 4_000_000,
            "round_investor_ids_nzi": [4, 99],  # 99 missing from investor df
        },
    ])
    investors = pd.DataFrame([
        # Acme VC: primary VC, also a Corporate VC (secondary) — both types should appear.
        {"investor_id_nzi": 1, "name_nzi": "Acme VC",       "primary_type_nzi": "Venture Capital", "secondary_types_nzi": ["Corporate Venture Capital"]},
        # Green Fund: only primary type (secondary missing).
        {"investor_id_nzi": 2, "name_nzi": "Green Fund",    "primary_type_nzi": "Foundation",      "secondary_types_nzi": None},
        # Gov: secondary repeats the primary — should dedupe.
        {"investor_id_nzi": 3, "name_nzi": "Gov Grant Co.", "primary_type_nzi": "Government",      "secondary_types_nzi": ["Government", "Public Agency"]},
        {"investor_id_nzi": 4, "name_nzi": "Mega Capital",  "primary_type_nzi": "Venture Capital", "secondary_types_nzi": []},
    ])

    out = divided_funding_rows_and_flatten(
        rounds, id_col="client_id_nzi", processed_investor_df=investors,
    )
    row = out[out["client_id_nzi"] == "A"].iloc[0]

    # up_to_a: investors 1, 2, 3 (2 deduped), first-seen order preserved.
    # investor_types merges primary + secondary, deduped, primary first.
    assert row["up_to_a_investors"] == [
        {"id": 1, "name": "Acme VC",       "investor_types": ["Venture Capital", "Corporate Venture Capital"]},
        {"id": 2, "name": "Green Fund",    "investor_types": ["Foundation"]},
        {"id": 3, "name": "Gov Grant Co.", "investor_types": ["Government", "Public Agency"]},
    ]

    # a_to_b: investor 4 plus the unknown id 99 (name/types empty, not dropped).
    assert row["a_to_b_investors"] == [
        {"id": 4,  "name": "Mega Capital", "investor_types": ["Venture Capital"]},
        {"id": 99, "name": None,           "investor_types": []},
    ]

    # No b_to_late bucket → list column stays None.
    assert row["b_to_late_investors"] is None

    # Without the investor df, the column should be absent entirely.
    out_no_inv = divided_funding_rows_and_flatten(rounds, id_col="client_id_nzi")
    assert "up_to_a_investors" not in out_no_inv.columns


# ── Leading non-equity drop / happy-path bucket boundaries ──────────────


def test_leading_debt_dropped_when_no_pre_seed_and_no_series_a():
    # [Debt, Late VC] — no Pre-Seed/Seed (so no up_to_a) and no Series A
    # (so no a_to_b). The leading Debt row is intentionally dropped rather
    # than absorbed into middle/late. See split_early_late_funding_rounds.py
    # comment around line 599.
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2019-01-01"),
            "round_type_nzi": "Debt",
            "financing_type_nzi": "Debt",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Late VC",
            "financing_type_nzi": "Equity",
        },
    ])

    buckets = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    # Leading Debt is dropped — middle starts at Late VC.
    assert buckets["up_to_a"] is None
    assert buckets["a_to_b"] is None
    assert buckets["b_to_late"]["round_type_nzi"].tolist() == ["Late VC"]
    assert buckets["late_to_exit"] is None
    assert buckets["exit"] is None


def test_leading_debt_dropped_with_interleaved_debt_and_ipo():
    # [Debt × 4, Late VC, Debt × 3, IPO] — the 4 leading Debt rounds have
    # no equity boundary before them, so they're dropped. The 3 Debt rounds
    # between Late VC and IPO get absorbed chronologically into the middle
    # bucket (which extends through exit_start - 1).
    company_funding_rows = make_company_funding_rows([
        {"round_date_nzi": pd.Timestamp("2015-01-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2015-06-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2016-01-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2016-06-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2017-01-01"), "round_type_nzi": "Late VC", "financing_type_nzi": "Equity"},
        {"round_date_nzi": pd.Timestamp("2017-06-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2018-06-01"), "round_type_nzi": "Debt",    "financing_type_nzi": "Debt"},
        {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "IPO",     "financing_type_nzi": "Equity"},
    ])

    buckets = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert buckets["up_to_a"] is None
    assert buckets["a_to_b"] is None
    # Middle = Late VC + the 3 trailing Debts that fall between Late VC and IPO.
    assert buckets["b_to_late"]["round_type_nzi"].tolist() == [
        "Late VC", "Debt", "Debt", "Debt",
    ]
    assert buckets["late_to_exit"] is None
    assert buckets["exit"]["round_type_nzi"].tolist() == ["IPO"]


def test_happy_path_bucket_boundaries_pre_seed_through_series_b():
    # Canonical journey: one round per stage from Pre-Seed through Series B.
    # Locks down the up_to_a / a_to_b / middle boundaries.
    company_funding_rows = make_company_funding_rows([
        {
            "round_date_nzi": pd.Timestamp("2018-01-01"),
            "round_type_nzi": "Pre-Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2019-01-01"),
            "round_type_nzi": "Seed",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2020-01-01"),
            "round_type_nzi": "Series A",
            "financing_type_nzi": "Equity",
        },
        {
            "round_date_nzi": pd.Timestamp("2021-01-01"),
            "round_type_nzi": "Series B",
            "financing_type_nzi": "Equity",
        },
    ])

    buckets = divide_funding_rows(
        company_funding_rows,
        split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
    )

    assert buckets["up_to_a"]["round_type_nzi"].tolist() == ["Pre-Seed", "Seed"]
    assert buckets["a_to_b"]["round_type_nzi"].tolist() == ["Series A"]
    assert buckets["b_to_late"]["round_type_nzi"].tolist() == ["Series B"]
    assert buckets["late_to_exit"] is None
    assert buckets["exit"] is None
