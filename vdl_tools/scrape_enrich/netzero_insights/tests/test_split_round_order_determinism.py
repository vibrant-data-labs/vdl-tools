"""`divide_funding_rows` must not depend on the order rounds arrive in.

Found by comparing v1-cache and v2-live records for the same companies: a
company with a SPAC and a PIPE on the same date had $375M land in
`late_to_exit` from one source and in `exit` from the other, because the
split sorted by date only and the two sources delivered the tied rounds in
opposite order. 23 of 40 sampled companies have same-date rounds.
"""

import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    divide_funding_rows,
)


def _rows(rounds):
    df = pd.DataFrame(rounds)
    df["round_date_nzi"] = pd.to_datetime(df["round_date_nzi"])
    return df


def _bucket_ids(split):
    return {k: (sorted(v["co_funding_round_id_nzi"]) if v is not None and len(v) else None) for k, v in split.items()}


SAME_DAY_EXIT = [
    {"co_funding_round_id_nzi": 134675, "round_date_nzi": "2018-02-14", "round_type_nzi": "Seed", "financing_type_nzi": "Equity", "round_amount_usd_nzi": None},
    {"co_funding_round_id_nzi": 134692, "round_date_nzi": "2019-05-21", "round_type_nzi": "Series A", "financing_type_nzi": "Equity", "round_amount_usd_nzi": 82e6},
    {"co_funding_round_id_nzi": 134695, "round_date_nzi": "2020-09-16", "round_type_nzi": "Series C", "financing_type_nzi": "Equity", "round_amount_usd_nzi": 28e6},
    # Same date, two different rounds: an exit-type round and a non-boundary one.
    {"co_funding_round_id_nzi": 137526, "round_date_nzi": "2021-02-01", "round_type_nzi": "PIPE", "financing_type_nzi": "Equity", "round_amount_usd_nzi": 375e6},
    {"co_funding_round_id_nzi": 134696, "round_date_nzi": "2021-02-01", "round_type_nzi": "SPAC", "financing_type_nzi": "Other", "round_amount_usd_nzi": 100e6},
]


def test_same_date_rounds_split_identically_regardless_of_input_order():
    forward = divide_funding_rows(_rows(SAME_DAY_EXIT))
    reversed_ = divide_funding_rows(_rows(list(reversed(SAME_DAY_EXIT))))
    assert _bucket_ids(forward) == _bucket_ids(reversed_)


def test_tie_break_is_by_round_id_so_it_is_stable_across_sources():
    split = divide_funding_rows(_rows(SAME_DAY_EXIT))
    # The lower ID (SPAC, 134696) is ordered first on the tied date, so the
    # exit bucket opens with it and the PIPE that follows lands in `exit` too.
    assert _bucket_ids(split)["exit"] == [134696, 137526]


def test_rounds_without_an_id_column_still_split():
    # Unit tests elsewhere build frames without co_funding_round_id_nzi.
    rows = _rows([{k: v for k, v in r.items() if k != "co_funding_round_id_nzi"} for r in SAME_DAY_EXIT])
    split = divide_funding_rows(rows)
    assert split["exit"] is not None and len(split["exit"]) >= 1
