"""Tests for climate_landscape.venture_backed_flag.

Builds a tiny raw funding-rounds parquet and an org frame that together cover
every branch of the definition: a venture round, grant-only for-profit,
grant-only non-profit, debt-only, post-IPO-only, a pre-2010 venture round,
and a Candid org with no rounds at all.
"""

import pandas as pd
import pytest

from vdl_tools.shared_tools import parquet_cache as pqc
from vdl_tools.shared_tools.climate_landscape import venture_backed_flag as vbf


def _round(uuid, investment_type, announced_on="2018-05-01"):
    """One raw-API-shaped round row: the org uuid sits inside an identifier dict."""
    return {
        "investment_type": investment_type,
        "announced_on": announced_on,
        "funded_organization_identifier": {"uuid": uuid, "permalink": uuid},
    }


@pytest.fixture()
def rounds_uri(tmp_path):
    rounds = pd.DataFrame([
        _round("series-a-co", "series_a"),
        _round("series-a-co", "debt_financing"),
        _round("sbir-startup", "grant"),
        _round("grant-nonprofit", "grant"),
        _round("grant-nodata", "grant"),
        _round("debt-only-co", "debt_financing"),
        _round("public-co", "post_ipo_equity"),
        _round("old-venture-co", "seed", announced_on="2007-03-15"),
    ])
    uri = str(tmp_path / "funding_rounds.parquet")
    pqc.write_dataframe(rounds, uri)
    return uri


@pytest.fixture()
def orgs():
    return pd.DataFrame({
        "id": ["series-a-co", "sbir-startup", "grant-nonprofit", "grant-nodata",
               "debt-only-co", "public-co", "old-venture-co", "candid-org"],
        "Data Source": ["Crunchbase"] * 7 + ["Candid"],
        # Crunchbase's own field: has a 'No Data' gap
        "Org Type": ["For Profit", "For Profit", "Non Profit", "No Data",
                     "For Profit", "For Profit", "For Profit", "Non Profit"],
        # The pipeline's classifier: no gaps
        "OrgType Prediction": ["For Profit", "For Profit", "Non Profit", "For Profit",
                               "For Profit", "For Profit", "For Profit", "Non Profit"],
    })


def test_venture_backed_org_uuids(rounds_uri):
    rounds = pqc.read_dataframe(rounds_uri)
    venture, grant = vbf.venture_backed_org_uuids(rounds)
    assert venture == {"series-a-co", "old-venture-co"}
    assert grant == {"sbir-startup", "grant-nonprofit", "grant-nodata"}


def test_add_venture_backed_flag(rounds_uri, orgs):
    out = vbf.add_venture_backed_flag(orgs, rounds_uri).set_index("id")

    assert out["venture_backed"].dtype == bool
    expected = {
        "series-a-co": (True, "venture round"),
        "sbir-startup": (True, "grant-only + for-profit"),   # grant-only for-profit counts
        "grant-nonprofit": (False, ""),                  # grant-only non-profit does not
        "grant-nodata": (True, "grant-only + for-profit"),    # prediction fills the CB gap
        "debt-only-co": (False, ""),
        "public-co": (False, ""),                        # post-IPO alone is not venture
        "old-venture-co": (True, "venture round"),       # pre-2010 round still counts
        "candid-org": (False, ""),
    }
    for org, (flag, reason) in expected.items():
        assert out.at[org, "venture_backed"] == flag, org
        assert out.at[org, "venture_backed_reason"] == reason, org


def test_org_type_col_switch(rounds_uri, orgs):
    """With Crunchbase's 'Org Type', the 'No Data' grant-only org is no longer flagged."""
    out = vbf.add_venture_backed_flag(orgs, rounds_uri, org_type_col="Org Type").set_index("id")
    assert out.at["grant-nodata", "venture_backed"] == False  # noqa: E712
    assert out.at["sbir-startup", "venture_backed"] == True  # noqa: E712


def test_requires_org_type_column(rounds_uri, orgs):
    with pytest.raises(AssertionError):
        vbf.add_venture_backed_flag(orgs.drop(columns=["OrgType Prediction"]), rounds_uri)


@pytest.mark.parametrize("labels, for_profit_label", [
    (["For Profit", "Non Profit"], "For Profit"),     # display names (climate-landscape)
    (["for_profit", "non_profit"], "for_profit"),     # raw Crunchbase company_type
    (["for_profit", "non_profit"], "For Profit"),     # raw column, default display label
    (["For-Profit", "Non-Profit"], "for_profit"),     # hyphenated spelling
])
def test_for_profit_label_matches_either_vocabulary(rounds_uri, labels, for_profit_label):
    """Raw slugs and display names give the same flag, whichever label the caller passes."""
    fp, np_ = labels
    orgs = pd.DataFrame({"id": ["sbir-startup", "grant-nonprofit"], "Org Type": [fp, np_]})
    out = vbf.add_venture_backed_flag(orgs, rounds_uri, org_type_col="Org Type",
                                      for_profit_label=for_profit_label).set_index("id")
    assert out.at["sbir-startup", "venture_backed"] == True  # noqa: E712
    assert out.at["grant-nonprofit", "venture_backed"] == False  # noqa: E712


def test_unmatched_for_profit_label_raises(rounds_uri, orgs):
    """A label that matches nothing would silently switch off the grant-only rule."""
    with pytest.raises(ValueError, match="matches no value"):
        vbf.add_venture_backed_flag(orgs, rounds_uri, org_type_col="Org Type",
                                    for_profit_label="Commercial")


def test_secondary_market_alone_is_not_venture_backed(tmp_path):
    """A secondary-market sale (mostly PE / infrastructure stakes) does not make an org venture-backed."""
    rounds = pd.DataFrame([_round("pe-owned-co", "secondary_market"),
                           _round("pe-owned-co", "debt_financing")])
    uri = str(tmp_path / "funding_rounds.parquet")
    pqc.write_dataframe(rounds, uri)
    venture, grant = vbf.venture_backed_org_uuids(pqc.read_dataframe(uri))
    assert venture == set()
