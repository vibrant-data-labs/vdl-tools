import pandas as pd
import pytest

from vdl_tools.scrape_enrich.funder_names import (
    clean_funder_text,
    funder_filed_name,
    funder_key,
    funder_kind,
    pair_cb_gt_funders,
    pick_spelling,
    us_state,
    website_domain,
)


@pytest.mark.parametrize(
    "line1, line2, dba, expected",
    [
        ("COMMUNITY FOUNDATION OF", "ELKHART COUNTY INC", None, "COMMUNITY FOUNDATION OF ELKHART COUNTY INC"),
        ("AMERICAN FEDERATION OF LABOR AND", "CONGRESS OF INDUSTRIAL ORGANIZATIONS", None,
         "AMERICAN FEDERATION OF LABOR AND CONGRESS OF INDUSTRIAL ORGANIZATIONS"),
        ("FOUNDATION", "C/O COATUE MANAGEMENT", None, "FOUNDATION"),
        ("NEIL AND VIRGINIA WEISS FOUNDATION IN", "MEMORY OF STEVEN WEISSC/O SHELTER ROCK", None,
         "NEIL AND VIRGINIA WEISS FOUNDATION IN MEMORY OF STEVEN WEISS"),
        ("RATTNER FAMILY FOUNDATION INC", "C/O RATTNER FAMILY OFFICE", None, "RATTNER FAMILY FOUNDATION INC"),
        ("WALTON FAMILY FOUNDATION", None, None, "WALTON FAMILY FOUNDATION"),
    ],
)
def test_filed_name_completes_the_lines_and_drops_care_of(line1, line2, dba, expected):
    assert funder_filed_name(line1, line2, dba) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Gmoney ‏‏‎ ‎", "Gmoney"),
        ("David ‎‏Adeleke", "David Adeleke"),
        ("Dauntless Ventures.", "Dauntless Ventures"),
        ("Mayﬁeld Fund, LLC.", "Mayfield Fund, LLC"),
        ("FAST — by GETTYLAB", "FAST - by GETTYLAB"),
        (None, ""),
    ],
)
def test_clean_funder_text(raw, expected):
    assert clean_funder_text(raw) == expected


@pytest.mark.parametrize(
    "a, b",
    [
        ("The W.K. Kellogg Foundation", "WK KELLOGG FOUNDATION"),
        ("Alfred P. Sloan Foundation", "ALFRED P SLOAN FOUNDATION"),
        ("Kresge Foundation", "THE KRESGE FOUNDATION INC"),
        ("Women's Sports Foundation", "Womens Sports Foundation"),
        ("Naples Children & Education Foundation", "NAPLES CHILDREN AND EDUCATION FOUNDATION INC"),
        ("Patrick J. McGovern Foundation", "THE PATRICK J MCGOVERN FOUNDATION INC"),
    ],
)
def test_funder_key_ignores_case_punctuation_the_and_legal_suffix(a, b):
    assert funder_key(a) == funder_key(b)


def test_funder_key_keeps_foundation_fund_and_trust():
    # "X Foundation" and "X Fund" are often different funders
    assert funder_key("Acme Foundation") != funder_key("Acme Fund")
    assert funder_key("Acme Trust") != funder_key("Acme")


@pytest.mark.parametrize(
    "name, ein, kind",
    [
        ("AMAZONSMILE FOUNDATION", "46-2626883", "pass_through"),
        ("Fidelity Investments Charitable Gift Fund", "110303001", "pass_through"),
        ("Angel Investor(s)", None, "placeholder"),
        ("Multiple Undisclosed Angel Investors", None, "placeholder"),
        ("Confidential", None, "placeholder"),
        ("Anonymous Angel", None, "placeholder"),
        ("Angel Capital Group", None, None),
        ("Siegel Family Endowment", None, None),
        ("The Rockefeller Foundation", "131659629", None),
    ],
)
def test_funder_kind(name, ein, kind):
    assert funder_kind(name, ein) == kind


def test_pick_spelling_prefers_mixed_case_then_most_frequent():
    assert pick_spelling(["THE CLEVELAND FOUNDATION", "The Cleveland Foundation"]) == "The Cleveland Foundation"
    assert pick_spelling(["AARP", "AARP"]) == "AARP"
    assert pick_spelling(["A Fund", "B Fund", "B Fund"]) == "B Fund"
    assert pick_spelling([]) == ""


def test_us_state_and_website_domain():
    assert us_state("California") == "CA"
    assert us_state("ny") == "NY"
    assert us_state("North America") is None
    assert website_domain("https://www.Abstract.com/about") == "abstract.com"
    assert website_domain("N/A") is None
    assert website_domain("https://facebook.com/x") is None


def _cb(*rows):
    return pd.DataFrame(rows, columns=["uuid", "name", "legal_name", "website", "state"])


def _gt(*rows):
    return pd.DataFrame(rows, columns=["ein", "name", "website", "state"])


def test_pairs_when_state_matches_and_the_name_is_unique():
    cb = _cb(("u1", "The Cleveland Foundation", None, None, "Ohio"))
    gt = _gt(("e1", "THE CLEVELAND FOUNDATION", None, "OH"))
    assert pair_cb_gt_funders(cb, gt).to_dict("records") == [
        {"uuid": "u1", "ein": "e1", "reason": "state_unique_name"}
    ]


def test_pairs_on_website_even_when_the_name_is_shared():
    cb = _cb(("u1", "Horizon Foundation", None, "https://horizonfdn.org", "Maryland"))
    gt = _gt(
        ("e1", "HORIZON FOUNDATION INC", "horizonfdn.org", "MD"),
        ("e2", "THE HORIZON FOUNDATION", None, "TX"),
    )
    assert pair_cb_gt_funders(cb, gt)["ein"].tolist() == ["e1"]


def test_does_not_pair_when_states_differ():
    # ECMC files from MN but is headquartered in CA: unpaired on purpose
    cb = _cb(("u1", "ECMC Foundation", None, None, "California"))
    gt = _gt(("e1", "ECMC FOUNDATION", None, "MN"))
    assert pair_cb_gt_funders(cb, gt).empty


def test_does_not_pair_a_shared_name_without_a_website():
    cb = _cb(("u1", "Green Family Foundation", None, None, "Pennsylvania"))
    gt = _gt(
        ("e1", "GREEN FAMILY FOUNDATION INC", None, "PA"),
        ("e2", "GREEN FAMILY FOUNDATION", None, "FL"),
    )
    assert pair_cb_gt_funders(cb, gt).empty


def test_does_not_pair_with_no_second_signal():
    cb = _cb(("u1", "Charles Schwab Foundation", None, None, None))
    gt = _gt(("e1", "CHARLES SCHWAB FOUNDATION", None, "CO"))
    assert pair_cb_gt_funders(cb, gt).empty


def test_pairs_on_legal_name_when_one_ein_carries_the_key():
    cb = _cb(("u1", "Patrick J. McGovern Foundation", "The Patrick J McGovern Foundation Inc", None, None))
    gt = _gt(("e1", "THE PATRICK J MCGOVERN FOUNDATION INC", None, "MA"))
    assert pair_cb_gt_funders(cb, gt)["reason"].tolist() == ["website_or_legal_name"]


def test_an_ein_matched_by_two_uuids_is_left_unpaired():
    cb = _cb(
        ("u1", "Echo Foundation", None, "echo.org", "Georgia"),
        ("u2", "Echo Foundation", None, "echo.org", "Georgia"),
    )
    gt = _gt(("e1", "ECHO FOUNDATION", "echo.org", "GA"))
    assert pair_cb_gt_funders(cb, gt).empty
