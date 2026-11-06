import pytest

from vdl_tools.shared_tools.funders.names import (
    clean_funder_text,
    funder_filed_name,
    funder_key,
    funder_name_and_dba,
    pick_spelling,
    us_state,
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


def test_pick_spelling_prefers_mixed_case_then_most_frequent():
    assert pick_spelling(["THE CLEVELAND FOUNDATION", "The Cleveland Foundation"]) == "The Cleveland Foundation"
    assert pick_spelling(["AARP", "AARP"]) == "AARP"
    assert pick_spelling(["A Fund", "B Fund", "B Fund"]) == "B Fund"
    assert pick_spelling([]) == ""


def test_us_state():
    assert us_state("California") == "CA"
    assert us_state("ny") == "NY"
    assert us_state("North America") is None


def test_funder_name_and_dba():
    assert funder_name_and_dba("THE OPPORTUNITY TRUST", None, "ZEST EDUCATION") == ("THE OPPORTUNITY TRUST", "ZEST EDUCATION")
    assert funder_name_and_dba("WALTON FAMILY FOUNDATION") == ("WALTON FAMILY FOUNDATION", "")
