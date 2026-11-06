import pytest

from vdl_tools.shared_tools.funders.ids import FUNDER_UID_RE, normalize_ein, slugify


@pytest.mark.parametrize(
    "name, slug",
    [
        ("Siegel Family Endowment", "siegel-family-endowment"),
        ("S&P Global Foundation", "sandp-global-foundation"),
        ("  The  Ford Foundation! ", "the-ford-foundation"),
        ("", ""),
        (None, ""),
    ],
)
def test_slugify(name, slug):
    assert slugify(name) == slug


@pytest.mark.parametrize(
    "value, ein",
    [("01-2345678", "012345678"), ("012345678", "012345678"), (" 01-2345678 ", "012345678"),
     ("12-345-6789", None), ("123", None), (123456789, None), (None, None)],
)
def test_normalize_ein(value, ein):
    assert normalize_ein(value) == ein


def test_funder_uid_regex():
    assert FUNDER_UID_RE.match("cb:11111111-1111-1111-1111-111111111111")
    assert FUNDER_UID_RE.match("ein:012345678")
    assert FUNDER_UID_RE.match("name:acme-capital")
    assert not FUNDER_UID_RE.match("ein:12345")
    assert not FUNDER_UID_RE.match("acme")
