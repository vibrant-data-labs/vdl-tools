import pytest

from vdl_tools.portfolio_comparison.intake.normalize import (
    name_tokens,
    normalize_domain,
    normalize_ein,
    normalize_name,
)


@pytest.mark.parametrize("raw,expected", [
    ("Acme, Inc.", "acme"),
    ("Acme Incorporated", "acme"),
    ("ACME Climate Co", "acme climate"),
    ("The Nature Conservancy", "the nature conservancy"),
    ("Solaris  Energy   LLC", "solaris energy"),
    ("Ørsted A/S", "ørsted a s"),
    ("", ""),
    (None, ""),
])
def test_normalize_name(raw, expected):
    assert normalize_name(raw) == expected


def test_normalize_name_only_strips_trailing_suffixes():
    # "Co" inside the name must survive; only trailing legal suffixes drop.
    assert normalize_name("Co-op Power Inc") == "co op power"


@pytest.mark.parametrize("raw,expected", [
    ("https://www.acme.com/about", "acme.com"),
    ("http://acme.com", "acme.com"),
    ("acme.com", "acme.com"),
    ("www.acme.com/", "acme.com"),
    ("HTTPS://ACME.COM:443/x?y=1", "acme.com"),
    ("", ""),
    (None, ""),
])
def test_normalize_domain(raw, expected):
    assert normalize_domain(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("12-3456789", "12-3456789"),
    ("123456789", "12-3456789"),
    (123456789, "12-3456789"),
    (12345678, "01-2345678"),  # leading zero lost to numeric parsing
    ("12-345", ""),
    (None, ""),
])
def test_normalize_ein(raw, expected):
    assert normalize_ein(raw) == expected


def test_name_tokens():
    assert name_tokens("Acme Climate, Inc.") == {"acme", "climate"}
