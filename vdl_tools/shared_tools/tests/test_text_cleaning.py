r"""clean_non_ascii_text / clean_scraped_text: Unicode spaces separate words.

Regression context: clean_non_ascii_text ran text.encode("ascii", "ignore")
before replacing "\xa0", so non-breaking and other Unicode spaces were deleted
outright and the words on either side were glued together ("formed in
2017\xa0focused" became "2017focused"). Every in-house page text passes through
it via clean_scraped_text in website_processor and scrape_websites.
"""
import pytest

from vdl_tools.shared_tools.tools.text_cleaning import (
    clean_non_ascii_text,
    clean_scraped_text,
)

UNICODE_SPACES = [
    "\xa0",  # no-break space
    " ",  # ogham space mark
    " ",  # en space
    " ",  # thin space
    " ",  # hair space
    " ",  # narrow no-break space
    " ",  # medium mathematical space
    "　",  # ideographic space
    " ",  # line separator
    "\x85",  # next line
]


def test_non_breaking_space_keeps_words_apart():
    assert clean_non_ascii_text("formed in 2017\xa0focused") == "formed in 2017 focused"


@pytest.mark.parametrize("space", UNICODE_SPACES)
def test_unicode_spaces_become_a_standard_space(space):
    assert clean_non_ascii_text(f"climate{space}finance") == "climate finance"


@pytest.mark.parametrize("space", ["\t", "\r", "\f", "\v"])
def test_ascii_whitespace_other_than_newline_becomes_a_standard_space(space):
    assert clean_non_ascii_text(f"climate{space}finance") == "climate finance"


def test_newlines_are_kept():
    assert clean_non_ascii_text("line one\nline two\xa0\nline three") == (
        "line one\nline two \nline three"
    )


def test_other_non_ascii_characters_are_still_removed():
    # Zero-width space is a format character, not whitespace, so it is removed.
    assert clean_non_ascii_text("café — net​zero 🌍") == "caf  netzero "


def test_three_or_more_spaces_collapse_to_one():
    assert clean_non_ascii_text("a\xa0\xa0\xa0b") == "a b"
    assert clean_non_ascii_text("a \xa0b") == "a  b"


def test_ascii_text_without_special_whitespace_is_unchanged():
    text = "Vibrant Data Labs maps climate finance.\n\n* Funders\n* Grantees"
    assert clean_non_ascii_text(text) == text


def test_clean_scraped_text_keeps_words_apart():
    assert clean_scraped_text("formed in 2017\xa0focused") == "formed in 2017 focused"
    assert clean_scraped_text("solar\xa0\xa0power") == "solar power"
