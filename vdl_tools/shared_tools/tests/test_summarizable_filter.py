"""is_summarizable: the choke point that keeps junk scrapes from ever reaching
the summarizer, so it is never asked to describe a page it can only guess at.

Regression context: before this filter existed, the summarizer was given
every non-empty scrape regardless of quality. Asked to describe a parked
domain, a bot wall, or binary mojibake, it did not reliably report seeing
nothing — it sometimes invented a plausible-sounding description from the
organization's name instead. Boy Scouts of America, scraped behind a
Cloudflare wall, was summarized as "focuses on providing youth development
programs through scouting activities"; a domain-for-sale listing for an
unrelated org was summarized as a description of that org's programs.
"""

from vdl_tools.shared_tools.web_summarization.website_summarization_psql import (
    is_summarizable,
)

REAL_PAGE = "Real org text about climate work, long enough to clear the thin threshold. " * 5
WALL = "This website uses a security service to protect against malicious bots."
PARKED = "This domain is for sale. Buy this domain today. " * 5
GARBLED = "".join(chr(i % 20 + 1) if i % 5 == 0 else "x" for i in range(500))


def test_real_content_is_summarizable():
    assert is_summarizable(REAL_PAGE)


def test_thin_real_content_is_still_summarizable():
    # "thin" is short, not junk -- it may still be a true, if brief, page.
    assert is_summarizable("A small but real page about our mission.")


def test_junk_classes_are_not_summarizable():
    assert not is_summarizable(WALL)
    assert not is_summarizable(PARKED)
    assert not is_summarizable(GARBLED)
    assert not is_summarizable(None)
    assert not is_summarizable("")


def test_no_text_is_never_summarizable_regardless_of_errors():
    assert not is_summarizable(None, num_errors=0)
    assert not is_summarizable("", num_errors=5)
