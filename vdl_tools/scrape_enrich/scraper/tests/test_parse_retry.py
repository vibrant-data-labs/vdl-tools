"""Tests for parse-layer (WebPagesParsed) retry gating.

These pin down the predicate that decides whether a previously-parsed site is
re-combined or returned as-is. The bug this guards against: a single combine
failure (num_errors -> 1) used to be permanent because the combine step only ran
for sites with NO parsed row at all, so the error counter could never climb to
max_errors. The predicate below is the single source of truth shared by both the
"return stale rows" filter and the "what to re-combine" filter.
"""

import pytest

from vdl_tools.scrape_enrich.scraper.scrape_websites import (
    _parsed_row_is_retryable,
    MAX_ERRORS,
)


@pytest.mark.parametrize(
    "num_errors,max_errors,expected",
    [
        # success: 0 (or None) errors -> done, never retry
        (0, 5, False),
        (None, 5, False),
        # failures under the limit -> retry
        (1, 5, True),
        (4, 5, True),
        # at or over the limit -> given up, return as-is
        (5, 5, False),
        (6, 5, False),
        # boundary: with max_errors=1 even a single failure is already "given up"
        (1, 1, False),
        # boundary: with max_errors=2, one failure is retryable, two is not
        (1, 2, True),
        (2, 2, False),
    ],
)
def test_parsed_row_is_retryable(num_errors, max_errors, expected):
    assert _parsed_row_is_retryable(num_errors, max_errors) is expected


def test_lifecycle_with_default_max_errors():
    """Walk a site through its parse lifecycle under the default MAX_ERRORS.

    A failing site should be retryable for counts 1..MAX_ERRORS-1, then drop out
    of the retry set once it reaches MAX_ERRORS. This is what lets the error
    counter actually accumulate to the limit instead of being pinned at 1.
    """
    # never parsed (count 0 / success) -> not retryable via this predicate
    assert _parsed_row_is_retryable(0, MAX_ERRORS) is False
    # every count strictly between 0 and MAX_ERRORS retries
    assert all(_parsed_row_is_retryable(n, MAX_ERRORS) for n in range(1, MAX_ERRORS))
    # at the limit, we give up
    assert _parsed_row_is_retryable(MAX_ERRORS, MAX_ERRORS) is False
