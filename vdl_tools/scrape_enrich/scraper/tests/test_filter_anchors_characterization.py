"""Characterization (golden-snapshot) tests for link extraction.

These tests pin down what ``filter_anchors`` / ``extract_website_links``
*currently* do — quirks and known bugs included. They are NOT a specification
of desired behavior. Their job is to make any future change to the link logic
(in particular, the proposed ``urllib.parse``-based rewrite) produce an explicit,
reviewable diff instead of a silent behavior change.

When you intentionally change the logic, expect some of these to fail. For each
failure, decide whether the new output is correct, then update the golden value
in the same commit. Cases known to be wrong today (and therefore expected to
change in a rewrite) are flagged with ``# KNOWN-WRONG`` below.

See issue: scraper link extraction — urlparse rewrite.
"""

import pytest

from vdl_tools.scrape_enrich.scraper.website_processor import (
    filter_anchors,
    extract_website_links,
)


# (base_url, href, current_output)  -- current_output is None when the link is dropped.
# Generated empirically from the current implementation; do not hand-edit values
# without re-running against the code.
GOLDEN_FILTER_ANCHORS = [
    # --- ordinary same-site links (stable, should survive any rewrite) ---
    ('http://www.aises.org', '/about', '/about'),
    ('http://www.aises.org', '/news', '/news'),
    ('http://www.aises.org', 'http://www.aises.org/news', '/news'),
    ('http://www.aises.org', 'https://aises.org/news', '/news'),

    # --- scheme-insensitivity (the bug fixed in PR #138; guard against regression) ---
    ('http://www.aises.org', 'https://www.aises.org/programs', '/programs'),
    ('https://www.aises.org', 'http://www.aises.org/legacy', '/legacy'),
    ('https://www.aises.org', '/about', '/about'),

    # --- relative links WITHOUT a leading slash ---
    ('http://www.aises.org', 'about', None),    # KNOWN-WRONG: dropped today; urljoin would keep -> '/about'
    ('http://www.aises.org', './about', None),  # KNOWN-WRONG: dropped today; urljoin would keep -> '/about'

    # --- normalization quirks ---
    ('http://www.aises.org', '/about/', '/about/'),         # trailing slash preserved (deduped downstream)
    ('http://www.aises.org', '/about#team', '/about#team'), # KNOWN-WRONG: fragment kept; should collapse to '/about'
    ('http://www.aises.org', '/about?ref=partner.com', None),  # KNOWN-WRONG: dropped due to dot in query string

    # --- protocol-relative ---
    ('http://www.aises.org', '//www.aises.org/contact', 'contact'),  # quirk: no leading slash on result

    # --- domain boundary cases ---
    ('http://www.aises.org', 'https://aises.org.evil.com/phish', '/phish'),  # KNOWN-WRONG: lookalike kept as same-site
    ('http://www.aises.org', 'https://foo.aises.org/sub', None),             # subdomain dropped
    ('http://www.aises.org', 'https://twitter.com/aises', None),             # external dropped

    # --- non-http schemes ---
    ('http://www.aises.org', 'mailto:info@aises.org', None),
    ('http://www.aises.org', 'tel:+15551234', None),

    # --- index / blobs / extensions ---
    ('http://www.aises.org', '/', None),
    ('http://www.aises.org', '/promo.mp4', None),
    ('http://www.aises.org', '/files/report.pdf', None),
    ('http://www.aises.org', '/page.html', '/page.html'),
    ('http://www.aises.org', '/reports/v1.2', None),  # KNOWN-WRONG: legit page dropped (dot in path segment)
]


@pytest.mark.parametrize("base,href,expected", GOLDEN_FILTER_ANCHORS)
def test_filter_anchors_current_behavior(base, href, expected):
    """Snapshot of per-link behavior. A failure means the link logic changed."""
    _, res_links = filter_anchors(base, [href])
    actual = res_links[0] if res_links else None
    assert actual == expected


def test_filter_anchors_scheme_insensitive_regression():
    """Explicit guard for the PR #138 fix: an http base must still match https hrefs."""
    _, res = filter_anchors('http://www.aises.org', ['https://www.aises.org/about'])
    assert res == ['/about']
    _, res = filter_anchors('https://www.aises.org', ['http://www.aises.org/about'])
    assert res == ['/about']


# --- integration snapshot through extract_website_links (includes filter_links) ---

SAMPLE_HTML = """
<html><body>
  <nav>
    <a href="/about">About Us</a>
    <a href="https://www.aises.org/programs">Programs</a>
    <a href="/news">News</a>
    <a href="/privacy-policy">Privacy</a>
    <a href="https://twitter.com/aises">Twitter</a>
    <a href="mailto:info@aises.org">Email</a>
  </nav>
</body></html>
"""


def test_extract_website_links_all_current_behavior():
    """subpage_type='all' returns the deduped same-site relative paths."""
    links = extract_website_links('http://www.aises.org', SAMPLE_HTML, 'all')
    assert sorted(links) == ['/about', '/news', '/privacy-policy', '/programs']


def test_extract_website_links_about_applies_keep_paths():
    """subpage_type='about' drops paths not in PATHS_TO_KEEP (e.g. privacy-policy)."""
    links = extract_website_links('http://www.aises.org', SAMPLE_HTML, 'about')
    # 'about', 'news', 'programs' are in PATHS_TO_KEEP; 'privacy-policy' is not.
    assert 'privacy-policy' not in {l.strip('/') for l in links}
    assert {'about', 'news', 'programs'}.issubset({l.strip('/') for l in links})
