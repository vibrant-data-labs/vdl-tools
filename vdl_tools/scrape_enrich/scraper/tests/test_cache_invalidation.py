"""Tests for delete_cached_urls against a real (in-memory SQLite) DB.

These exercise actual SQLAlchemy delete semantics rather than mocking the
query builder, since the function's entire job is the DB side-effect. Uses
StaticPool so the schema/data created by one session survives across the
sessions delete_cached_urls opens and closes internally.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from vdl_tools.scrape_enrich.scraper.cache_invalidation import delete_cached_urls
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    WebPagesParsed,
    WebPagesScraped,
)


@pytest.fixture
def make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    WebPagesParsed.__table__.create(engine)
    WebPagesScraped.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session


def _seed_host(session, home_key, home_url=None, with_subpage=True):
    """Seed WebPagesParsed + index (+ optional subpage) WebPagesScraped rows
    for a host. home_url can be overridden to reproduce real-world quirks
    (e.g. an inconsistent trailing slash)."""
    home_url = home_url or f"https://{home_key}"
    session.add(WebPagesParsed(
        cleaned_home_key=home_key,
        home_url=home_url,
        num_errors=0,
        combined_text="garbled text",
    ))
    session.add(WebPagesScraped(
        cleaned_key=home_key,
        home_url=home_url,
        subpath="/",
        page_type="index",
        response_status_code=200,
        num_errors=0,
        parsed_html="garbled text",
    ))
    if with_subpage:
        session.add(WebPagesScraped(
            cleaned_key=f"{home_key}/about",
            home_url=home_url,
            subpath="about",
            page_type="page",
            response_status_code=200,
            num_errors=0,
            parsed_html="garbled subpage text",
        ))


def test_delete_cached_urls_removes_host_and_subpages_only_for_matched_host(make_session):
    session = make_session()
    _seed_host(session, "example.org")
    _seed_host(session, "other.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="example.org").count() == 0
    assert check.query(WebPagesScraped).filter(
        WebPagesScraped.cleaned_key.like("example.org%")
    ).count() == 0
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="other.org").count() == 1
    assert check.query(WebPagesScraped).filter(
        WebPagesScraped.cleaned_key.like("other.org%")
    ).count() == 2


def test_delete_cached_urls_dry_run_counts_without_deleting(make_session):
    session = make_session()
    _seed_host(session, "example.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org"], session=make_session(), dry_run=True)

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "dry_run": True}

    check = make_session()
    assert check.query(WebPagesParsed).count() == 1
    assert check.query(WebPagesScraped).count() == 2


@pytest.mark.parametrize("url_variant", [
    "example.org",
    "https://example.org",
    "HTTPS://example.org",
    "example.org/",
    "example.org?utm=1",
])
def test_delete_cached_urls_matches_scheme_slash_and_query_variants_on_both_tables(make_session, url_variant):
    """extract_website_name strips scheme/query/trailing-slash, and both
    tables now key off it (not the raw home_url), so all these variants
    match reliably."""
    session = make_session()
    _seed_host(session, "example.org", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls([url_variant], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_matches_scraped_rows_despite_inconsistent_home_url_trailing_slash(make_session):
    """Regression: WebPagesScraped.home_url can carry a trailing slash
    (confirmed live on www.bgccam.org) while cleaned_key never does -
    matching must not depend on home_url's formatting."""
    session = make_session()
    _seed_host(session, "www.bgccam.org", home_url="https://www.bgccam.org/", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls(["www.bgccam.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_treats_www_prefix_as_a_distinct_host(make_session):
    """www.example.org and example.org are different cache keys (confirmed
    live: bgccam.org vs www.bgccam.org) - deleting one must not touch the other."""
    session = make_session()
    _seed_host(session, "bgccam.org")
    _seed_host(session, "www.bgccam.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["www.bgccam.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="bgccam.org").count() == 1
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="www.bgccam.org").count() == 0


def test_delete_cached_urls_subpage_prefix_match_does_not_catch_unrelated_similar_hosts(make_session):
    """The 'key/%' LIKE pattern must not match an unrelated host that shares
    a prefix without a '/' boundary (e.g. example.org.other.com)."""
    session = make_session()
    _seed_host(session, "example.org")
    _seed_host(session, "example.org.other.com", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="example.org.other.com").count() == 1
    assert check.query(WebPagesScraped).filter_by(cleaned_key="example.org.other.com").count() == 1


def test_delete_cached_urls_ignores_empty_and_none_entries(make_session):
    session = make_session()
    _seed_host(session, "example.org", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org", None, ""], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_empty_list_deletes_nothing(make_session):
    session = make_session()
    _seed_host(session, "example.org")
    session.commit()
    session.close()

    result = delete_cached_urls([], session=make_session())

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).count() == 1
