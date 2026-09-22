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


def _seed_host(session, home_key, home_url, with_subpage=True):
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
    _seed_host(session, "example.org", "https://example.org")
    _seed_host(session, "other.org", "https://other.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="example.org").count() == 0
    assert check.query(WebPagesScraped).filter_by(home_url="https://example.org").count() == 0
    assert check.query(WebPagesParsed).filter_by(cleaned_home_key="other.org").count() == 1
    assert check.query(WebPagesScraped).filter_by(home_url="https://other.org").count() == 2


def test_delete_cached_urls_dry_run_counts_without_deleting(make_session):
    session = make_session()
    _seed_host(session, "example.org", "https://example.org")
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
])
def test_delete_cached_urls_matches_bare_and_scheme_variants_on_both_tables(make_session, url_variant):
    """A bare domain or scheme-case variant matches the row on BOTH tables,
    since ensure_url_scheme normalizes it to the same 'https://example.org'
    that scrape_websites_psql would have stored as home_url."""
    session = make_session()
    _seed_host(session, "example.org", "https://example.org", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls([url_variant], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_query_string_only_matches_parsed_table(make_session):
    """extract_website_name strips the query string (so WebPagesParsed still
    matches), but ensure_url_scheme does not (so WebPagesScraped, keyed on
    the literal home_url, does not) - the caller must pass the exact URL
    that was scraped to clear subpages too, not just the bare domain."""
    session = make_session()
    _seed_host(session, "example.org", "https://example.org", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org?utm=1"], session=make_session())

    assert result["parsed_deleted"] == 1
    assert result["scraped_deleted"] == 0


def test_delete_cached_urls_ignores_empty_and_none_entries(make_session):
    session = make_session()
    _seed_host(session, "example.org", "https://example.org", with_subpage=False)
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org", None, ""], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_empty_list_deletes_nothing(make_session):
    session = make_session()
    _seed_host(session, "example.org", "https://example.org")
    session.commit()
    session.close()

    result = delete_cached_urls([], session=make_session())

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "dry_run": False}

    check = make_session()
    assert check.query(WebPagesParsed).count() == 1
