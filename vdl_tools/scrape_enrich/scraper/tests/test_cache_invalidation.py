"""Tests for delete_cached_urls against a real (in-memory SQLite) DB.

These exercise actual SQLAlchemy delete semantics rather than mocking the
query builder, since the function's entire job is the DB side-effect. Uses
StaticPool so the schema/data created by one session survives across the
sessions delete_cached_urls opens and closes internally.

Every delete test runs twice: against the in-house pair (web_pages_parsed,
web_pages_scraped) and the Firecrawl pair (the _v2 tables) with purge=True.
The Firecrawl default, marking the home row for a re-fetch, has its own tests
at the end.
"""

from functools import partial
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from vdl_tools.scrape_enrich.scraper.cache_invalidation import BACKEND_TABLES, delete_cached_urls
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesHistoryV2


@pytest.fixture(params=sorted(BACKEND_TABLES))
def tables(request):
    """One backend's models and delete_cached_urls bound to that backend, deleting rows."""
    parsed, scraped = BACKEND_TABLES[request.param]
    return SimpleNamespace(backend=request.param, parsed=parsed, scraped=scraped,
                           delete=partial(delete_cached_urls, backend=request.param, purge=True))


@pytest.fixture
def make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    for parsed, scraped in BACKEND_TABLES.values():
        parsed.__table__.create(engine)
        scraped.__table__.create(engine)
    WebPagesHistoryV2.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session


def _seed_host(session, tables, home_key, home_url=None, with_subpage=True):
    """Seed a parsed row + index (+ optional subpage) scraped rows
    for a host. home_url can be overridden to reproduce real-world quirks
    (e.g. an inconsistent trailing slash)."""
    home_url = home_url or f"https://{home_key}"
    session.add(tables.parsed(
        cleaned_home_key=home_key,
        home_url=home_url,
        num_errors=0,
        combined_text="garbled text",
    ))
    session.add(tables.scraped(
        cleaned_key=home_key,
        home_url=home_url,
        subpath="/",
        page_type="index",
        response_status_code=200,
        num_errors=0,
        parsed_html="garbled text",
    ))
    if with_subpage:
        session.add(tables.scraped(
            cleaned_key=f"{home_key}/about",
            home_url=home_url,
            subpath="about",
            page_type="page",
            response_status_code=200,
            num_errors=0,
            parsed_html="garbled subpage text",
        ))


def test_delete_cached_urls_removes_host_and_subpages_only_for_matched_host(make_session, tables):
    session = make_session()
    _seed_host(session, tables, "example.org")
    _seed_host(session, tables, "other.org")
    session.commit()
    session.close()

    result = tables.delete(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(tables.parsed).filter_by(cleaned_home_key="example.org").count() == 0
    assert check.query(tables.scraped).filter(
        tables.scraped.cleaned_key.like("example.org%")
    ).count() == 0
    assert check.query(tables.parsed).filter_by(cleaned_home_key="other.org").count() == 1
    assert check.query(tables.scraped).filter(
        tables.scraped.cleaned_key.like("other.org%")
    ).count() == 2


def test_delete_cached_urls_dry_run_counts_without_deleting(make_session, tables):
    session = make_session()
    _seed_host(session, tables, "example.org")
    session.commit()
    session.close()

    result = tables.delete(["example.org"], session=make_session(), dry_run=True)

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "dry_run": True}

    check = make_session()
    assert check.query(tables.parsed).count() == 1
    assert check.query(tables.scraped).count() == 2


@pytest.mark.parametrize("url_variant", [
    "example.org",
    "https://example.org",
    "HTTPS://example.org",
    "example.org/",
    "example.org?utm=1",
])
def test_delete_cached_urls_matches_scheme_slash_and_query_variants_on_both_tables(make_session, tables, url_variant):
    """extract_website_name strips scheme/query/trailing-slash, and both
    tables now key off it (not the raw home_url), so all these variants
    match reliably."""
    session = make_session()
    _seed_host(session, tables, "example.org", with_subpage=False)
    session.commit()
    session.close()

    result = tables.delete([url_variant], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_matches_scraped_rows_despite_inconsistent_home_url_trailing_slash(make_session, tables):
    """Regression: tables.scraped.home_url can carry a trailing slash
    (confirmed live on www.bgccam.org) while cleaned_key never does -
    matching must not depend on home_url's formatting."""
    session = make_session()
    _seed_host(session, tables, "www.bgccam.org", home_url="https://www.bgccam.org/", with_subpage=False)
    session.commit()
    session.close()

    result = tables.delete(["www.bgccam.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_treats_www_prefix_as_a_distinct_host(make_session, tables):
    """www.example.org and example.org are different cache keys (confirmed
    live: bgccam.org vs www.bgccam.org) - deleting one must not touch the other."""
    session = make_session()
    _seed_host(session, tables, "bgccam.org")
    _seed_host(session, tables, "www.bgccam.org")
    session.commit()
    session.close()

    result = tables.delete(["www.bgccam.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(tables.parsed).filter_by(cleaned_home_key="bgccam.org").count() == 1
    assert check.query(tables.parsed).filter_by(cleaned_home_key="www.bgccam.org").count() == 0


def test_delete_cached_urls_subpage_prefix_match_does_not_catch_unrelated_similar_hosts(make_session, tables):
    """The 'key/%' LIKE pattern must not match an unrelated host that shares
    a prefix without a '/' boundary (e.g. example.org.other.com)."""
    session = make_session()
    _seed_host(session, tables, "example.org")
    _seed_host(session, tables, "example.org.other.com", with_subpage=False)
    session.commit()
    session.close()

    result = tables.delete(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}

    check = make_session()
    assert check.query(tables.parsed).filter_by(cleaned_home_key="example.org.other.com").count() == 1
    assert check.query(tables.scraped).filter_by(cleaned_key="example.org.other.com").count() == 1


def test_delete_cached_urls_ignores_empty_and_none_entries(make_session, tables):
    session = make_session()
    _seed_host(session, tables, "example.org", with_subpage=False)
    session.commit()
    session.close()

    result = tables.delete(["example.org", None, ""], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 1, "dry_run": False}


def test_delete_cached_urls_empty_list_deletes_nothing(make_session, tables):
    session = make_session()
    _seed_host(session, tables, "example.org")
    session.commit()
    session.close()

    result = tables.delete([], session=make_session())

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "dry_run": False}

    check = make_session()
    assert check.query(tables.parsed).count() == 1


def test_delete_cached_urls_leaves_the_other_backend_and_the_history(make_session, tables):
    """The two caches are independent, and the history row is the only other
    copy of a page's text."""
    import datetime as dt

    other = next(b for b in BACKEND_TABLES if b != tables.backend)
    other_parsed, other_scraped = BACKEND_TABLES[other]
    session = make_session()
    _seed_host(session, tables, "example.org")
    _seed_host(session, SimpleNamespace(parsed=other_parsed, scraped=other_scraped), "example.org")
    session.add(WebPagesHistoryV2(cleaned_key="example.org", fetched_at=dt.datetime(2026, 3, 1),
                                  parsed_html="last good text"))
    session.commit()
    session.close()

    result = tables.delete(["example.org"], session=make_session())

    assert result == {"parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}
    check = make_session()
    assert check.query(other_parsed).count() == 1
    assert check.query(other_scraped).count() == 2
    assert check.query(WebPagesHistoryV2).count() == 1


def test_delete_cached_urls_defaults_to_the_in_house_tables(make_session):
    parsed, scraped = BACKEND_TABLES["inhouse"]
    session = make_session()
    _seed_host(session, SimpleNamespace(parsed=parsed, scraped=scraped), "example.org")
    session.commit()
    session.close()

    assert delete_cached_urls(["example.org"], session=make_session()) == {
        "parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}


def test_delete_cached_urls_rejects_an_unknown_backend(make_session):
    with pytest.raises(ValueError, match="backend must be one of"):
        delete_cached_urls(["example.org"], session=make_session(), backend="olostep")


# ---------------------------------------------------------------------------
# The Firecrawl default: mark for a re-fetch, delete nothing
# ---------------------------------------------------------------------------

def _firecrawl_tables():
    parsed, scraped = BACKEND_TABLES["firecrawl"]
    return SimpleNamespace(parsed=parsed, scraped=scraped)


def test_firecrawl_default_marks_the_home_row_and_deletes_nothing(make_session):
    tables = _firecrawl_tables()
    session = make_session()
    _seed_host(session, tables, "example.org")
    _seed_host(session, tables, "other.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["https://example.org/"], session=make_session(), backend="firecrawl")

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "refetch_marked": 1, "dry_run": False}
    check = make_session()
    assert check.query(tables.parsed).count() == 2
    assert check.query(tables.scraped).count() == 4
    marked = {r.cleaned_key for r in check.query(tables.scraped).filter(
        tables.scraped.refetch_requested_at.isnot(None))}
    assert marked == {"example.org"}                       # the home row only, and only this host
    assert check.get(tables.scraped, "example.org").parsed_html == "garbled text"   # the text is still there


def test_firecrawl_default_dry_run_marks_nothing(make_session):
    tables = _firecrawl_tables()
    session = make_session()
    _seed_host(session, tables, "example.org")
    session.commit()
    session.close()

    result = delete_cached_urls(["example.org"], session=make_session(), backend="firecrawl", dry_run=True)

    assert result == {"parsed_deleted": 0, "scraped_deleted": 0, "refetch_marked": 0, "dry_run": True}
    assert make_session().query(tables.scraped).filter(
        tables.scraped.refetch_requested_at.isnot(None)).count() == 0


def test_purge_is_ignored_on_the_in_house_tables(make_session):
    """Deleting is the only way to refresh the in-house cache, so purge=False still deletes."""
    parsed, scraped = BACKEND_TABLES["inhouse"]
    session = make_session()
    _seed_host(session, SimpleNamespace(parsed=parsed, scraped=scraped), "example.org")
    session.commit()
    session.close()

    assert delete_cached_urls(["example.org"], session=make_session(), purge=False) == {
        "parsed_deleted": 1, "scraped_deleted": 2, "dry_run": False}
