"""Orchestration tests for scrape_websites_firecrawl against a fake Firecrawl.

The vendor is ``FakeFirecrawl`` from the client tests behind an
``httpx.MockTransport``; the database is in-memory SQLite holding only the v2
tables, so a write to ``web_pages_scraped`` or ``web_pages_parsed`` fails the
test with "no such table".
"""

import datetime as dt

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
from vdl_tools.scrape_enrich.scraper.firecrawl import pipeline as pl
from vdl_tools.scrape_enrich.scraper.firecrawl.client import FirecrawlClient
from vdl_tools.scrape_enrich.scraper.firecrawl.tests.test_client import (
    KEY,
    FakeFirecrawl,
    SleepRecorder,
    api_error,
    page,
    run,
)
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    OrgSiteKey,
    ScrapeRun,
    WebPagesHistoryV2,
    WebPagesParsedV2,
    WebPagesScrapedV2,
)

HOME = "https://www.example.org"
KEY_HOME = "www.example.org"
T0 = dt.datetime(2026, 11, 1, 12, 0, 0)
LINKS = (f"{HOME}/about", f"{HOME}/team", f"{HOME}/mission", f"{HOME}/privacy", "https://elsewhere.com/about")


def md(topic: str) -> str:
    """A page long enough to be ``ok``, with text no other topic shares."""
    return f"# {topic}\n\n" + " ".join(
        f"{topic} sentence {i} about restoring the {topic} river basin wetlands." for i in range(12))


def site(vendor: FakeFirecrawl, home=HOME, links=LINKS, **pages) -> FakeFirecrawl:
    """Script a home page and its subpages (``about=...``); values are answers or markdown."""
    vendor.script(home, page(markdown=md("home"), links=links, url=home + "/", source=home))
    for name, answer in pages.items():
        url = f"{home}/{name}"
        vendor.script(url, answer if not isinstance(answer, str) else page(markdown=answer, url=url, source=url, links=()))
    return vendor


def standard_site(vendor: FakeFirecrawl) -> FakeFirecrawl:
    return site(vendor, about=md("about"), team=md("team"), mission=md("mission"))


class Db:
    def __init__(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        for model in (ScrapeRun, WebPagesScrapedV2, WebPagesParsedV2, WebPagesHistoryV2, OrgSiteKey):
            model.__table__.create(engine)
        self.Session = sessionmaker(bind=engine)

    def page(self, key) -> WebPagesScrapedV2:
        return self.Session().get(WebPagesScrapedV2, key)

    def org(self, key=KEY_HOME) -> WebPagesParsedV2:
        return self.Session().get(WebPagesParsedV2, key)

    def all(self, model):
        return self.Session().query(model).all()


@pytest.fixture
def db():
    return Db()


def scrape(db, vendor, urls=(HOME,), *, now=T0, credit_ceiling=1000, **kw):
    client = FirecrawlClient(KEY, transport=httpx.MockTransport(vendor.handler), sleep=SleepRecorder(),
                             concurrency=5, rpm=None)
    return pl.scrape_websites_firecrawl(list(urls), session=db.Session(), client=client, clock=lambda: now,
                                        credit_ceiling=credit_ceiling, **kw)


def requested(vendor) -> list[str]:
    return sorted(r["url"] for r in vendor.requests)


# ---------------------------------------------------------------------------
# A first run
# ---------------------------------------------------------------------------

def test_first_run_fetches_home_and_picks_and_writes_every_table(db):
    vendor = standard_site(FakeFirecrawl())
    frame = scrape(db, vendor)

    # Picks come from the vendor's links: allow-listed, same site, the rest left alone.
    assert requested(vendor) == [HOME, f"{HOME}/about", f"{HOME}/mission", f"{HOME}/team"]

    home = db.page(KEY_HOME)
    assert (home.subpath, home.page_type, home.response_status_code) == ("/", "index", 200)
    assert (home.text_quality, home.extractor, home.num_errors) == ("ok", mc.EXTRACTOR, 0)
    assert (home.last_attempt_result, home.last_attempt_at, home.fetched_at) == ("ok", T0, T0)
    assert (home.final_url, home.final_host, home.source_url) == (HOME + "/", "example.org", HOME)
    assert home.markdown == md("home") and "home sentence 3" in home.parsed_html
    about = db.page(f"{KEY_HOME}/about")
    assert (about.subpath, about.page_type, about.text_quality) == ("about", "page", "ok")

    org = db.org()
    assert org.text_quality == "ok" and org.num_errors == 0 and org.combined_at == T0
    assert org.extractor == mc.EXTRACTOR and org.prompt_token_budget == mc.ORG_TOKEN_CAP
    assert org.source_keys[0] == KEY_HOME
    assert sorted(org.source_keys[1:]) == [f"{KEY_HOME}/about", f"{KEY_HOME}/mission", f"{KEY_HOME}/team"]
    assert org.combined_text.startswith(f"URL: {HOME}/\nTEXT: ")
    for topic in ("home", "about", "team", "mission"):
        assert f"{topic} sentence 3" in org.combined_text

    (run_row,) = db.all(ScrapeRun)
    assert run_row.run_id == home.run_id == org.run_id
    assert run_row.credits_used == 4 and run_row.credit_ceiling == 1000 and run_row.stop_reason is None
    assert run_row.request_profile["onlyMainContent"] is False
    assert (run_row.started_at, run_row.extractor_version, run_row.credits_before) == (T0, mc.EXTRACTOR, 1000)

    # The frame scrape_websites_psql(return_combined_res=True) returns.
    assert len(frame) == 1
    assert frame.loc[0, "home_url"] == HOME and frame.loc[0, "combined_text"] == org.combined_text
    assert {"cleaned_home_key", "num_errors", "text_quality", "last_attempt_result", "failure_reason"} <= set(frame.columns)


def test_combined_text_is_what_combine_org_gives_for_the_stored_rows(db):
    scrape(db, standard_site(FakeFirecrawl()))
    org = db.org()
    rows = [db.page(k) for k in org.source_keys]
    expected = mc.combine_org(HOME, [mc.StoredPage(r.subpath, r.page_type, r.response_status_code, r.markdown)
                                     for r in rows]).org
    assert org.combined_text.encode() == expected.text.encode()


def test_second_run_is_served_from_the_cache_without_a_request_or_a_run_row(db):
    scrape(db, standard_site(FakeFirecrawl()))
    again = FakeFirecrawl()
    frame = scrape(db, again, now=T0 + dt.timedelta(days=400))
    assert again.requests == [] and again.balance_calls == 0
    assert len(db.all(ScrapeRun)) == 1
    assert frame.loc[0, "combined_text"] == db.org().combined_text


def test_picks_are_built_on_the_host_the_home_page_was_served_from(db):
    new = "https://www.newname.org"
    vendor = FakeFirecrawl()
    vendor.script(HOME, page(markdown=md("home"), url=new + "/", source=HOME,
                             links=(f"{new}/about", f"{HOME}/team")))
    vendor.script(f"{new}/about", page(markdown=md("about"), url=f"{new}/about", links=()))
    scrape(db, vendor)
    assert requested(vendor) == [HOME, f"{new}/about"]
    about = db.page(f"{KEY_HOME}/about")          # keyed under the home we asked for
    assert about.home_url == HOME and about.final_host == "newname.org"
    assert db.page(KEY_HOME).final_host == "newname.org"


def test_single_page_websites_get_no_subpages(db):
    # Most of the built-in list (medium.com, linkedin.com, ...) is platform hosts the
    # client already refuses as invalid_url, so the argument is the easy one to test.
    vendor = standard_site(FakeFirecrawl())
    scrape(db, vendor, single_page_websites=["example.org"])
    assert requested(vendor) == [HOME]
    assert db.org().source_keys == [KEY_HOME]


def test_urls_are_keyed_like_the_in_house_cache_and_blanks_are_dropped(db):
    vendor = standard_site(FakeFirecrawl())
    frame = scrape(db, vendor, urls=["www.example.org", "https://www.example.org/", "", None])
    assert len(frame) == 1 and frame.loc[0, "home_url"] == "https://www.example.org"
    assert [o.cleaned_home_key for o in db.all(WebPagesParsedV2)] == [KEY_HOME]


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------

def test_home_failure_fetches_no_subpages_and_returns_no_text(db):
    vendor = FakeFirecrawl().script(HOME, page(404, markdown="# Not found\n\n" + md("lost"), links=LINKS))
    frame = scrape(db, vendor)
    assert requested(vendor) == [HOME]
    home = db.page(KEY_HOME)
    assert (home.response_status_code, home.failure_reason, home.last_attempt_result) == (404, "http_error", "gone")
    assert (home.num_errors, home.fetched_at, home.parsed_html, home.text_quality) == (1, None, "", None)
    assert home.markdown.startswith("# Not found")        # kept on non-2xx rows too
    org = db.org()
    assert (org.combined_text, org.text_quality, org.num_errors, org.last_attempt_result) == (None, "dead", 1, "gone")
    assert frame.loc[0, "combined_text"] is None and frame.loc[0, "failure_reason"] == "http_error"


def test_vendor_error_leaves_num_errors_at_zero_and_is_retried_next_run(db):
    vendor = FakeFirecrawl().script(HOME, api_error(408, code="SCRAPE_TIMEOUT"))
    frame = scrape(db, vendor, max_errors=1)
    home, org = db.page(KEY_HOME), db.org()
    assert (home.failure_reason, home.num_errors, home.last_attempt_result) == ("vendor_error", 0, "unknown")
    assert (org.num_errors, org.combined_text) == (0, None)
    assert frame.loc[0, "failure_reason"] == "vendor_error" and frame.loc[0, "combined_text"] is None

    # Even under the climate caller's max_errors=1, the next run tries again.
    retry = standard_site(FakeFirecrawl())
    scrape(db, retry, max_errors=1, now=T0 + dt.timedelta(hours=1))
    assert HOME in requested(retry) and db.org().text_quality == "ok"


def test_a_target_failure_under_max_errors_one_is_not_retried_until_the_window_passes(db):
    scrape(db, FakeFirecrawl().script(HOME, page(403, markdown="Forbidden")), max_errors=1)
    assert (db.page(KEY_HOME).failure_reason, db.page(KEY_HOME).last_attempt_result) == ("blocked", "unknown")

    soon = standard_site(FakeFirecrawl())
    scrape(db, soon, max_errors=1, now=T0 + dt.timedelta(days=30))
    assert soon.requests == []

    later = standard_site(FakeFirecrawl())
    scrape(db, later, max_errors=1, now=T0 + dt.timedelta(days=91))
    assert HOME in requested(later)
    assert (db.org().text_quality, db.org().num_errors, db.page(KEY_HOME).num_errors) == ("ok", 0, 0)


def test_a_target_failure_is_retried_each_run_until_max_errors(db):
    blocked = lambda: FakeFirecrawl().script(HOME, page(403, markdown="Forbidden"))  # noqa: E731
    for n in (1, 2, 3):
        vendor = blocked()
        scrape(db, vendor, max_errors=3, now=T0 + dt.timedelta(days=n))
        assert requested(vendor) == [HOME] and db.page(KEY_HOME).num_errors == n
    vendor = blocked()
    scrape(db, vendor, max_errors=3, now=T0 + dt.timedelta(days=4))
    assert vendor.requests == []


def test_an_invalid_url_costs_nothing_and_is_never_retried(db):
    vendor = FakeFirecrawl()
    frame = scrape(db, vendor, urls=["facebook.com/some-org"])
    assert vendor.requests == []
    key = "facebook.com_some-org"
    assert (db.page(key).failure_reason, db.page(key).last_attempt_result) == ("invalid_url", "invalid_url")
    assert frame.loc[0, "combined_text"] is None
    scrape(db, vendor, urls=["facebook.com/some-org"], now=T0 + dt.timedelta(days=400))
    assert vendor.requests == []


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

def test_a_parked_home_page_makes_the_org_parked_and_buys_no_subpages(db):
    vendor = FakeFirecrawl().script(HOME, page(markdown="# example.org\n\nThis domain is for sale. Buy this domain today.",
                                               links=LINKS))
    frame = scrape(db, vendor)
    assert requested(vendor) == [HOME]
    home, org = db.page(KEY_HOME), db.org()
    assert (home.text_quality, home.last_attempt_result, home.num_errors) == ("parked", "gone", 1)
    assert (org.text_quality, org.num_errors) == ("parked", 1)
    assert frame.loc[0, "text_quality"] == "parked" and frame.loc[0, "combined_text"] is None


def test_page_and_org_verdicts_are_stored_and_a_junk_subpage_stays_out_of_the_text(db):
    vendor = site(FakeFirecrawl(), about=md("about"),
                  team="Checking the site connection security. Verify you are human.",
                  mission="Our mission is clean rivers for every town in the basin, and we work on it every day.")
    scrape(db, vendor)
    assert db.page(f"{KEY_HOME}/team").text_quality == "blocked"
    assert db.page(f"{KEY_HOME}/mission").text_quality == "thin"
    org = db.org()
    assert org.text_quality == "ok"
    assert f"{KEY_HOME}/team" not in org.source_keys and f"{KEY_HOME}/mission" in org.source_keys
    assert "Verify you are human" not in org.combined_text


# ---------------------------------------------------------------------------
# Refresh: last known good, subpages follow the home page
# ---------------------------------------------------------------------------

def refresh(db, vendor, days=100, **kw):
    return scrape(db, vendor, now=T0 + dt.timedelta(days=days), max_age_days=90, **kw)


def test_a_blocked_refetch_keeps_the_previous_text_and_records_the_attempt(db):
    scrape(db, standard_site(FakeFirecrawl()))
    before = db.org().combined_text

    vendor = FakeFirecrawl().script(HOME, page(403, markdown="Access denied"))
    frame = refresh(db, vendor)
    assert requested(vendor) == [HOME]
    home, org = db.page(KEY_HOME), db.org()
    assert (home.markdown, home.response_status_code, home.text_quality, home.fetched_at) == (md("home"), 200, "ok", T0)
    assert (home.last_attempt_result, home.failure_reason, home.attempts_since_fetch) == ("unknown", "blocked", 1)
    assert home.last_attempt_at == T0 + dt.timedelta(days=100) and home.num_errors == 1
    assert (org.combined_text, org.text_quality, org.stale_reason, org.combined_at) == (before, "ok", None, T0)
    assert org.last_attempt_result == "unknown"
    assert frame.loc[0, "combined_text"] == before
    assert db.all(WebPagesHistoryV2) == []

    # Not hammered: nothing is sent again until retry_failed_after_days has passed.
    quiet = FakeFirecrawl()
    refresh(db, quiet, days=150)
    assert quiet.requests == []
    back = standard_site(FakeFirecrawl())
    refresh(db, back, days=191)
    assert HOME in requested(back) and db.page(KEY_HOME).last_attempt_result == "ok"


def test_a_requested_refetch_is_due_at_any_age_and_a_blocked_one_keeps_the_text(db):
    """delete_cached_urls on the Firecrawl tables asks for a re-fetch instead of deleting."""
    from vdl_tools.scrape_enrich.scraper.cache_invalidation import delete_cached_urls

    scrape(db, standard_site(FakeFirecrawl()))
    before = db.org().combined_text
    assert delete_cached_urls([HOME], session=db.Session(), backend="firecrawl")["refetch_marked"] == 1
    assert db.page(KEY_HOME).refetch_requested_at is not None

    # A vendor error does not answer the request: it stands for the next run.
    vendor = FakeFirecrawl().script(HOME, api_error(408, code="SCRAPE_TIMEOUT"))
    scrape(db, vendor, now=T0 + dt.timedelta(days=1))
    assert requested(vendor) == [HOME] and db.page(KEY_HOME).refetch_requested_at is not None

    # One day old and no max_age_days, so only the request makes it due. The site blocks us.
    vendor = FakeFirecrawl().script(HOME, page(403, markdown="Access denied"))
    frame = scrape(db, vendor, now=T0 + dt.timedelta(days=2))
    assert requested(vendor) == [HOME]
    home = db.page(KEY_HOME)
    assert (home.markdown, home.fetched_at, home.last_attempt_result) == (md("home"), T0, "unknown")
    assert home.refetch_requested_at is None
    assert db.org().combined_text == before and frame.loc[0, "combined_text"] == before

    quiet = FakeFirecrawl()
    scrape(db, quiet, now=T0 + dt.timedelta(days=3))
    assert quiet.requests == []


def test_a_gone_home_page_keeps_the_text_and_marks_it_stale(db):
    scrape(db, standard_site(FakeFirecrawl()))
    before = db.org().combined_text
    frame = refresh(db, FakeFirecrawl().script(HOME, page(404, markdown="Not found")))
    org = db.org()
    assert (org.combined_text, org.stale_reason, org.last_attempt_result) == (before, "home_gone", "gone")
    assert frame.loc[0, "combined_text"] == before      # the same text, so the summary cache still hits


def test_vanished_and_404_subpages_leave_the_text_and_a_blocked_one_stays(db):
    scrape(db, standard_site(FakeFirecrawl()))

    # Now: /mission is no longer linked, /about answers 404, /team is blocked.
    vendor = site(FakeFirecrawl(), links=(f"{HOME}/about", f"{HOME}/team"),
                  about=page(404, markdown="Not found"), team=page(403, markdown="Access denied"))
    refresh(db, vendor)
    assert requested(vendor) == [HOME, f"{HOME}/about", f"{HOME}/team"]

    org = db.org()
    assert org.source_keys == [KEY_HOME, f"{KEY_HOME}/team"]
    assert "team sentence 3" in org.combined_text
    assert "about sentence 3" not in org.combined_text and "mission sentence 3" not in org.combined_text
    assert org.text_quality == "ok" and org.stale_reason is None

    about, team, mission = (db.page(f"{KEY_HOME}/{p}") for p in ("about", "team", "mission"))
    assert (about.last_attempt_result, about.markdown) == ("gone", md("about"))       # last good text kept
    assert (team.last_attempt_result, team.markdown, team.fetched_at) == ("unknown", md("team"), T0)
    assert mission.superseded_at == T0 + dt.timedelta(days=100)
    assert about.superseded_at is None and team.superseded_at is None


def test_max_age_days_refetches_only_the_older_rows(db):
    old, young = "https://old.example.org", "https://young.example.org"
    scrape(db, site(FakeFirecrawl(), home=old, links=()), urls=[old], now=T0)
    scrape(db, site(FakeFirecrawl(), home=young, links=()), urls=[young], now=T0 + dt.timedelta(days=60))

    vendor = site(site(FakeFirecrawl(), home=old, links=()), home=young, links=())
    scrape(db, vendor, urls=[old, young], now=T0 + dt.timedelta(days=100), max_age_days=90)
    assert requested(vendor) == [old]
    assert db.page("old.example.org").fetched_at == T0 + dt.timedelta(days=100)
    assert db.page("young.example.org").fetched_at == T0 + dt.timedelta(days=60)


def test_two_runs_over_the_same_pages_give_byte_identical_text_and_no_history(db):
    scrape(db, standard_site(FakeFirecrawl()))
    first = db.org()
    refresh(db, standard_site(FakeFirecrawl()))
    second = db.org()
    assert second.combined_text.encode() == first.combined_text.encode()
    assert second.source_keys == first.source_keys
    assert second.combined_at == T0 + dt.timedelta(days=100) and second.run_id != first.run_id
    assert db.all(WebPagesHistoryV2) == []


def test_a_replaced_text_goes_to_the_history_first_one_version_per_page(db):
    scrape(db, standard_site(FakeFirecrawl()))
    first_run = db.page(KEY_HOME).run_id
    changed = standard_site(FakeFirecrawl())
    changed.script(HOME, page(markdown=md("renewed"), links=LINKS))
    refresh(db, changed)
    (kept,) = db.all(WebPagesHistoryV2)
    assert (kept.cleaned_key, kept.fetched_at, kept.markdown, kept.run_id) == (KEY_HOME, T0, md("home"), first_run)
    assert kept.text_quality == "ok" and "home sentence 3" in kept.parsed_html
    assert "renewed sentence 3" in db.org().combined_text

    again = standard_site(FakeFirecrawl())
    again.script(HOME, page(markdown=md("third"), links=LINKS))
    refresh(db, again, days=200)
    (kept,) = db.all(WebPagesHistoryV2)
    assert kept.markdown == md("renewed") and kept.fetched_at == T0 + dt.timedelta(days=100)


def test_a_successful_recombine_resets_num_errors(db):
    scrape(db, FakeFirecrawl().script(HOME, page(500, markdown="Server error")), max_errors=3)
    assert (db.org().num_errors, db.page(KEY_HOME).num_errors) == (1, 1)
    scrape(db, standard_site(FakeFirecrawl()), max_errors=3, now=T0 + dt.timedelta(days=1))
    assert (db.org().num_errors, db.page(KEY_HOME).num_errors) == (0, 0)


# ---------------------------------------------------------------------------
# Credits, stops, dry run
# ---------------------------------------------------------------------------

def test_a_run_refuses_to_start_without_a_credit_ceiling(db):
    vendor = standard_site(FakeFirecrawl())
    for ceiling in (None, 0):
        with pytest.raises(ValueError, match="credit_ceiling"):
            scrape(db, vendor, credit_ceiling=ceiling)
    assert vendor.requests == [] and db.all(ScrapeRun) == []


def test_dry_run_reports_what_is_due_and_fetches_and_writes_nothing(db):
    scrape(db, standard_site(FakeFirecrawl()))
    vendor = FakeFirecrawl(remaining=500)
    report = scrape(db, vendor, urls=[HOME, "https://new-one.org", "https://new-two.org"], dry_run=True,
                    credit_ceiling=None)
    assert vendor.requests == [] and vendor.balance_calls == 1
    assert (report.n_urls, report.n_cached, report.n_misses) == (3, 1, 2)
    assert report.estimated_credits == pytest.approx(2 * pl.CREDITS_PER_ORG)
    assert report.remaining_credits == 500 and report.within_balance is True
    assert len(db.all(ScrapeRun)) == 1 and len(db.all(WebPagesParsedV2)) == 1


def test_a_402_mid_org_writes_nothing_for_it_and_the_run_row_says_why(db):
    vendor = site(FakeFirecrawl(), about=md("about"), team=api_error(402, error="Insufficient credits"),
                  mission=md("mission"))
    frame = scrape(db, vendor)
    assert db.all(WebPagesScrapedV2) == [] and db.all(WebPagesParsedV2) == []
    (run_row,) = db.all(ScrapeRun)
    assert "Insufficient credits" in run_row.stop_reason and run_row.ended_at == T0
    assert run_row.credits_used >= 1                      # from client.stats, though no row was written
    assert frame.loc[0, "home_url"] == HOME and frame.loc[0, "combined_text"] is None

    # The org is due again on the next run.
    retry = standard_site(FakeFirecrawl())
    scrape(db, retry, now=T0 + dt.timedelta(hours=1))
    assert db.org().text_quality == "ok"


def test_the_credit_ceiling_stops_the_run_and_finished_orgs_are_kept(db):
    homes = [f"https://org{i}.example.org" for i in range(6)]
    vendor = FakeFirecrawl()
    for home in homes:
        site(vendor, home=home, links=())
    client = FirecrawlClient(KEY, transport=httpx.MockTransport(vendor.handler), sleep=SleepRecorder(),
                             concurrency=1, rpm=None)
    frame = pl.scrape_websites_firecrawl(homes, session=db.Session(), client=client, clock=lambda: T0,
                                         credit_ceiling=3)
    assert len(vendor.requests) == 3 and len(db.all(WebPagesParsedV2)) == 3
    (run_row,) = db.all(ScrapeRun)
    assert "credit ceiling" in run_row.stop_reason and run_row.credits_used == 3
    assert frame["combined_text"].notnull().sum() == 3 and len(frame) == 6


def test_one_org_failing_unexpectedly_does_not_end_the_run(db, monkeypatch):
    other = "https://other.example.org"
    vendor = site(standard_site(FakeFirecrawl()), home=other, links=())
    real = pl.pick_subpages

    def picky(home, max_per_subpath=pl.MAX_PER_SUBPATH):
        if home.url == HOME:
            raise RuntimeError("boom")
        return real(home, max_per_subpath)

    monkeypatch.setattr(pl, "pick_subpages", picky)
    frame = scrape(db, vendor, urls=[HOME, other])
    assert db.org() is None and db.org("other.example.org").text_quality == "ok"
    assert frame["combined_text"].notnull().tolist() == [False, True]


# ---------------------------------------------------------------------------
# org_site_keys
# ---------------------------------------------------------------------------

def test_org_ids_are_linked_to_the_site_fetched_for_them(db):
    ids = {HOME: [("crunchbase", "8a1b"), ("ein", "941234567")], "https://never-scraped.org": [("nzi", "77")]}
    scrape(db, standard_site(FakeFirecrawl()), org_ids=ids)
    rows = {(r.id_type, r.id_value): r for r in db.all(OrgSiteKey)}
    assert set(rows) == {("crunchbase", "8a1b"), ("ein", "941234567")}
    link = rows[("ein", "941234567")]
    assert (link.cleaned_home_key, link.final_host, link.source_url) == (KEY_HOME, "example.org", HOME)
    assert link.first_seen == link.last_seen == T0 and link.run_id == db.org().run_id

    # Served from the cache later: last_seen moves, first_seen and the run that fetched it stay.
    scrape(db, FakeFirecrawl(), org_ids={HOME: [("ein", "941234567")]}, now=T0 + dt.timedelta(days=5))
    link = {(r.id_type, r.id_value): r for r in db.all(OrgSiteKey)}[("ein", "941234567")]
    assert (link.first_seen, link.last_seen) == (T0, T0 + dt.timedelta(days=5))
    assert link.run_id == db.org().run_id


# ---------------------------------------------------------------------------
# is_due
# ---------------------------------------------------------------------------

def _row(**kw):
    defaults = dict(last_attempt_result="ok", fetched_at=T0, last_attempt_at=T0, failure_reason=None,
                    text_quality="ok", num_errors=0)
    return WebPagesScrapedV2(**{**defaults, **kw})


@pytest.mark.parametrize("row,days,max_age,expected", [
    (None, 0, None, True),
    (_row(), 400, None, False),                                              # good, no age limit
    (_row(), 80, 90, False),
    (_row(refetch_requested_at=T0), 0, None, True),                          # a re-fetch was asked for
    (_row(), 91, 90, True),
    (_row(last_attempt_result="unknown", failure_reason="vendor_error"), 0, None, True),
    (_row(last_attempt_result="invalid_url", failure_reason="invalid_url", fetched_at=None, text_quality=None,
          num_errors=1), 400, 0, False),
    (_row(last_attempt_result="gone", failure_reason="dead", fetched_at=None, text_quality=None, num_errors=1),
     1, None, True),                                                         # no text, under max_errors
    (_row(last_attempt_result="gone", failure_reason="dead", fetched_at=None, text_quality=None, num_errors=3),
     1, None, False),                                                        # gave up, inside the window
    (_row(last_attempt_result="gone", failure_reason="dead", fetched_at=None, text_quality=None, num_errors=3),
     91, None, True),                                                        # the window passed
    (_row(last_attempt_result="unknown", failure_reason="blocked", num_errors=1), 30, 0, False),   # has good text
    (_row(last_attempt_result="unknown", failure_reason="blocked", num_errors=1), 91, 0, True),
])
def test_is_due(row, days, max_age, expected):
    now = T0 + dt.timedelta(days=days)
    assert pl.is_due(row, now, max_age, retry_failed_after_days=90, max_errors=3) is expected


def test_sync_wrapper_and_async_form_agree(db):
    vendor = standard_site(FakeFirecrawl())
    client = FirecrawlClient(KEY, transport=httpx.MockTransport(vendor.handler), sleep=SleepRecorder(), rpm=None)
    frame = run(pl.scrape_websites_firecrawl_async([HOME], session=db.Session(), client=client,
                                                   clock=lambda: T0, credit_ceiling=10))
    assert frame.loc[0, "text_quality"] == "ok"
