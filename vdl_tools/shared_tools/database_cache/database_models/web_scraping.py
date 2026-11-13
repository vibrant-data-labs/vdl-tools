from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy_utils import generic_repr

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class WebPagesParsed(BaseMixin):
    """Table to hold scraped webpages"""
    __tablename__ = 'web_pages_parsed'

    cleaned_home_key = Column(String, nullable=False, primary_key=True, index=True)
    home_url = Column(String, nullable=False, index=True)
    num_errors = Column(Integer, nullable=True)
    combined_text = Column(String, nullable=True)

@generic_repr
class WebPagesScraped(BaseMixin):
    """Table to hold scraped webpages"""
    __tablename__ = 'web_pages_scraped'

    cleaned_key = Column(String, nullable=False, primary_key=True, index=True)
    # parent_key = Column(String, ForeignKey('web_pages_parsed.cleaned_home_key'), nullable=False)
    full_path = Column(String, index=True)
    home_url = Column(String, nullable=False, index=True)
    subpath = Column(String, nullable=False)
    raw_html = Column(String, nullable=True)
    parsed_html = Column(String, nullable=True)
    page_type = Column(String, nullable=False, index=True)
    response_status_code = Column(Integer, nullable=True)
    num_errors = Column(Integer, nullable=True)



# ---------------------------------------------------------------------------
# Firecrawl adapter tables (docs/FIRECRAWL_ADAPTER_PLAN.md section 4)
#
# New tables rather than new columns on the 17 GB web_pages_scraped. Keys are
# the same as the old pair, so make_group_text and the cache lookups read them
# unchanged. All timestamps are naive UTC, like date_added on BaseMixin.
# run_id is lineage and is not an enforced foreign key. scrape_runs rows are
# never deleted, so a run_id always resolves. The counters (num_errors,
# attempts_since_fetch) are NOT NULL with a default of 0: NULL + 1 is NULL in
# SQL, so a nullable counter never reaches max_errors and a dead site is
# refetched, and billed, on every run.
# ---------------------------------------------------------------------------

_JSON = JSON().with_variant(JSONB, "postgresql")


@generic_repr
class ScrapeRun(BaseMixin):
    """One run of the Firecrawl pipeline: what was asked for and what it cost."""
    __tablename__ = 'scrape_runs'

    run_id = Column(String, nullable=False, primary_key=True)
    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    git_sha = Column(String, nullable=True)
    extractor_version = Column(String, nullable=True)
    request_profile = Column(_JSON, nullable=True)    # the request settings sent with every page
    plan_concurrency = Column(Integer, nullable=True)
    plan_rpm = Column(Integer, nullable=True)
    credit_ceiling = Column(Float, nullable=True)
    credits_before = Column(Float, nullable=True)
    credits_after = Column(Float, nullable=True)
    credits_used = Column(Float, nullable=True)
    stop_reason = Column(String, nullable=True)


@generic_repr
class WebPagesScrapedV2(BaseMixin):
    """One row per page. The text columns hold the last fetch that passed the
    status gate and the text gate; the attempt columns hold the newest attempt,
    whatever it returned."""
    __tablename__ = 'web_pages_scraped_v2'

    cleaned_key = Column(String, nullable=False, primary_key=True)
    home_url = Column(String, nullable=False, index=True)
    subpath = Column(String, nullable=False)
    page_type = Column(String, nullable=False)
    run_id = Column(String, nullable=True, index=True)
    source_url = Column(String, nullable=True)           # metadata.sourceURL: what Firecrawl was asked for
    final_url = Column(String, nullable=True)            # metadata.url: where the page was served from
    final_host = Column(String, nullable=True, index=True)
    response_status_code = Column(Integer, nullable=True)   # the target's own status; name kept for the readers
    failure_reason = Column(String, nullable=True)       # dead, http_error, blocked, invalid_url, vendor_error
    markdown = Column(String, nullable=True)             # as returned; kept on non-2xx rows too
    parsed_html = Column(String, nullable=True)          # cleaned text; '' on non-2xx rows
    extractor = Column(String, nullable=True)            # firecrawl-md-v<CLEANER_VERSION>
    text_quality = Column(String, nullable=True)
    proxy_used = Column(String, nullable=True)
    credits_used = Column(Float, nullable=True)
    content_type = Column(String, nullable=True)
    language = Column(String, nullable=True)
    scrape_id = Column(String, nullable=True)
    num_errors = Column(Integer, nullable=False, default=0, server_default='0')   # target failures only; a vendor error leaves it alone
    fetched_at = Column(DateTime, nullable=True, index=True)        # date of the stored text
    last_attempt_at = Column(DateTime, nullable=True)
    last_attempt_result = Column(String, nullable=True)  # ok, gone, unknown, invalid_url
    attempts_since_fetch = Column(Integer, nullable=False, default=0, server_default='0')
    superseded_at = Column(DateTime, nullable=True)      # a newer home fetch no longer picks this page


@generic_repr
class WebPagesParsedV2(BaseMixin):
    """One row per org: the combined text and the page rows it was built from."""
    __tablename__ = 'web_pages_parsed_v2'

    cleaned_home_key = Column(String, nullable=False, primary_key=True)
    home_url = Column(String, nullable=False, index=True)
    run_id = Column(String, nullable=True, index=True)
    combined_text = Column(String, nullable=True)
    source_keys = Column(_JSON, nullable=True)           # cleaned_key of each page in combined_text, in order
    extractor = Column(String, nullable=True)
    prompt_token_budget = Column(Integer, nullable=True)
    text_quality = Column(String, nullable=True)
    num_errors = Column(Integer, nullable=False, default=0, server_default='0')
    combined_at = Column(DateTime, nullable=True)
    last_attempt_at = Column(DateTime, nullable=True)
    last_attempt_result = Column(String, nullable=True)
    stale_reason = Column(String, nullable=True)         # home_gone: the text is the last known good


@generic_repr
class WebPagesHistoryV2(BaseMixin):
    """The previous good version of a page, written only when a newer good
    fetch replaces its text."""
    __tablename__ = 'web_pages_history_v2'

    cleaned_key = Column(String, nullable=False, primary_key=True)
    fetched_at = Column(DateTime, nullable=False, primary_key=True)
    run_id = Column(String, nullable=True)
    extractor = Column(String, nullable=True)
    markdown = Column(String, nullable=True)
    parsed_html = Column(String, nullable=True)
    text_quality = Column(String, nullable=True)


@generic_repr
class OrgSiteKey(BaseMixin):
    """The link from a source org id to the site fetched for it. One id may map
    to several home keys over time and one home key to several ids."""
    __tablename__ = 'org_site_keys'

    id_type = Column(String, nullable=False, primary_key=True)    # crunchbase, ein, nzi, givingtuesday, linkedin, coresignal
    id_value = Column(String, nullable=False, primary_key=True)   # normalized: EIN digits only, uuid lowercase
    cleaned_home_key = Column(String, nullable=False, primary_key=True, index=True)
    source_url = Column(String, nullable=True)
    final_host = Column(String, nullable=True)
    run_id = Column(String, nullable=True)
    first_seen = Column(DateTime, nullable=True)
    last_seen = Column(DateTime, nullable=True)
