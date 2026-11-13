"""get_scraped_df's LinkedIn fallback, with the scrapers and the database stubbed out."""

from contextlib import contextmanager

import pandas as pd
import pytest

from vdl_tools.shared_tools.climate_landscape import enrichment_pipeline as ep


@contextmanager
def _no_session(*_args, **_kwargs):
    yield None


@pytest.fixture
def stubs(monkeypatch):
    """Websites scraped per call, and what Coresignal says each LinkedIn page's website is."""
    calls = {"web": [], "coresignal_websites": {}}

    def scrape_websites(urls, **_kwargs):
        urls = list(urls)
        calls["web"].append(urls)
        return pd.DataFrame({
            "home_url": urls,
            "combined_text": [None if "dead" in u else f"text of {u}" for u in urls],
        })

    def scrape_linkedin(linkedin_urls, **_kwargs):
        return pd.DataFrame([{"original_id": u, "website": calls["coresignal_websites"].get(u)}
                             for u in linkedin_urls])

    monkeypatch.setattr(ep, "get_session", _no_session)
    monkeypatch.setattr(ep, "scrape_websites_psql", scrape_websites)
    monkeypatch.setattr(ep.li, "scrape_organizations_psql", scrape_linkedin)
    monkeypatch.setattr(ep, "GLOBAL_CONFIG", {"linkedin": {"coresignal_api_key": "test"}})
    return calls


def _orgs():
    return pd.DataFrame({
        "Website_cb_cd": ["https://dead.example.org", "https://alive.example.org"],
        "LinkedIn": ["https://www.linkedin.com/company/dead-org", None],
    })


def test_fallback_that_finds_no_website_returns_the_first_scrape(stubs):
    """Coresignal has no website for the failed org. This raised NameError before."""
    df = _orgs()
    df["Website"] = None      # nothing to fall back on but what Coresignal returns
    success, _ = ep.get_scraped_df(df)
    assert success["home_url"].tolist() == ["https://alive.example.org"]
    assert len(stubs["web"]) == 1          # no second scrape with nothing to scrape


def test_fallback_scrapes_the_website_coresignal_returns(stubs):
    stubs["coresignal_websites"]["https://www.linkedin.com/company/dead-org"] = "https://found.example.org"
    success, df = ep.get_scraped_df(_orgs())
    assert sorted(success["home_url"]) == ["https://alive.example.org", "https://found.example.org"]
    assert stubs["web"][1] == ["https://found.example.org"]
    assert df.loc[0, "Website"] == "https://found.example.org"
