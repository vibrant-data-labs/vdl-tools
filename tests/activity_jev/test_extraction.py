import pandas as pd
import pytest

from vdl_tools.shared_tools.activity_extraction.pipeline import extract_frame

from .conftest import COLUMNS, profile


def test_all_orgs_accounted_and_minor_activity_kept(extraction):
    assert extraction.organizations.extraction_status.tolist() == [
        "success",
        "missing_text",
        "request_failed",
        "empty",
    ]
    assert extraction.activities.prominence.tolist() == ["core", "minor"]
    assert extraction.lineage["n_input_orgs"] == 4
    assert extraction.lineage["status_counts"] == {
        "success": 1,
        "missing_text": 1,
        "request_failed": 1,
        "empty": 1,
    }
    assert not extraction.activities.source_sha256.isna().any()
    assert "state" not in extraction.activities
    assert "cache_id" not in extraction.activities


def test_run_identity_changes_but_source_hash_does_not(organizations):
    texts = {"1": "URL: https://solar.example/\nTEXT: We install rooftop solar panels with our own trained crews."}
    first = extract_frame(
        organizations.iloc[:1], run_id="first", **COLUMNS, website_texts=texts, runner=lambda *a, **k: {"1": profile()}
    )
    second = extract_frame(
        organizations.iloc[:1], run_id="second", **COLUMNS, website_texts=texts, runner=lambda *a, **k: {"1": profile()}
    )
    assert set(first.activities.activity_id).isdisjoint(second.activities.activity_id)
    assert first.activities.source_sha256.tolist() == second.activities.source_sha256.tolist()


def test_missing_text_never_falls_back_to_descriptions(organizations):
    source = organizations.assign(
        Description="Solar panels", Description_990="Solar panels", Summary="Solar panels"
    )
    sent = []
    result = extract_frame(
        source, run_id="missing", **COLUMNS, website_texts={}, runner=lambda inputs, *a, **kw: sent.append(inputs) or {}
    )
    assert sent == [[]]
    assert set(result.organizations.extraction_status) == {"missing_text"}
    assert result.activities.empty


def test_duplicate_normalized_ids_rejected(organizations):
    source = pd.concat([organizations.iloc[:1], organizations.iloc[:1].assign(id=1)])
    with pytest.raises(ValueError, match="unique"):
        extract_frame(source, run_id="duplicate", **COLUMNS, website_texts={})


def test_caller_names_the_organization_columns(organizations, monkeypatch):
    from vdl_tools.shared_tools.activity_extraction import pipeline

    renamed = organizations.rename(columns={"id": "org_key", "Organization": "org_title", "Website": "homepage"})
    read = []

    def by_url(urls):
        read.append(urls)
        return {"https://solar.example": "URL: https://solar.example/\nTEXT: We install rooftop solar panels."}

    monkeypatch.setattr(pipeline, "load_website_texts", by_url)
    sent = []
    result = extract_frame(
        renamed, run_id="renamed", id_col="org_key", name_col="org_title", url_col="homepage",
        runner=lambda pairs, *a, **kw: sent.extend(pairs) or {"1": profile()},
    )
    assert read == [["https://solar.example", "https://failed.example", "https://empty.example"]]
    assert sent == [("1", "ORGANIZATION: Solar\n\nURL: https://solar.example/\nTEXT: We install rooftop solar panels.")]
    assert result.organizations[["uid", "name", "url"]].iloc[0].tolist() == ["1", "Solar", "https://solar.example"]
    assert result.organizations.extraction_status.tolist() == ["success", "missing_text", "missing_text", "missing_text"]
    assert result.activities.name.unique().tolist() == ["Solar"]
    with pytest.raises(ValueError, match="url_col"):
        extract_frame(renamed, run_id="no-url", id_col="org_key", name_col="org_title")
    with pytest.raises(ValueError, match="Missing organization columns"):
        extract_frame(renamed, run_id="wrong", id_col="id", name_col="org_title", website_texts={})
    # Supplied text needs no URL column at all.
    supplied = extract_frame(
        renamed.drop(columns="homepage"), run_id="supplied", id_col="org_key", name_col="org_title",
        website_texts={"1": "We install rooftop solar panels."}, runner=lambda *a, **kw: {"1": profile()},
    )
    assert supplied.organizations.url.isna().all()
    assert supplied.organizations.extraction_status.iloc[0] == "success"


def test_run_extraction_prompt_uses_shared_instructor_cache(monkeypatch):
    from contextlib import nullcontext
    from unittest.mock import MagicMock

    from vdl_tools.shared_tools.activity_extraction.extraction import run_extraction_prompt
    from vdl_tools.shared_tools.activity_extraction.general import GENERAL_EXTRACTION_PROMPT
    from vdl_tools.shared_tools.database_cache import database_utils
    from vdl_tools.shared_tools.openai import prompt_response_cache_instructor as instructor

    captured = {}

    class FakeCache:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def bulk_get_parsed(self, pairs, **kwargs):
            captured["pairs"], captured["bulk"] = pairs, kwargs
            return {"1": "profile"}

    monkeypatch.setattr(database_utils, "get_session", lambda **kw: nullcontext(MagicMock()))
    monkeypatch.setattr(instructor, "InstructorPRC", FakeCache)
    prompt = GENERAL_EXTRACTION_PROMPT
    assert run_extraction_prompt([("1", "text")], prompt, max_workers=3) == {"1": "profile"}
    assert captured["init"]["filter_by_model"] is True
    assert captured["init"]["prompt_str"] == prompt.text
    assert captured["init"]["response_model"] is prompt.response_model
    assert captured["init"]["model"] == prompt.model
    assert captured["bulk"].get("reasoning") == prompt.reasoning
    assert captured["bulk"]["max_workers"] == 3
    assert "text_format" not in captured["bulk"]
    assert run_extraction_prompt([], prompt) == {}


def test_load_website_texts_uses_shared_scraper_and_junk_verdicts(monkeypatch):
    import pandas as pd

    from vdl_tools.scrape_enrich.scraper import scrape_websites
    from vdl_tools.shared_tools.activity_extraction.extraction import load_website_texts

    calls = []

    def fake_scrape(urls, **kwargs):
        calls.append(list(urls))
        return pd.DataFrame(
            [
                {"cleaned_home_key": "solar.example", "home_url": "https://solar.example", "combined_text": "URL: https://solar.example/\nTEXT: We install rooftop solar panels for homes and train installers.", "num_errors": 0},
                {"cleaned_home_key": "parked.example", "home_url": "https://parked.example", "combined_text": "This domain is for sale. Buy this domain today.", "num_errors": 0},
                {"cleaned_home_key": "dead.example", "home_url": "https://dead.example", "combined_text": None, "num_errors": 2},
                {"cleaned_home_key": "moved.example", "home_url": "https://moved.example", "combined_text": "URL: https://moved.example/\nTEXT: The page you are looking for might have been removed, had its name changed, or is temporarily unavailable.", "num_errors": 0},
            ]
        )

    monkeypatch.setattr(scrape_websites, "scrape_websites_psql", fake_scrape)
    texts = load_website_texts(["https://solar.example", "https://parked.example", "https://dead.example", "https://moved.example", "", "https://solar.example"])
    assert calls == [["https://solar.example", "https://parked.example", "https://dead.example", "https://moved.example"]]
    assert list(texts) == ["https://solar.example"]
    assert texts["https://solar.example"].startswith("URL: https://solar.example/")
    assert load_website_texts([]) == {}


def test_extraction_accepts_a_schema_without_jev_fields(organizations):
    from pydantic import BaseModel

    from vdl_tools.shared_tools.activity_extraction.extraction import ExtractionPrompt

    class Activity(BaseModel):
        activity: str
        evidence: str
        certainty: str

    class Profile(BaseModel):
        summary: str
        coverage: str
        activities: list[Activity]

    prompt = ExtractionPrompt(text="Extract activities", prompt_name="minimal", response_model=Profile, model="offline")
    profile = Profile(summary="Teaches", coverage="adequate", activities=[Activity(activity="Teaching", evidence="We teach", certainty="high")])
    result = extract_frame(
        organizations.iloc[:1], run_id="independent", prompt=prompt, **COLUMNS,
        website_texts={"1": "We teach"},
        runner=lambda *a, **kw: {"1": profile},
    )
    assert result.activities.activity.tolist() == ["Teaching"]
    assert "state" not in result.activities and "cache_id" not in result.activities
