import pandas as pd
import pytest

from vdl_tools.shared_tools.activity_extraction.extraction import cap_pages
from vdl_tools.shared_tools.activity_extraction.pipeline import extract_frame
from vdl_tools.shared_tools.activity_extraction.scraped_text import SiteText

from .conftest import profile


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
    assert "Evidence:" not in extraction.activities.state.iloc[0]
    assert "Prominence:" not in extraction.activities.state.iloc[0]


def test_run_identity_changes_but_request_cache_identity_does_not(organizations):
    url = organizations.Website.iloc[0]
    sites = {
        url: SiteText(
            url,
            "solar.example",
            [
                {
                    "source": url,
                    "subpath": "/",
                    "text": "We install rooftop solar panels with our own trained crews across the state.",
                }
            ],
            "fixture",
        )
    }
    first = extract_frame(
        organizations.iloc[:1],
        run_id="first",
        site_texts=sites,
        runner=lambda *a, **k: {"1": profile()},
    )
    second = extract_frame(
        organizations.iloc[:1],
        run_id="second",
        site_texts=sites,
        runner=lambda *a, **k: {"1": profile()},
    )
    assert set(first.activities.activity_id).isdisjoint(second.activities.activity_id)
    assert first.activities.cache_id.tolist() == second.activities.cache_id.tolist()
    assert first.activities.source_sha256.tolist() == second.activities.source_sha256.tolist()


def test_missing_text_never_falls_back_to_990_or_summary(organizations):
    source = organizations.assign(
        Description="Solar panels", Description_990="Solar panels", Summary="Solar panels"
    )
    result = extract_frame(
        source,
        run_id="missing",
        site_texts={},
        runner=lambda *a, **kw: pytest.fail("Unexpected model call"),
    )
    assert set(result.organizations.extraction_status) == {"missing_text"}
    assert result.activities.empty


def test_duplicate_normalized_ids_rejected(organizations):
    source = pd.concat([organizations.iloc[:1], organizations.iloc[:1].assign(id="1")])
    with pytest.raises(ValueError, match="unique"):
        extract_frame(source, run_id="duplicate", site_texts={})


def test_empty_frame_and_empty_page_cap(organizations):
    result = extract_frame(organizations.iloc[:0], run_id="empty", site_texts={})
    assert result.activities.empty and result.organizations.empty
    assert "state" in result.activities and "extraction_status" in result.organizations
    assert cap_pages([]) == []


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
