import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import jev_gateway_request


def test_incomplete_gateway_answers_are_not_cache_successes(offline_config, monkeypatch):
    from vdl_tools.shared_tools.database_cache import database_utils
    from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt
    from vdl_tools.shared_tools.openai.prompt_response_cache_jev import (
        JevPromptResponseCacheSQL,
        _JevResponse,
    )
    from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

    monkeypatch.setattr(database_utils, "get_session", lambda **kw: nullcontext(MagicMock()))
    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **kw: Prompt(prompt_str=kw["prompt_str"]),
    )
    monkeypatch.setattr(
        JevPromptResponseCacheSQL, "get_completion", lambda *a, **k: _JevResponse({"answers": {}})
    )

    def bulk(self, pairs, **kwargs):
        assert self.model_provider == "typesafe-ai"
        assert self._request_kwargs(kwargs)["extra_body"]["providerOptions"]["gateway"] == {
            "order": ["typesafe-ai"], "only": ["typesafe-ai"],
        }
        with pytest.raises(ValueError, match="Missing or unexpected"):
            self.get_completion("prompt", "state", return_all=True)
        return {}

    monkeypatch.setattr(JevPromptResponseCacheSQL, "bulk_get_cache_or_run", bulk)
    assert (
        jev_gateway_request(
            {"n": {"type": "noul", "instructions": "Solar?"}},
            [("a", "solar")],
            prompt_name="fixture",
        )
        == {}
    )


def test_bulk_get_parsed_validates_rows_and_raises_on_corrupt_cache(offline_config, monkeypatch):
    from pydantic import BaseModel, ValidationError

    from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt
    from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
    from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

    class Answer(BaseModel):
        capital: str

    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **kw: Prompt(prompt_str=kw["prompt_str"]),
    )
    rows = {"fr": {"response_text": json.dumps({"capital": "Paris"})}}
    monkeypatch.setattr(InstructorPRC, "bulk_get_cache_or_run", lambda self, pairs, **kw: rows)
    cache = InstructorPRC(session=MagicMock(), prompt_str="Capital?", response_model=Answer)
    # A failed request ("de") is absent from the result, never invented.
    assert cache.bulk_get_parsed([("fr", "France"), ("de", "Germany")]) == {
        "fr": Answer(capital="Paris")
    }
    rows["de"] = {"response_text": json.dumps({"city": "Berlin"})}
    with pytest.raises(ValidationError):
        cache.bulk_get_parsed([("de", "Germany")])
