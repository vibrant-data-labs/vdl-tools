import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import sql_request


def test_malformed_old_cache_rows_are_refreshed(offline_config, monkeypatch):
    from vdl_tools.shared_tools.database_cache import database_utils
    from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt
    from vdl_tools.shared_tools.openai.prompt_response_cache_jev import JevPromptResponseCacheSQL
    from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

    monkeypatch.setattr(database_utils, "get_session", lambda **kw: nullcontext(MagicMock()))
    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **kw: Prompt(prompt_str=kw["prompt_str"]),
    )
    calls = []

    def bulk(self, pairs, **kwargs):
        calls.append(kwargs)
        return {"a": {"response_text": json.dumps({} if len(calls) == 1 else {"n": {"noul": 0.9}})}}

    monkeypatch.setattr(JevPromptResponseCacheSQL, "bulk_get_cache_or_run", bulk)
    result = sql_request(
        {"n": {"type": "noul", "instructions": "Solar?"}}, [("a", "solar")], prompt_name="fixture"
    )
    assert json.loads(result["a"]["response_text"])["n"]["noul"] == 0.9
    assert [call["read_from_cache"] for call in calls] == [True, False]


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
        with pytest.raises(ValueError, match="Missing or unexpected"):
            self.get_completion("prompt", "state", return_all=True)
        return {}

    monkeypatch.setattr(JevPromptResponseCacheSQL, "bulk_get_cache_or_run", bulk)
    assert (
        sql_request(
            {"n": {"type": "noul", "instructions": "Solar?"}},
            [("a", "solar")],
            prompt_name="fixture",
        )
        == {}
    )
