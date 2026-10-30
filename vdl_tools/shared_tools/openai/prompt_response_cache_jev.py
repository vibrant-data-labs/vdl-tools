"""SQL-cached Jev decisions through Vercel AI Gateway's TypeSafe API.

Jev evaluates shared state against typed questions. It does not implement the
Responses API used by the general-purpose ``PromptResponseCacheSQL`` client.
This adapter retains that client's SQL lookup and bulk persistence while
sending Jev's native request shape to the gateway.
"""

import json
import logging
import time
from collections.abc import Mapping
from threading import Event, Lock
from typing import Any

import requests

from vdl_tools.shared_tools.openai.openai_api_utils import get_vercel_client
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    BulkRequestAborted,
    PromptResponseCacheSQL,
)

JEV_GATEWAY_URL = "https://ai-gateway.vercel.sh/typesafe/v1/systemone"
JEV_GATEWAY_MODEL = "typesafe-ai/jev"
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
logger = logging.getLogger(__name__)


class _JevResponse:
    """Expose the response attributes expected by the existing row builder."""

    def __init__(self, payload: dict[str, Any]):
        answers = payload.get("answers")
        if not isinstance(answers, dict):
            raise ValueError("Jev response is missing an answers object")
        self.payload = payload
        self.output_text = json.dumps(answers)

    def model_dump_json(self) -> str:
        return json.dumps(self.payload)


class JevPromptResponseCacheSQL(PromptResponseCacheSQL):
    """Cache Jev's typed answers by question definition, state, and model.

    ``questions`` uses TypeSafe's named question format, for example::

        {"group": {"type": "choice", "instructions": "Which group?",
                   "criteria": {"01": "Agriculture", "02": "Forestry"}}}

    Pass the document or other shared state as ``text`` to
    ``get_cache_or_run`` / ``bulk_get_cache_or_run``.

    A cache instance stops new calls and retries after ``max_server_errors`` HTTP
    5xx responses (default 10, including retry attempts). In-flight calls finish;
    completed responses are cached before the bulk call raises ``BulkRequestAborted``.

    The cache stores the full gateway response in ``response_full`` and JSON-encoded answers in
    ``response_text``. Changing the questions changes the prompt ID.
    """

    def __init__(
        self,
        session,
        questions: Mapping[str, Mapping[str, Any]],
        *,
        prompt_name: str = "Jev decision",
        prompt_description: str = "",
        store_results: bool = True,
        timeout: float = 120.0,
        max_retries: int = 2,
        max_server_errors: int = 10,
    ):
        if not questions:
            raise ValueError("Jev requires at least one question")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        if max_server_errors <= 0:
            raise ValueError("max_server_errors must be positive")
        # Canonical JSON makes question/criteria changes part of the prompt
        # identity without introducing a second cache-key mechanism.
        prompt_str = json.dumps(questions, sort_keys=True, separators=(",", ":"))
        self.questions = json.loads(prompt_str)
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_server_errors = max_server_errors
        self._server_errors = 0
        self._server_error_lock = Lock()
        self._circuit_open = Event()
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            prompt_description=prompt_description,
            filter_by_model=True,
            model=JEV_GATEWAY_MODEL,
            store_results=store_results,
            api_backend="vercel",
        )

    def _check_circuit(self):
        if self._circuit_open.is_set():
            raise BulkRequestAborted(
                f"JEV gateway batch stopped after {self.max_server_errors} HTTP 5xx responses; "
                "retry the run when the gateway has recovered"
            )

    def get_prompt_response_obj(self, given_id, text, request_kwargs=None):
        """Treat a previously stored API error as a miss, so it can be retried."""
        row = super().get_prompt_response_obj(given_id, text, request_kwargs)
        return None if row is not None and row.num_errors else row

    def get_completion(
        self, prompt_str: str, text: str, return_all: bool = False, **kwargs
    ):
        """Evaluate state with Jev; the cache supplies ``prompt_str`` for identity."""
        self._check_circuit()
        if kwargs:
            raise TypeError(f"Jev does not accept Responses API options: {sorted(kwargs)}")
        headers = {"Authorization": f"Bearer {get_vercel_client().api_key}"}
        for attempt in range(self.max_retries + 1):
            self._check_circuit()
            try:
                response = requests.post(
                    JEV_GATEWAY_URL,
                    headers=headers,
                    json={
                        "model": self.model,
                        "providerOptions": {"gateway": {"only": ["typesafe-ai"]}},
                        "state": text,
                        "questions": self.questions,
                    },
                    timeout=self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_retries:
                    raise
            else:
                if 500 <= response.status_code < 600:
                    logger.error(
                        "JEV gateway HTTP %s (attempt %s/%s, x-vercel-id=%s, x-request-id=%s): %s",
                        response.status_code, attempt + 1, self.max_retries + 1,
                        response.headers.get("x-vercel-id", "-"),
                        response.headers.get("x-request-id", "-"),
                        response.text[:2000],
                    )
                    with self._server_error_lock:
                        self._server_errors += 1
                        if self._server_errors >= self.max_server_errors:
                            self._circuit_open.set()
                    self._check_circuit()
                if response.status_code not in _RETRYABLE_STATUS_CODES or attempt == self.max_retries:
                    response.raise_for_status()
                    break
            time.sleep(2**attempt)
        result = _JevResponse(response.json())
        return result if return_all else result.payload["answers"]
