"""Prompt/response cache with structured (Pydantic) output.

This module provides `InstructorPRC`, a subclass of `PromptResponseCacheSQL`
that always uses a Pydantic response model for completions. It uses the
OpenAI Responses API (via `openai_api_utils.get_completion`) with
``text_format`` set to the response model—**not** the Instructor library.
The prompt stored in the cache includes the JSON schema of the response
model so cache lookups are consistent with the expected output shape. All
OpenAI API options are passed through via ``**kwargs``; see the class
docstring Notes for model-specific parameters (e.g. reasoning for gpt-5).
"""


import logging

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL, DEFAULT_MODEL
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion

logger.setLevel(logging.DEBUG)
logging.getLogger("openai").setLevel(logging.DEBUG)


class InstructorPRC(PromptResponseCacheSQL):
    """SQL-backed cache for completions that use a Pydantic response model.

    Extends `PromptResponseCacheSQL` so that every call uses the OpenAI
    Responses API (formerly used through the Instructor library) with a fixed ``text_format``
    set to the given response model. The prompt registered in the cache is
    the user prompt plus the response model's JSON schema, so cache keys
    reflect the expected output structure.

    Notes
    -----
    **OpenAI API kwargs and model-specific parameters**

    Methods that call the API (`get_cache_or_run`, `bulk_get_cache_or_run`)
    accept ``**kwargs`` and forward them to `get_completion`. **Which kwargs
    are valid depends on the model** set in the constructor:

    - **All models**: ``response_format`` / ``text_format`` is set internally
      from the constructor's response_model; you can pass other options
      supported by the Responses API.
    - **Reasoning models** (e.g. ``gpt-5-nano``): support ``reasoning``, e.g.
      ``reasoning={"effort": "high", "summary": "auto"}``. Pass these via
      kwargs in `get_cache_or_run` or `bulk_get_cache_or_run`.
    - **Chat / non-reasoning models** (e.g. ``gpt-4.1-mini``): support
      parameters like ``temperature``, ``max_tokens``, etc. as allowed by the
      API for that model.

    Use only kwargs that match the configured model. See
    `prompt_response_cache_sql.PromptResponseCacheSQL` for more detail on
    cache keying and model filtering.
    """

    def __init__(
        self,
        session,
        prompt_str,
        response_model,
        prompt_name=None,
        prompt_id=None,
        model=DEFAULT_MODEL,
        filter_by_model=False,
        store_results=True,
    ):
        """Initialize the cache with a prompt and Pydantic response model.

        The stored prompt is ``prompt_str`` plus the response model's JSON
        schema, so the schema is part of the cache identity.

        Parameters
        ----------
        session
            SQLAlchemy session for database access.
        prompt_str : str
            User-facing prompt text (instructions for the model).
        response_model : type
            Pydantic model class for structured output (e.g. BaseModel
            subclass). Used as ``text_format`` for all completions.
        prompt_name : str, optional
            Name for the prompt when registering. Default is None.
        prompt_id : str, optional
            Id of an existing prompt to load; if given, prompt_str and
            response_model should match the existing prompt. Default is None.
        model : str, optional
            OpenAI model name (e.g. gpt-4.1-mini, gpt-5-nano). Default is
            DEFAULT_MODEL.
        filter_by_model : bool, optional
            If True, cache is scoped by model name. Default is False.
        store_results : bool, optional
            If True, persist responses to the database. Default is True.
        """
        # If None or "" is passed in
        prompt_str_w_schema = self.build_prompt_str(prompt_str, response_model)
        super().__init__(
            session=session,
            prompt_str=prompt_str_w_schema,
            prompt_name=prompt_name,
            prompt_id=prompt_id,
            filter_by_model=filter_by_model,
            model=model,
            store_results=store_results,
        )
        self.prompt_text = prompt_str
        self.response_model = response_model
        self.model = model

    @classmethod
    def build_prompt_str(cls, prompt_str, response_model):
        """The full prompt this cache registers: prompt text + response schema JSON."""
        return f"{prompt_str}\n{response_model.schema_json()}"

    @classmethod
    def build_prompt_id(cls, prompt_str, response_model):
        """Deterministic cache ``prompt_id`` for (prompt_str, response_model).

        The id this cache would register or look up for the pair — a hash of
        ``build_prompt_str``'s output, identical to the ``prompt`` table's
        ``id`` column. Needs no session, so callers can record the cache
        identity in output lineage or query the table directly.
        """
        return Prompt.create_text_id(cls.build_prompt_str(prompt_str, response_model))

    def get_completion(
        self,
        prompt_str,
        text,
        **kwargs
    ):
        """Call the OpenAI Responses API with this cache's response model.

        Always passes ``text_format=self.response_model`` so output is
        validated and parsed into the Pydantic model. Other kwargs are
        forwarded to the API; use model-appropriate options (e.g. reasoning
        for gpt-5). See class docstring Notes.

        Parameters
        ----------
        prompt_str : str
            System prompt / instructions (typically the prompt without
            schema; the full prompt with schema is stored on the cache).
        text : str
            User message content.
        **kwargs
            Passed to the OpenAI Responses API (e.g. reasoning for
            reasoning models). Valid options depend on the model.

        Returns
        -------
        response_model instance
            Parsed completion as an instance of the Pydantic response model.
        """
        response = get_completion(
            prompt=prompt_str,
            text=text,
            model=self.model,
            text_format=self.response_model,
            **kwargs,
        )

        return response


if __name__ == "__main__":
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from pydantic import BaseModel
    class Response(BaseModel):
        capital: str

    with get_session() as session:
        prc = InstructorPRC(
            session=session,
            model="gpt-5-nano",
            prompt_str="Give the world capital of the country.",
            response_model=Response,
        )
        response_gpt_5_nano = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
        )
        print("GPT-5-Nano response:", response_gpt_5_nano['response_text'])


    with get_session() as session:
        prc = InstructorPRC(
            session=session,
            model="gpt-4.1-mini",
            prompt_str="Give the world capital of the country.",
            response_model=Response,
        )
        response_gpt_4_1_mini = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
        )
        print("GPT-4.1-Mini response:", response_gpt_4_1_mini['response_text'])