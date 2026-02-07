"""Few-shot cache for taxonomy relevance classification.

This module provides `FewShotCache`, an `InstructorPRC` subclass for the
taxonomy-mapping use case: given an entity and an activity (category), it
asks the model whether the entity is relevant to the category, using
few-shot examples in the prompt. Inputs are (entity_description,
activity_description) plus optional examples; the response is a structured
IsRelevant (boolean). Cache keys include the full prompt (entity, activity,
examples). All OpenAI API options are passed through via ``**kwargs``; see
the class docstring Notes for model-specific parameters (e.g. reasoning).
"""

import json
from textwrap import dedent

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from openai import APIConnectionError
from pydantic import BaseModel, field_validator
from typing import  Optional

from vdl_tools.shared_tools.database_cache.database_models.prompt import PromptResponse
from vdl_tools.shared_tools.openai.openai_api_utils import CLIENT
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL
from vdl_tools.shared_tools.openai.openai_constants import is_reasoning_model
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.tools.logger import logger

import logging

logging.basicConfig(level=logging.DEBUG)


class ExampleDict(BaseModel):
    """Single few-shot example: entity, activity/category, and relevance label."""

    entity_description: str
    activity_description: str
    relevant: bool
    activity_name: Optional[str] = None


class EntityActivityDict(BaseModel):
    """Entity and activity (category) to be classified for relevance."""

    entity_description: str
    activity_description: str
    acitvity_name: Optional[str] = None


class EntityActivityDictExamplesDict(BaseModel):
    """Input for a single classification: entity+activity plus optional few-shot examples."""

    entity_activity_dict: EntityActivityDict
    examples_dicts: Optional[list[ExampleDict]] = None

    @field_validator("examples_dicts", mode="before")
    @classmethod
    def check_examples(cls, examples_dicts):
        if examples_dicts is None:
            return EXAMPLES_CLASSED
        return examples_dicts


class IsRelevant(BaseModel):
    """Structured response: whether the entity is relevant to the activity/category."""

    is_relevant: bool


def _check_type_and_convert_to_dict(value, pydantic_class=EntityActivityDictExamplesDict):
    """Ensure value is a dict conforming to the given Pydantic model.

    Parameters
    ----------
    value : dict or pydantic_class instance
        Input to normalize (dict or already-validated model).
    pydantic_class : type, optional
        Pydantic model to validate against (default EntityActivityDictExamplesDict).

    Returns
    -------
    dict
        Valid dict representation (model_dump()).

    Raises
    ------
    ValueError
        If value is neither a dict nor an instance of pydantic_class.
    """
    if isinstance(value, pydantic_class):
        # Already converted, return the dict form
        return value.model_dump()
    if isinstance(value, dict):
        # Convert it to class to make sure it's compliant
        return pydantic_class(**value).model_dump()
    raise ValueError(f"Value must be a {pydantic_class} or a dict")


EXAMPLES = [
    {
        "entity_description": "Solution for blue-green algea problem in Friesland In warm weather in particular, blue-green algae can easily multiply and cause health problems for people who swim in outdoor waters. The water authority Fryslân, together with the Municipality of Leeuwarden, the province of Friesland and a university of applied sciences, has developed an innovative solution to this problem. In the bathing water area Kleine Wielen near Leeuwarden, a remote-controlled flushing system has been put in place. An artificial waterfall, new small dykes and a flushing canal provide fresh water along the beaches designated for swimming. Sending the water across a sludge screen made it possible to dredge the bathing water area separately and empty it of fish. A self-designed fish screen restricts the inflow of fish. In addition, water quality has been improved by the use of environmentally friendly shores. This recreational area has now been free of bluegreen algae for several years and the number of visitors has increased.",
        "activity_name": "Surface Water Protection (excl. wetlands)",
        "activity_description": "Surface Water Protection within Freshwater Ecosystem Entities involves safeguarding rivers, lakes, and streams from pollution, invasive species, and disruptions like aquatic barriers to ensure ecological balance and resilience against climate impacts. For example, controlling invasive species helps maintain native biodiversity which can better withstand climate-induced stresses, while beaver dams naturally regulate water flow and quality.",
        "relevant": True,
    },
    {
        "entity_description": "To raise awareness on climate mitigation in india through field-level information gathering and reporting. . Description: Mongabay.org (Mongabay) is a nonprofit environmental science news platform that publishes original content in several languages to raise awareness about social, climate, and environmental issues. Established in 2017, Mongabay's India team has published extensively on climate adaptation, resilience-building, and just transition. These reports have found strong resonance with readers in India and abroad. With this award, Mongabay's India team is conducting a Special Reporting Project on field issues that must be addressed proactively for India to achieve its climate mitigation goals in just and equitable ways. The award enables Mongabay to explore, investigate, and report on ways in which India is progressing with its greenhouse gas mitigation commitments, and the obstacles, challenges, and opportunities the country encounters. It supports Mongabay in expanding the reach of its storytelling through both vernacular reporting and social media.",
        "activity_name": "Terrestrial Wildlife and Biodiversity Management",
        "activity_description": "Terrestrial Wildlife and Biodiversity Management for climate adaptation and resilience involves the implementation of strategies and practices aimed at preserving the functionality and diversity of ecosystems under changing climatic conditions through active monitoring, regulation, and conservation of species and their habitats.",
        "relevant": False,
    },
    {
        "entity_description": "Gce phase ii - low cost biotic and abiotic stress prediction using ai to develop accurate predictive models for crop pest and disease occurrence, utilizing multiple data sources at different scales",
        "activity_name": "Pest Management",
        "activity_description": "Pest management involves deploying strategies such as organic pesticides, organic herbicides, and integrated weed management to sustainably control pest populations. These methods  promote robust plant health and reduce dependency on synthetic chemicals.",
        "relevant": True,
    },
    {
        "entity_description": "Market analysis for pastoralists (map) to identify best-bet inclusive and innovative market interventions to increase livestock market participation of pastoral communities in Burkina Faso, Nigeria and Ethiopia",
        "activity_name": "Agribusiness Marketplaces",
        "activity_description": "Agribusiness Marketplaces refer to digital or physical platforms that facilitate the distribution and sale of agricultural products directly from producers to consumers. These marketplaces promote climate adaptation and resilience by enabling farming collectives to diversify their customer base, reduce post-harvest losses through better logistics, and enhance access to resilient seed varieties and climate-smart farming practices.",
        "relevant": True,
    },
    {
        "entity_description": "Climateworks foundation Programme: Conservation and Science. Description: for support for Indigenous People and Local Community (IPLC) territorial rights, slowing deforestation in Brazil, and ensuring the integrity of carbon markets",
        "activity_name": "Non-Coastal (pelagic) Ocean Ecosystem Protection",
        "activity_description": "Non-Coastal (pelagic) Ocean Ecosystem Protection focuses on preserving the biodiversity and functionality of oceanic environments away from the shore. This includes implementing measures such as fishing quotas to prevent overexploitation of marine species and prohibiting deep-sea mining to safeguard underwater habitats.",
        "relevant": False,
    }
]

EXAMPLES_CLASSED = [ExampleDict(**x) for x in EXAMPLES]

INTRO_SENTENCE = dedent("""
    You are a strict taxonomist acting as a validation filter for a vector-search system to reject false positives.
    Task: Determine if the provided Entity strictly belongs to or is a direct instance of the Category.
    Rules for Evaluation:
    Direct Membership: The Entity must be a type of, or directly perform the function described in, the Category.
    Reject Tangential Relations: If the Entity is merely related to the Category (e.g., a tool used by the industry, or a neighbor concept) but is not of that Category, output "False".
    When in Doubt: If the relationship is ambiguous or weak, output "False".
    Output: Output ONLY the token "True" or "False".
""").strip()

DEFAULT_PROMPT = dedent("""You are an expert in climate mitigation, adaptation, resilience, and general climate change topics.""").strip()


class FewShotCache(InstructorPRC):
    """Few-shot cache for taxonomy relevance: entity vs activity/category with examples.

    Extends `InstructorPRC` with a fixed response model `IsRelevant` (boolean).
    Each request is (entity_description, activity_description) plus optional
    few-shot examples; the prompt is built from an intro, the examples, and
    the target entity/activity. Cache is keyed by the full input (including
    examples), so changing examples yields a new cache entry.

    Notes
    -----
    **OpenAI API kwargs and model-specific parameters**

    Methods that call the API (`get_cache_or_run`, `bulk_get_cache_or_run`)
    accept ``**kwargs`` and forward them to the Responses API. **Which kwargs
    are valid depends on the model** set in the constructor:

    - **All models**: response is always structured as `IsRelevant`; you can
      pass other options supported by the Responses API.
    - **Reasoning models** (e.g. ``gpt-5-nano``): support ``reasoning``, e.g.
      ``reasoning={"effort": "high", "summary": "auto"}``.
    - **Chat / non-reasoning models** (e.g. ``gpt-4.1-mini``): support
      ``temperature``, ``max_tokens``, etc. as allowed by the API.

    Use only kwargs that match the configured model. See
    `prompt_response_cache_sql.PromptResponseCacheSQL` and
    `prompt_response_cache_instructor.InstructorPRC` for more on cache
    keying and model filtering.
    """

    def __init__(
        self,
        session,
        model=DEFAULT_MODEL,
        filter_by_model=False,
        prompt_str = DEFAULT_PROMPT,
        prompt_name: str = "taxonomy_few_shot",
        store_results=True,
    ):
        """Initialize the few-shot cache for taxonomy relevance classification.

        Parameters
        ----------
        session
            SQLAlchemy session for database access.
        model : str, optional
            OpenAI model name (e.g. gpt-4.1-mini, gpt-5-nano). Default is
            DEFAULT_MODEL.
        filter_by_model : bool, optional
            If True, cache is scoped by model name. Default is False.
        prompt_str : str, optional
            System prompt (expert role / task description). Default is
            DEFAULT_PROMPT (climate expert).
        prompt_name : str, optional
            Name for the prompt when registering. Default is "taxonomy_few_shot".
        store_results : bool, optional
            If True, store the results in the cache. Default is True.
        """
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            response_model=IsRelevant,
            filter_by_model=filter_by_model,
            model=model,
            store_results=store_results,
        )

    def _format_messages(
        self,
        examples_dicts,
        entity_activity_dict,
        intro_sentence=INTRO_SENTENCE,
    ):
        """Build chat messages: intro + few-shot examples + target entity/activity.

        Parameters
        ----------
        examples_dicts : list of dict
            Few-shot examples; each dict has entity_description,
            activity_description, relevant (and optionally activity_name).
        entity_activity_dict : dict
            Target entity/activity to classify; keys entity_description,
            activity_description (and optionally activity_name).
        intro_sentence : str, optional
            Task instructions prepended to the prompt. Default is INTRO_SENTENCE.

        Returns
        -------
        list of dict
            OpenAI chat format: [{"role": "system", "content": ...}, {"role": "user", "content": ...}].
        """
        example_text_format = dedent("""
            Entity: {entity_description}
            Category: "{activity}"
            Relevant: {relevant}
        """).strip()

        examples_text = "\n\n".join([
            example_text_format.format(
                entity_description=x["entity_description"],
                activity=x["activity_description"],
                relevant=x["relevant"]
            )
            for x in examples_dicts
        ])

        message_text = "\n\n".join([
            intro_sentence,
            examples_text,
            example_text_format.format(
                entity_description=entity_activity_dict["entity_description"],
                activity=entity_activity_dict['activity_description'],
                relevant="",
            ).strip(),
        ]
        )

        messages = [
            {"role": "system", "content": self.prompt_text},
            {
                "role": "user",
                "content": message_text,
            }
        ]

        return messages


    def bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, EntityActivityDictExamplesDict]],
        use_cached_result: bool = True,
        n_per_commit: int = 50,
        max_workers=5,
        max_errors=1,
        return_parsed_results: bool = True,
        **kwargs
    ) -> dict:
        """Bulk get cached or fresh relevance results for many (given_id, entity+examples) pairs.

        Converts each text to `EntityActivityDictExamplesDict` dict form, then
        delegates to the parent bulk cache. Optionally returns parsed JSON
        (dict with is_relevant) per id instead of full response dicts.

        Parameters
        ----------
        given_ids_texts : list[tuple[str, EntityActivityDictExamplesDict]]
            (given_id, entity_activity_dict_examples_dict) pairs; the second
            element can be a dict or Pydantic instance.
        use_cached_result : bool, optional
            If True, use cached results when available. Default is True.
        n_per_commit : int, optional
            Chunk size for DB commits. Default is 50.
        max_workers : int, optional
            Number of parallel workers. Default is 5.
        max_errors : int, optional
            Max errors per (given_id, text) before skipping. Default is 1.
        return_parsed_results : bool, optional
            If True, return dict mapping id -> parsed JSON (e.g. {"is_relevant": bool}).
            If False, return dict mapping id -> full response dict. Default is True.
        **kwargs
            Passed to the OpenAI API (model-dependent; see class Notes).

        Returns
        -------
        dict
            Map from given_id to response (parsed dict or full response dict
            depending on return_parsed_results).
        """
        given_ids_texts = [
            (given_id, _check_type_and_convert_to_dict(text, pydantic_class=EntityActivityDictExamplesDict))
            for given_id, text in given_ids_texts
        ]
        ids_to_responses = self._bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            use_cached_result=use_cached_result,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
            **kwargs,
        )

        if not return_parsed_results:
            return ids_to_responses
        return {id_: json.loads(response["response_text"]) for id_, response in ids_to_responses.items()}


    def get_cache_or_run(
        self,
        given_id: str,
        activity_entity_dict: EntityActivityDict|dict,
        examples_dicts: list[dict]=None,
        use_cached_result: bool = True,
        **kwargs
    ):
        """Return cached or fresh relevance result for one entity/activity (and optional examples).

        Parameters
        ----------
        given_id : str
            User-defined identifier for this request.
        activity_entity_dict : EntityActivityDict or dict
            Entity and activity (category) to classify; keys
            entity_description, activity_description (and optionally
            activity_name).
        examples_dicts : list of dict, optional
            Few-shot examples; each dict has entity_description,
            activity_description, relevant (and optionally activity_name).
            Default is EXAMPLES.
        use_cached_result : bool, optional
            If True, use cached result when available. Default is True.
        **kwargs
            Passed to the OpenAI API (model-dependent; see class Notes).

        Returns
        -------
        dict or None
            Cached or newly fetched response dict (e.g. response_text,
            response_full), or None if the call failed.
        """
        examples_dicts = examples_dicts or EXAMPLES

        activity_entity_dict = _check_type_and_convert_to_dict(activity_entity_dict, pydantic_class=EntityActivityDict)
        examples_dicts = [
            _check_type_and_convert_to_dict(x, pydantic_class=ExampleDict)
            for x in examples_dicts
        ]
        # Re-format so that it adheres to the PromptResponseCache inputs
        entity_activity_dict_examples_dicts = {
            "entity_activity_dict": activity_entity_dict,
            "examples_dicts": examples_dicts,
        }
        return self._get_cache_or_run(
            given_id=given_id,
            text=entity_activity_dict_examples_dicts,
            use_cached_result=use_cached_result,
            **kwargs,
        )

    @retry(
        stop=stop_after_attempt(5),  # Retry up to 5 times
        wait=wait_fixed(2),          # Wait 2 seconds between retries
        retry=retry_if_exception_type(APIConnectionError),  # Retry only on ConnectionError
        reraise=True                 # Reraise the exception if max retries are exceeded
    )
    def get_completion(
        self,
        _, # prompt_str
        entity_activity_dict_examples_dicts: EntityActivityDictExamplesDict | dict,
        max_tokens=4096,
        **kwargs
    ):
        """Call the Responses API for one relevance classification (with retries on connection errors).

        Builds messages from the intro, few-shot examples, and target
        entity/activity; uses IsRelevant as text_format. For reasoning
        models, passes instructions and only the last user message as input.

        Parameters
        ----------
        _ : Any
            Unused; present for compatibility with parent get_completion(prompt_str, text, **kwargs).
        entity_activity_dict_examples_dicts : EntityActivityDictExamplesDict or dict
            Target entity/activity and optional few-shot examples. Dict shape:
            {"entity_activity_dict": {"entity_description": ..., "activity_description": ...},
             "examples_dicts": [{"entity_description": ..., "activity_description": ..., "relevant": bool}, ...]}.
        max_tokens : int, optional
            Max tokens for the completion. Default is 4096.
        **kwargs
            Passed to the Responses API (e.g. reasoning for reasoning models).

        Returns
        -------
        Response
            Full API response (with output_parsed as IsRelevant instance).
        """
        entity_activity_dict_examples_dicts = _check_type_and_convert_to_dict(
            entity_activity_dict_examples_dicts,
            pydantic_class=EntityActivityDictExamplesDict
        )

        entity_activity_dict = entity_activity_dict_examples_dicts["entity_activity_dict"]
        examples_dicts = entity_activity_dict_examples_dicts["examples_dicts"]

        messages = self._format_messages(
            examples_dicts,
            entity_activity_dict,
        )
        response_kwargs = {
            "model": self.model,
            "input": messages,
            "text_format": self.response_model,
        }
        if is_reasoning_model(self.model):
            # Make the prompt_str the instructions
            response_kwargs["instructions"] = self.prompt_text
            last_message = messages[-1]['content']
            response_kwargs['input'] = last_message
        response = CLIENT.responses.parse(**response_kwargs)

        return response

    def store_item(
        self,
        given_id: str,
        text,
        response,
    ):
        """Store a successful relevance response in the cache.

        Overrides parent to serialize text and response for the few-shot
        input shape and IsRelevant output.

        Parameters
        ----------
        given_id : str
            User-defined identifier for the request.
        text
            Input (entity_activity_dict_examples_dict); stored as JSON.
        response
            Full API response (must have model_dump(), output_parsed with model_dump()).

        Returns
        -------
        PromptResponse
            The merged cache row.
        """
        prompt_response_obj = PromptResponse(
            prompt_id=self.prompt.id,
            given_id=given_id,
            model_name=self.model,
            input_text=json.dumps(text),
            response_full=response.model_dump(),
            response_text=json.dumps(response.output_parsed.model_dump()),
            num_errors=0,
        )

        if self.store_results:
            self.session.merge(prompt_response_obj)
        return prompt_response_obj


    def store_error(
        self,
        given_id: str,
        text,
        response_full,
    ):
        """Store or update a cache row for a failed API call (increment num_errors).

        Overrides parent to match by prompt_id and given_id only (no text_id)
        for this cache's error storage behavior.

        Parameters
        ----------
        given_id : str
            User-defined identifier for the request.
        text
            Input that was sent (stored as JSON).
        response_full
            Raw error/response payload to store.

        Returns
        -------
        PromptResponse
            The merged cache row.
        """
        logger.info("Storing error for %s, %s", self.prompt.name, given_id)
        previous_response = (
            self.session.query(PromptResponse)
            .filter(
                PromptResponse.prompt_id == self.prompt.id,
                PromptResponse.given_id == given_id,
            )
            .first()
        )
        if previous_response:
            previous_response.response_full = response_full
            if previous_response.num_errors:
                previous_response.num_errors += 1
            else:
                previous_response.num_errors = 1
            if self.store_results:
                self.session.merge(previous_response)
            prompt_response_obj = previous_response
        else:
            prompt_response_obj = PromptResponse(
                prompt_id=self.prompt.id,
                given_id=given_id,
                input_text=json.dumps(text),
                response_full=response_full,
                num_errors=1,
            )
            if self.store_results:
                self.session.merge(prompt_response_obj)
        return prompt_response_obj


if __name__ == "__main__":
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    with get_session() as session:
        few_shot_cache = FewShotCache(
            session=session,
            model="gpt-5-nano",
            prompt_str="Always reply with False."
        )
        response = few_shot_cache.get_cache_or_run(
            given_id="test",
            activity_entity_dict={
                "entity_description": "DogRunner trains dogs to run",
                "activity_description": "Excerise for Pets: Things that make pets exercise",
            },
            examples_dicts=EXAMPLES_CLASSED,
            use_cached_result=False,
        )
        import ipdb; ipdb.set_trace()
