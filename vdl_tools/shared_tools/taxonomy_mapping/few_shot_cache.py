import json
from textwrap import dedent

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from openai import OpenAI, pydantic_function_tool, APIConnectionError
from pydantic import BaseModel, field_validator
from typing import Literal, Optional

from vdl_tools.shared_tools.database_cache.database_models.prompt import PromptResponse
from vdl_tools.shared_tools.openai.openai_api_utils import CLIENT
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.tools.logger import logger

import logging

logging.basicConfig(level=logging.DEBUG)


class ExampleDict(BaseModel):
    entity_description: str
    activity_description: str
    relevant: bool
    activity_name: Optional[str] = None

class EntityActivityDict(BaseModel):
    entity_description: str
    activity_description: str
    acitvity_name: Optional[str] = None


class EntityActivityDictExamplesDict(BaseModel):
    entity_activity_dict: EntityActivityDict
    examples_dicts: Optional[list[ExampleDict]] = None

    @field_validator("examples_dicts", mode="before")
    @classmethod
    def check_examples(cls, examples_dicts):
        if examples_dicts is None:
            return EXAMPLES_CLASSED
        return examples_dicts

class IsRelevant(BaseModel):
    is_relevant: bool



def _check_type_and_convert_to_dict(value, pydantic_class=EntityActivityDictExamplesDict):
    if isinstance(value, pydantic_class):
        # Already converted, return the dict form
        return value.dict()
    if isinstance(value, dict):
        # Convert it to class to make sure it's compliant
        return pydantic_class(**value).dict()
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
    You are an Assistant responsible for helping detect whether the mapped categories are relevant to the entity description.
    For a given input, you need to output a single token: "True" or "False" indicating the retrieved category is relevant to the entity.
""").strip()


class FewShotCache(InstructorPRC):
    def __init__(
        self,
        session,
        model="gpt-4.1-mini",
    ):
        prompt_str = "You are an expert in climate mitigation, adaptation, resilience, and general climate change topics."

        prompt_name = "taxonomy_few_shot"
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            model=model,
            response_model=IsRelevant,
        )

    def _format_messages(
        self,
        examples_dicts,
        entity_activity_dict,
        intro_sentence=INTRO_SENTENCE,
    ):

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
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        n_per_commit: int = 50,
        max_workers=5,
        max_errors=1,
        return_parsed_results: bool = True,
        **kwargs
    ) -> str:
        given_ids_texts = [
            (given_id, _check_type_and_convert_to_dict(text, pydantic_class=EntityActivityDictExamplesDict))
            for given_id, text in given_ids_texts
        ]
        ids_to_responses = self._bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            model=model,
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
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        **kwargs
    ):
        examples_dicts = examples_dicts or EXAMPLES

        activity_entity_dict = _check_type_and_convert_to_dict(activity_entity_dict, pydantic_class=EntityActivityDict)
        examples_dicts = [
            _check_type_and_convert_to_dict(x, pydantic_class=ExampleDict)
            for x in examples_dicts
        ]
        # Re-format so that it adheres to the PromptResponseCache inputs
        entity_activity_dict_examples_dicts = (activity_entity_dict, examples_dicts)
        return self._get_cache_or_run(
            given_id=given_id,
            text=entity_activity_dict_examples_dicts,
            model=model,
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
        """_summary_

        Parameters
        ----------
        _ : _type_
            _description_
        examples_entity_activity_dict : EntityActivityDictExamplesDict
            A tuple containing a list of examples and a dictionary containing the entity description and activities
            (
                [
                    {"entity_description": "description",
                    "activity_name": "activity",
                    "activity_description": "description",
                    "relevant": bool}
                ],
                {
                    "entity_description": "description",
                    "activity_name": "activity",
                    "activity_description": "description",
                }
            )
        max_tokens : int, optional
            _description_, by default 4096

        Returns
        -------
        _type_
            _description_
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
        response = CLIENT.beta.chat.completions.parse(
            model=MODEL_DATA[self.model]["model_name"],
            max_tokens=max_tokens,
            messages=messages,
            logprobs=True,
            seed=7118,
            temperature=0.2,
            tools=[
                pydantic_function_tool(
                    self.response_model,
                    name="relevance_classification",
                    description="Classify the entity as relevant to the category description."
                )
            ],
        )

        return response

    def store_item(
        self,
        given_id: str,
        text,
        response,
    ):
        prompt_response_obj = PromptResponse(
            prompt_id=self.prompt.id,
            given_id=given_id,
            input_text=json.dumps(text),
            response_full=response.dict(),
            response_text=json.dumps(response.choices[0].message.tool_calls[0].function.parsed_arguments.dict()),
            num_errors=0,
        )

        self.session.merge(prompt_response_obj)
        return prompt_response_obj


    def store_error(
        self,
        given_id: str,
        text,
        response_full,
    ):
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
            self.session.merge(prompt_response_obj)
        return prompt_response_obj
