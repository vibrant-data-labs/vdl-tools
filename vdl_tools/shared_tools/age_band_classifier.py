import json
from textwrap import dedent
from pydantic import BaseModel, Field
from typing import Literal

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC


class LearningJourneyStageClassification(BaseModel):
    learning_journey_stages: list[Literal[
        "early_childhood", "lower_grades", "upper_grades", "big_blur"
    ]] = Field(
        description=(
            "List of applicable learning journey stages from "
            "['early_childhood', 'lower_grades', 'upper_grades', 'big_blur']. "
            "May be empty if unclassifiable is True."
        )
    )
    evidence: str = Field(
        description=(
            "1 short sentence quoting or paraphrasing the text that most clearly "
            "indicates the learner stage. If unclassifiable, explain why."
        )
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "'high' if org explicitly names grades/ages; "
            "'medium' if inferred from context; 'low' if only weakly implied."
        )
    )
    unclassifiable: bool = Field(
        description="True if the text has no detectable signal about learner stage."
    )


LEARNING_JOURNEY_STAGE_PROMPT = dedent(
    """
    You are an education sector analyst. Given the description of an education-related
    organization, classify which learning journey stages the organization primarily serves.

    This framework is informed by the JFF "Big Blur" vision, which argues that the
    line between high school, college, and careers should dissolve for learners ages 16-20.

    ## Stage Definitions

    Use ONLY these exact token values in the learning_journey_stages list:

    - "early_childhood": Learners in preschool programs — PK-3 and PK-4 (~ages 3-5).
      Does NOT include Kindergarten, birth-to-3, or infant/toddler programs.

    - "lower_grades": Learners in Kindergarten through 5th grade (~ages 5-11).
      Foundational literacy and numeracy. Starts at Kindergarten.

    - "upper_grades": Learners in 6th through 12th grade (~ages 11-18).
      Covers middle school and high school as a unified stage.

    - "big_blur": Learners ages 16-20 in the integrated zone where secondary,
      postsecondary, and career pathways converge (grades 11-14 per JFF framework).
      Includes: dual enrollment, early college high schools, CTE programs bridging
      HS and postsecondary, apprenticeships for young adults, career-focused associate
      degrees, college access programs for first-generation students, and workforce
      preparation for 16-to-20-year-olds.

    ## Multiple Stages

    Select ALL stages that apply. Stages may overlap where age ranges intersect:
    - K-12 broadly → ["lower_grades", "upper_grades"]
    - Preschool through elementary → ["early_childhood", "lower_grades"]
    - Grades 9-12 with dual enrollment or postsecondary bridge → ["upper_grades", "big_blur"]
    - PreK through high school → ["early_childhood", "lower_grades", "upper_grades"]

    ### Implicit ranges
    When the text describes a span, assign ALL stages within that range:
    - "PreK through high school" → ["early_childhood", "lower_grades", "upper_grades"]
    - "K-12 and beyond" → ["lower_grades", "upper_grades", "big_blur"]
    - "middle school through college" → ["upper_grades", "big_blur"]

    ## Unclassifiable
    Set unclassifiable=True if the text has NO detectable signal about learner stage.
    When unclassifiable=True, learning_journey_stages must be an empty list [].

    ## Output
    Respond with a JSON object matching the required schema.
    """
).strip()


def get_bulk_learning_journey_stages(
    ids_texts: list[tuple],
    use_cached_results: bool = True,
    prompt_string: str = LEARNING_JOURNEY_STAGE_PROMPT,
    n_per_commit: int = 50,
    max_workers: int = 10,
    max_errors: int = 1,
    prompt_name: str = "learning_journey_stage_classification",
):
    with get_session() as session:
        prompt_response = InstructorPRC(
            session=session,
            prompt_str=prompt_string,
            response_model=LearningJourneyStageClassification,
            prompt_name=prompt_name,
        )
        ids_to_response = prompt_response.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_results,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
        )
    return {k: v["response_text"] for k, v in ids_to_response.items()}


def add_learning_journey_stage_classification(
        df,
        text_col="text_for_one_earth",
        id_col="id",
        use_cached_results=False,
):
    ids_text_lists = df[[id_col, text_col]].values.tolist()
    ids_to_response_text = get_bulk_learning_journey_stages(
        ids_text_lists,
        use_cached_results=use_cached_results,
    )

    def safe_json(text):
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {}

    parsed = df[id_col].map(
        lambda id_: safe_json(ids_to_response_text.get(str(id_)))
    )

    df["learning_journey_stages"] = parsed.apply(lambda x: x.get("learning_journey_stages"))
    df["ljs_evidence"]            = parsed.apply(lambda x: x.get("evidence"))
    df["ljs_confidence"]          = parsed.apply(lambda x: x.get("confidence"))
    df["ljs_unclassifiable"]      = parsed.apply(lambda x: x.get("unclassifiable"))

    return df
