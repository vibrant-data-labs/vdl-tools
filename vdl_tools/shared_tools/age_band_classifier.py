import json
from textwrap import dedent
from pydantic import BaseModel, Field
from typing import Literal

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC


class AgeBandClassification(BaseModel):
    age_bands: list[Literal["prek", "elementary", "middle_school", "high_school", "college_prep"]] = Field(
        description=(
            "List of applicable age bands. "
            "May be empty if unclassifiable is True."
        )
    )
    evidence: str = Field(
        description=(
            "1-3 sentences quoting or paraphrasing the text that most clearly "
            "indicates the learner age or stage. If unclassifiable, explain why."
        )
    )
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "'high' if org explicitly names grades/ages; "
            "'medium' if inferred from context; 'low' if only weakly implied."
        )
    )
    unclassifiable: bool = Field(
        description="True if the text has no detectable signal about learner age or stage."
    )
    serves_educators: bool = Field(
        description=(
            "True if the org's PRIMARY mission is serving teachers, administrators, "
            "curriculum designers, or instructional coaches rather than learners directly."
        )
    )


AGE_BAND_PROMPT = dedent(
    """
    You are an education sector analyst. Given the description of an education-related
    organization, classify which learner age bands the organization primarily serves.

    ## Age Band Definitions

    Use ONLY these exact token values in the age_bands list:

    - "prek": Children in pre-kindergarten programs (ages 3-5).
      Includes preschool and pre-K programs only. Does NOT include birth-to-3,
      infant/toddler care, or early intervention programs for children under age 3.

    - "elementary": Learners in kindergarten through 5th grade (~ages 5-11).
      K-5 schools, lower school programs.

    - "middle_school": Learners in 6th through 8th grade (~ages 11-14).
      Middle school, junior high programs.

    - "high_school": Learners in 9th through 12th grade (~ages 14-18).
      High school, secondary education programs.

    - "college_prep": Pre-college preparation or dual enrollment for K-12 students.
      SAT/ACT prep, college access, dual enrollment, early college high school.
      Do NOT include standard colleges/universities unless they have an explicit
      pre-college program for K-12 students.

    ## Multiple Bands

    Select ALL bands that apply. A program serving K-12 broadly should receive
    ["elementary", "middle_school", "high_school"]. Only select bands with clear
    evidence in the text.

    ## Special Cases

    ### serves_educators
    Set serves_educators=True if the PRIMARY mission is supporting teachers, admins,
    curriculum designers, or instructional coaches rather than delivering learning
    to students directly. An org may BOTH serve educators AND imply an age stage
    (e.g. pre-K teacher PD -> serves_educators=True, age_bands=["prek"]).

    ### unclassifiable
    Set unclassifiable=True if the text has NO detectable signal about learner age
    or stage. When unclassifiable=True, age_bands must be an empty list [].

    ## Output
    Respond with a JSON object matching the required schema.
    """
).strip()


def get_bulk_age_bands(
    ids_texts: list[tuple],
    use_cached_results: bool = True,
    prompt_string: str = AGE_BAND_PROMPT,
    n_per_commit: int = 50,
    max_workers: int = 10,
    max_errors: int = 1,
    prompt_name: str = "age_band_classification",
):
    with get_session() as session:
        prompt_response = InstructorPRC(
            session=session,
            prompt_str=prompt_string,
            response_model=AgeBandClassification,
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


def add_age_band_classification(
        df,
        text_col="text_for_one_earth",
        id_col="id",
        use_cached_results=False,
):
    ids_text_lists = df[[id_col, text_col]].values.tolist()
    ids_to_response_text = get_bulk_age_bands(
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

    df["age_bands"]                 = parsed.apply(lambda x: x.get("age_bands"))
    df["age_band_evidence"]         = parsed.apply(lambda x: x.get("evidence"))
    df["age_band_confidence"]       = parsed.apply(lambda x: x.get("confidence"))
    df["age_band_unclassifiable"]   = parsed.apply(lambda x: x.get("unclassifiable"))
    df["age_band_serves_educators"] = parsed.apply(lambda x: x.get("serves_educators"))

    return df
