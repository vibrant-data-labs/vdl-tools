"""Primary-category selection for multi-tag taxonomy matches.

When an organisation is accepted into more than one taxonomy category, this
module provides an LLM-backed step that picks the single category that best
represents the org's primary mission.  The result is stored in the SQL cache
so repeat runs with the same org/category set are served without hitting the
API.
"""

import json
from textwrap import dedent

from pydantic import BaseModel

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.tools.logger import logger


PRIMARY_CATEGORY_PROMPT = dedent("""\
    You are an expert taxonomist.
    An organization has been matched to multiple taxonomy categories.
    Your task is to identify which single category BEST represents
    the organization's PRIMARY mission and core focus.

    Rules:
    - Choose the category aligned with the org's central mission, not a secondary activity
    - Prefer more specific categories (solutions) over broader ones when the org clearly fits
    - If a website URL is provided, you may use web search to gather additional evidence
    - Return ONLY the exact category name as it appears in the input
""").strip()


class PrimaryCategory(BaseModel):
    """Structured output for primary-category selection."""

    reasoning: str
    evidence: list[str]
    primary_category_name: str


class PrimaryCategoryCache(InstructorPRC):
    """Cache for primary-category selection among multiple accepted taxonomy matches.

    Thin wrapper around InstructorPRC that wires up the fixed prompt, response
    model, and cache name. The user message is pre-formatted as a plain string
    by ``_format_user_message`` before being passed to ``bulk_get_cache_or_run``,
    so no method overrides are needed.
    """

    def __init__(
        self,
        session,
        model=DEFAULT_MODEL,
        filter_by_model=False,
        store_results=True,
    ):
        super().__init__(
            session=session,
            prompt_str=PRIMARY_CATEGORY_PROMPT,
            prompt_name="taxonomy_primary_category",
            response_model=PrimaryCategory,
            filter_by_model=filter_by_model,
            model=model,
            store_results=store_results,
        )


def _format_user_message(payload: dict) -> str:
    """Format org info and candidate categories into a plain user-message string.

    Parameters
    ----------
    payload : dict
        Keys: ``org_name`` (str, optional), ``org_url`` (str, optional),
        ``org_description`` (str), ``categories`` (list of dicts with keys
        ``name``, ``level0``, ``level1``, ``level2``, ``definition``).

    Returns
    -------
    str
        Formatted user message ready to be passed as ``text`` to
        ``InstructorPRC.bulk_get_cache_or_run``.
    """
    category_lines = []
    for i, cat in enumerate(payload["categories"], start=1):
        path_parts = [cat.get("level0"), cat.get("level1"), cat.get("level2")]
        path = " > ".join(p for p in path_parts if p)
        category_lines.append(
            f"{i}. {cat['name']}\n"
            f"   Path: {path}\n"
            f"   Definition: {cat['definition']}"
        )
    org_lines = []
    if payload.get("org_name"):
        org_lines.append(f"- Name: {payload['org_name']}")
    if payload.get("org_url"):
        org_lines.append(f"- Website: {payload['org_url']}")
    org_lines.append(f"- Description: {payload['org_description']}")

    return (
        "Organization:\n"
        + "\n".join(org_lines)
        + "\n\nMatched categories:\n"
        + "\n\n".join(category_lines)
        + "\n\nReturn the exact name of the primary category."
    )


def run_primary_category_selection(
    all_df,
    id_col,
    text_col,
    taxonomy,
    mapped_category_col='mapped_category',
    relevancy_col='reranked_relevancy',
    pct_col='pct',
    name_col=None,
    url_col=None,
    use_web_search=False,
    use_cached_results=True,
    max_workers=3,
    max_errors=1,
    model=None,
    **kwargs,
):
    """For orgs with >1 accepted taxonomy match, use an LLM to select the primary category.

    Adds a boolean column ``is_llm_primary`` to the DataFrame. Single-match
    orgs are marked primary directly without an LLM call. The cache key is
    ``"{org_id}|{sorted_category_names}"`` so re-runs with the same accepted
    set are served from cache.

    When ``filter_fewshot_classification=False`` the input DataFrame contains
    both accepted and rejected rows. ``relevancy_col`` is used to restrict
    primary-selection logic to accepted rows only; rejected rows always keep
    ``is_llm_primary=False``.

    Parameters
    ----------
    all_df : pd.DataFrame
        Per-match DataFrame (one row per org-category pair). May contain both
        accepted and rejected rows. Must have columns: id_col, text_col,
        mapped_category_col, level0, level1, level2.
    id_col : str
        Column name for the org identifier.
    text_col : str
        Column name for the org description text.
    taxonomy : list of dict
        Taxonomy definition used to look up category definitions.
    mapped_category_col : str, optional
        Column holding the matched category name. Default ``'mapped_category'``.
    relevancy_col : str, optional
        Boolean column that marks accepted rows. When present, only rows where
        this column is True are considered for primary selection.
        Default ``'reranked_relevancy'``.
    pct_col : str, optional
        Column used for fallback ranking when the LLM returns an unrecognized
        category. Default ``'pct'``. Pass ``'pct_ed_category'`` when working
        with data loaded from the saved results file (after column renaming).
    name_col : str, optional
        Column holding the org name. When provided, included in the LLM prompt.
    url_col : str, optional
        Column holding the org website URL. When provided, included in the LLM
        prompt so the model can use it as a web search anchor.
    use_web_search : bool, optional
        When True, passes ``tools=[{"type": "web_search_preview"}]`` to the
        API so the model can look up the org's website for additional evidence.
        Requires ``url_col`` to be set for best results. Default False.
    use_cached_results : bool, optional
        Whether to use the SQL cache. Default True.
    max_workers : int, optional
        Threads for parallel API calls. Default 3.
    max_errors : int, optional
        Max errors before giving up per item. Default 1.
    model : str, optional
        OpenAI model to use. Defaults to ``PrimaryCategoryCache`` default.
    **kwargs
        Extra kwargs forwarded to ``bulk_get_cache_or_run`` and then to the
        OpenAI API. Use model-appropriate options, e.g.
        ``reasoning={"effort": "medium"}`` for reasoning models such as
        ``gpt-5-nano``.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with new boolean column ``is_llm_primary``.
        Rejected rows (relevancy_col=False) always have is_llm_primary=False.
    """
    definitions_dict = dict([
        category_definition_tuple
        for tax_level in taxonomy
        for category_definition_tuple in
        tax_level['data'][[tax_level['name'], tax_level['textattr']]].values.tolist()
    ])

    all_df = all_df.copy()
    all_df['is_llm_primary'] = False

    # Restrict primary selection to accepted rows only
    if relevancy_col and relevancy_col in all_df.columns:
        accepted_df = all_df[all_df[relevancy_col].astype(bool)]
    else:
        accepted_df = all_df

    # ids_to_payloads: kept as dicts for response-mapping lookups
    # ids_to_texts: pre-formatted strings passed as `text` to the cache
    ids_to_payloads = {}
    ids_to_texts = {}

    for org_id, group in accepted_df.groupby(id_col):
        if len(group) == 1:
            # Single accepted match — mark it directly, no LLM call needed
            all_df.loc[group.index, 'is_llm_primary'] = True
            continue

        org_description = group[text_col].iloc[0]
        org_name = group[name_col].iloc[0] if name_col and name_col in group.columns else None
        org_url = group[url_col].iloc[0] if url_col and url_col in group.columns else None

        categories = []
        for _, row in group.iterrows():
            cat_name = row[mapped_category_col]
            categories.append({
                "name": cat_name,
                "level0": row.get("level0", "") or "",
                "level1": row.get("level1", "") or "",
                "level2": row.get("level2", "") or "",
                "definition": (definitions_dict.get(cat_name) or "").strip(),
            })

        # Sort categories for a stable, deterministic cache key :)
        sorted_categories = sorted(categories, key=lambda c: c["name"])
        sorted_cat_names = "|".join(c["name"] for c in sorted_categories)
        given_id = f"{org_id}|{sorted_cat_names}"

        payload = {
            "org_id": org_id,
            "org_name": org_name,
            "org_url": org_url,
            "org_description": org_description,
            "categories": sorted_categories,
        }
        ids_to_payloads[given_id] = payload
        ids_to_texts[given_id] = _format_user_message(payload)

    if ids_to_texts:
        cache_kwargs = {}
        if model is not None:
            cache_kwargs["model"] = model

        if use_web_search:
            kwargs.setdefault("tools", [{"type": "web_search_preview"}])
            kwargs.setdefault("max_output_tokens", 10000)

        with get_session() as session:
            primary_cache = PrimaryCategoryCache(session=session, **cache_kwargs)
            ids_to_responses = primary_cache.bulk_get_cache_or_run(
                given_ids_texts=ids_to_texts.items(),
                use_cached_result=use_cached_results,
                max_workers=max_workers,
                max_errors=max_errors,
                **kwargs,
            )

        # Map LLM responses back to DataFrame rows
        for given_id, response_dict in ids_to_responses.items():
            payload = ids_to_payloads[given_id]
            org_id = payload["org_id"]
            accepted_names = {c["name"] for c in payload["categories"]}

            response_text = response_dict.get("response_text")
            try:
                primary_name = json.loads(response_text).get("primary_category_name") if response_text else None
            except (json.JSONDecodeError, TypeError):
                primary_name = None

            if primary_name and primary_name in accepted_names:
                mask = (
                    (all_df[id_col] == org_id) &
                    (all_df[mapped_category_col] == primary_name)
                )
                all_df.loc[mask, 'is_llm_primary'] = True
            else:
                # Fallback: mark the highest-pct accepted row as primary
                logger.warning(
                    "Primary category '%s' not in accepted set for org %s. "
                    "Falling back to highest pct.", # prevents hallucinations
                    primary_name, org_id,
                )
                org_accepted_rows = accepted_df[accepted_df[id_col] == org_id]
                fallback_idx = all_df.loc[org_accepted_rows.index, pct_col].idxmax()
                all_df.loc[fallback_idx, 'is_llm_primary'] = True

    return all_df
