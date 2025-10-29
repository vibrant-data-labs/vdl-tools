import math
import logging
from typing import Optional, Tuple, List, Dict, Any
import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.model_caches.relevance_cache import generate_predictions
from vdl_tools.shared_tools.tools.logger import logger


# Fine-tuned model definitions
CB_CD_MODEL_4OMINI = 'ft:gpt-4o-mini-2024-07-18:vibrant-data-labs:cb-cd-just-conservative:9x1MMlJz'
CB_CD_MODEL_4OMINI_TAILWIND = "ft:gpt-4o-mini-2024-07-18:vibrant-data-labs:cb-cd-tw-airtable:A9l2oRot"


DEFAULT_CLIMATE_SYSTEM_PROMPT = "You are a climate change expert."
DEFAULT_CLIMATE_PROMPT_FORMAT = (
    "Categorize the following company descriptions as either pertinent (1) or irrelevant (0) to addressing the climate crisis: {text} -> \n#"
)

# This is technically unneccsary but convenience function
def generate_climate_relevance_predictions(
    df: pd.DataFrame,
    column_text: str,
    idn: str = "uuid",
    model: str = CB_CD_MODEL_4OMINI,
    max_workers: int = 3,
    n_per_commit: int = 50,
    use_cached_results: bool = True,
    label_override_dict: Dict[str, int] = None,
    label_override_filepath: str = None,
    system_prompt: str = DEFAULT_CLIMATE_SYSTEM_PROMPT,
    prompt_format: str = DEFAULT_CLIMATE_PROMPT_FORMAT,
    prompt_name: str = "climate_relevance_classification",
    session=None,
) -> Dict[str, Tuple[Optional[int], Optional[float]]]:
    """Generate climate relevance predictions for a DataFrame.

    This is a standalone function that provides a similar interface to the original
    generate_predictions function in gpt_relevant_for_thinning.py, but uses the
    modern ClimateRelevanceCache with database caching.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the data to predict.
    column_text : str
        Name of the column containing the text to classify.
    idn : str, default "uuid"
        Name of the column containing unique identifiers.
    model : str, default CB_CD_MODEL_4OMINI
        Fine-tuned model to use for predictions.
    max_workers : int, default 3
        Number of worker threads for parallel processing.
    n_per_commit : int, default 50
        Number of records per database commit.
    use_cached_results : bool, default True
        Whether to use cached results.
    label_override_dict : dict of str to int, optional
        Optional dict of {id: label} overrides.
    label_override_filepath: str, optional
        Optional filepath to the label_overrride dict
    system_prompt : str, default DEFAULT_CLIMATE_SYSTEM_PROMPT
        System prompt for the model.
    prompt_format : str, default DEFAULT_CLIMATE_PROMPT_FORMAT
        Format string for the user prompt.
    prompt_name : str, default "climate_relevance_classification"
        Name for the prompt in the database.
    session : sqlalchemy.orm.Session, optional
        Database session. If None, creates a new session.

    Returns
    -------
    dict of str to tuple of (int or None, float or None)
        Dictionary mapping IDs to (prediction, probability) tuples.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     'uuid': ['org_1', 'org_2'],
    ...     'description': ['Solar panel manufacturer', 'Restaurant chain']
    ... })
    >>> predictions = generate_predictions(df, 'description', 'uuid')
    >>> print(predictions)
    {'org_1': (1, 0.95), 'org_2': (0, 0.98)}
    """
    return generate_predictions(
        df=df,
        column_text=column_text,
        idn=idn,
        model=model,
        max_workers=max_workers,
        n_per_commit=n_per_commit,
        use_cached_results=use_cached_results,
        label_override_dict=label_override_dict,
        label_override_filepath=label_override_filepath,
        system_prompt=system_prompt,
        prompt_format=prompt_format,
        prompt_name=prompt_name,
        session=session,
    )
