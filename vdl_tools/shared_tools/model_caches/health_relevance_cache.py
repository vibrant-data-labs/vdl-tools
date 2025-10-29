import math
import logging
from typing import Optional, Tuple, List, Dict, Any
import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.model_caches.relevance_cache import generate_predictions
from vdl_tools.shared_tools.tools.logger import logger


# Fine-tuned model definitions for ARPA-H / Health relevance
ARPAH_MODEL = 'ft:gpt-4.1-mini-2025-04-14:vibrant-data-labs:arpa-h:Bzr8QOcl'
ARPAH_CONSERVATIVE = 'ft:gpt-4.1-mini-2025-04-14:vibrant-data-labs:without-maybe:CMM6fpnL'


DEFAULT_HEALTH_SYSTEM_PROMPT = "You are a health and biomedical research expert."
DEFAULT_HEALTH_PROMPT_FORMAT = (
    "Categorize the following company descriptions as either pertinent (1) or irrelevant (0) to ARPA-H's mission of advancing health and biomedical research: {text} -> \n#"
)

# Convenience function for ARPA-H / Health relevance predictions
def generate_health_relevance_predictions(
    df: pd.DataFrame,
    column_text: str,
    idn: str = "uuid",
    model: str = ARPAH_CONSERVATIVE,
    max_workers: int = 3,
    n_per_commit: int = 50,
    use_cached_results: bool = True,
    label_override_dict: Dict[str, int] = None,
    label_override_filepath: str = None,
    system_prompt: str = DEFAULT_HEALTH_SYSTEM_PROMPT,
    prompt_format: str = DEFAULT_HEALTH_PROMPT_FORMAT,
    prompt_name: str = "health_relevance_classification",
    session=None,
) -> Dict[str, Tuple[Optional[int], Optional[float]]]:
    """Generate health/ARPA-H relevance predictions for a DataFrame.

    This is a standalone function that provides a similar interface to the original
    generate_predictions function in gpt_relevant_for_thinning.py, but uses the
    modern RelevanceCache with database caching.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the data to predict.
    column_text : str
        Name of the column containing the text to classify.
    idn : str, default "uuid"
        Name of the column containing unique identifiers.
    model : str, default ARPAH_CONSERVATIVE
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
        Optional filepath to the label_override dict
    system_prompt : str, default DEFAULT_HEALTH_SYSTEM_PROMPT
        System prompt for the model.
    prompt_format : str, default DEFAULT_HEALTH_PROMPT_FORMAT
        Format string for the user prompt.
    prompt_name : str, default "health_relevance_classification"
        Name for the prompt in the database.
    session : sqlalchemy.orm.Session, optional
        Database session. If None, creates a new session.

    Returns
    -------
    pd.DataFrame
        DataFrame with added 'prediction' and 'probability' columns.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     'uuid': ['org_1', 'org_2'],
    ...     'description': ['Biotech developing cancer therapies', 'Restaurant chain']
    ... })
    >>> df_with_predictions = generate_health_relevance_predictions(df, 'description', 'uuid')
    >>> print(df_with_predictions[['uuid', 'prediction', 'probability']])
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

