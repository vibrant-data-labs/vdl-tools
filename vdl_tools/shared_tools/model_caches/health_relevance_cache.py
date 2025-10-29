
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


DEFAULT_HEALTH_SYSTEM_PROMPT = """
You are an expert evaluator. You are classifying organizations based on whether they work on the intersection of health outcomes and environmental
drivers
"""

DEFAULT_HEALTH_PROMPT_FORMAT = """
Definitions
Relevant:
The organization clearly addresses an environmental factor that affects human health, either by:
-Explicitly describing both the environmental driver and the health link, or
-Offering a solution that clearly helps people adapt to or reduce exposure to an environmental health hazard, 
even if the environmental driver and health outcome are not stated together.

Not Relevant:
The organization does not clearly work on environmental health. This includes:
-General environmental work (e.g., carbon markets, recycling technology, clean energy) with no health linkage
-General health work (e.g., medical devices, pharmaceuticals, diagnostics, or vaccines) without any connection to environmental exposure or risk
-Work on preventing environmental events (e.g., flood levees, emissions reduction) unless they address human exposure or vulnerability
-Health innovations (including vaccines, therapeutics, or diagnostics) are Not Relevant unless they are explicitly 
linked to an environmental driver of health (e.g., air quality, water contamination, heat, disease vectors)
-Lifestyle/behavior interventions (e.g., smoking cessation, fitness apps) unless tied to environmental exposures
-Any case that is vague, peripheral, or mixed should be labeled as Not Relevant

Environmental Drivers Examples
Air pollution, water contamination, chemical exposures (lead, PFAS, pesticides), extreme heat, wildfires, flooding, droughts,
mold, noise, radiation, vector-borne disease, poor sanitation, food security, sea level rise, and indoor air quality.

Health Impact Examples
Asthma, cancer, cardiovascular disease, heat stroke, infections, allergies, neurotoxicity, 
mental health impacts from environmental stress, malnutrition, chronic disease, and premature mortality.

Description: {text}
\n
Output
label 1 if Relevant
label 0 if Not Relevant
\n
"""


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

