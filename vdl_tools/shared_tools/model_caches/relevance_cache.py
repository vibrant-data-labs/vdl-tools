import math
from typing import Optional, Tuple, List, Dict, Any
import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion
from vdl_tools.shared_tools.tools.logger import logger


class RelevanceCache(PromptResponseCacheSQL):
    """Cache for relevance predictions using fine-tuned GPT models.

    This class extends PromptResponseCacheSQL to handle climate relevance classification
    with fine-tuned models that return binary predictions (0/1) with probability scores.
    """

    def __init__(
        self,
        session,
        model: str,
        system_prompt: str,
        prompt_format: str,
        prompt_name: str,
    ):
        """Initialize the ClimateRelevanceCache.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            Database session for caching operations.
        model : str,
            Fine-tuned model to use for predictions.
        system_prompt : str,
            System prompt for the model.
        prompt_format : str,
            Format string for the user prompt (should include {text} placeholder).
        prompt_name : str,
            Name for the prompt in the database.
        """
        self.system_prompt = system_prompt
        self.prompt_format = prompt_format

        super().__init__(
            session=session,
            prompt_str=prompt_format,  # Just use the prompt format directly
            prompt_name=prompt_name,
            model=model,
            filter_by_model=True,  # Must be true since model is very important
        )

    def get_completion(self, prompt_str: str, text: str, **kwargs) -> Any:
        """Override the base get_completion to handle fine-tuned models with logprobs.

        Parameters
        ----------
        prompt_str : str
            The prompt string (not used directly, we use our format).
        text : str
            The input text to classify.
        model : str, optional
            Model to use (defaults to self.model).
        **kwargs
            Additional arguments passed to get_completion.

        Returns
        -------
        Any
            OpenAI completion response with logprobs.
        """

        # Format the text using our prompt format
        formatted_text = self.prompt_format.format(text=text)

        # Use the updated get_completion function with logprobs support
        completion = get_completion(
            prompt=self.system_prompt,
            text=formatted_text,
            model=self.model,
            top_logprobs=0,
            include=["message.output_text.logprobs"],
            **kwargs
        )
        return completion
    def _parse_prediction_and_probability(
        self,
        pred_text: str,
        logprob: float = None
    ) -> Tuple[Optional[int], Optional[float]]:
        """Parse prediction text and logprob into prediction and probability.

        Parameters
        ----------
        pred_text : str
            The model's text output (e.g., "1" or "0").
        logprob : float, optional
            Optional logprob value for probability calculation.

        Returns
        -------
        tuple of (int or None, float or None)
            Tuple of (prediction, probability) where prediction is 0/1 or None if invalid,
            and probability is the confidence score or None if unavailable.
        """
        try:
            # Parse prediction
            pred_out = pred_text.strip()
            try:
                pred_value = int(float(pred_out))
            except ValueError:
                logger.error("Invalid prediction output: %s", pred_out)
                return None, None

            # Validate prediction
            if pred_value not in [0, 1]:
                logger.warning("Prediction not 0 or 1: %s", pred_value)
                return pred_value, None
            # Calculate probability from logprob if available
            probability = round(math.exp(logprob), 3) if logprob is not None else None
            return pred_value, probability

        except Exception as e:
            logger.error("Error parsing prediction: %s", e)
            return None, None

    def _parse_stored_response(self, result: Dict[str, Any]) -> Tuple[Optional[int], Optional[float]]:
        """Parse a stored database result to extract prediction and probability.

        Parameters
        ----------
        result : dict
            Dictionary from database containing response_text and response_full.

        Returns
        -------
        tuple of (int or None, float or None)
            Tuple of (prediction, probability).
        """
        if not result or not result.get('response_text'):
            return None, None

        # Extract logprob from stored JSON if available
        logprob = None
        if result.get('response_full'):
            try:
                response_full = json.loads(result['response_full'])
                # New Responses API shape: output[].content[].logprobs[]
                if (response_full.get('output') and
                    len(response_full['output']) > 0 and
                    response_full['output'][0].get('content') and
                    len(response_full['output'][0]['content']) > 0 and
                    response_full['output'][0]['content'][0].get('logprobs') and
                    len(response_full['output'][0]['content'][0]['logprobs']) > 0):
                    logprob = response_full['output'][0]['content'][0]['logprobs'][0]['logprob']
                # Legacy Chat Completions shape: choices[].logprobs.content[]
                elif (response_full.get('choices') and
                    len(response_full['choices']) > 0 and
                    response_full['choices'][0].get('logprobs') and
                    response_full['choices'][0]['logprobs'].get('content') and
                    len(response_full['choices'][0]['logprobs']['content']) > 0):
                    logprob = response_full['choices'][0]['logprobs']['content'][0]['logprob']
            except Exception as e:
                logger.debug("Could not extract logprob from stored response: %s", e)

        return self._parse_prediction_and_probability(result['response_text'], logprob)

    def get_relevance(
        self,
        given_id: str,
        text: str,
        use_cached_result: bool = True
    ) -> Tuple[Optional[int], Optional[float]]:
        """Get climate relevance prediction for a single text.

        Parameters
        ----------
        given_id : str
            Unique identifier for this text.
        text : str
            Text to classify.
        model : str, optional
            Model to use (defaults to self.model).
        use_cached_result : bool, default True
            Whether to use cached results.

        Returns
        -------
        tuple of (int or None, float or None)
            Tuple of (prediction, probability).
        """
        result = self.get_cache_or_run(
            given_id=given_id,
            text=text,
            use_cached_result=use_cached_result
        )
        return self._parse_stored_response(result)

    def bulk_get_relevance(
        self,
        given_ids_texts: List[Tuple[str, str]],
        use_cached_result: bool = True,
        n_per_commit: int = 50,
        max_workers: int = 3,
        max_errors: int = 1,
    ) -> Dict[str, Tuple[Optional[int], Optional[float]]]:
        """Get relevance predictions for multiple texts in bulk.

        Parameters
        ----------
        given_ids_texts : list of tuple of (str, str)
            List of (given_id, text) tuples.
        use_cached_result : bool, default True
            Whether to use cached results.
        n_per_commit : int, default 50
            Number of records per database commit.
        max_workers : int, default 3
            Number of worker threads.
        max_errors : int, default 1
            Maximum errors allowed per item.

        Returns
        -------
        dict of str to tuple of (int or None, float or None)
            Dictionary mapping given_id to (prediction, probability) tuples.
        """
        results = self.bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            use_cached_result=use_cached_result,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
        )

        return {given_id: self._parse_stored_response(result)
                for given_id, result in results.items()}


def generate_predictions(
    df: pd.DataFrame,
    column_text: str,
    idn: str,
    model: str,
    system_prompt: str,
    prompt_format: str,
    prompt_name: str,
    max_workers: int = 3,
    n_per_commit: int = 50,
    use_cached_results: bool = True,
    label_override_dict: Dict[str, int] = None,
    label_override_filepath: str = None,
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
    import pandas as pd
    df = pd.DataFrame({
        "uuid": ["org_1", "org_2"],
        "description": ["Solar panel manufacturer", "Restaurant chain"]
    })
    predictions = generate_predictions(df, 'description', 'uuid')
    print(predictions)
    # {'org_1': (1, 0.95), 'org_2': (0, 0.98)}
    """

    if 'prediction' in df.columns or 'probability' in df.columns:
        raise Exception(
            "DataFrame already has 'prediction' or 'probability' columns. "
            "Please remove/rename them before running this function."
        )

    if label_override_dict and label_override_filepath:
        raise Exception("Cannot provide both label overrides as a dict and filepath")

    # Prepare data for bulk processing
    given_ids_texts = df[[idn, column_text]].values.tolist()

    # Use provided session or create a new one
    # Create session using context manager
    with get_session(session=session) as session:
        cache = RelevanceCache(
            session=session,
            model=model,
            system_prompt=system_prompt,
            prompt_format=prompt_format,
            prompt_name=prompt_name,
        )

        # Get predictions using bulk method
        results = cache.bulk_get_relevance(
            given_ids_texts=given_ids_texts,
            use_cached_result=use_cached_results,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
        )

        # Load label overrides if provided
        if label_override_filepath:
            if not Path(label_override_filepath).exists():
                raise Exception(f"Label override filepath does not exist: {label_override_filepath}")
            label_override_dict = json.load(open(label_override_filepath))

        # Apply label overrides if provided
        if label_override_dict:
            for id_, label in label_override_dict.items():
                if id_ in results:
                    results[id_] = (label, None)  # Override with no probability

        # Add results to df
        df['prediction'] = df[idn].map(lambda x: results.get(x, (None, None))[0])
        df['probability'] = df[idn].map(lambda x: results.get(x, (None, None))[1])

        return df


if __name__ == "__main__":
    import pandas as pd
    from vdl_tools.shared_tools.model_caches.climate_relevance_cache import CB_CD_MODEL_4OMINI, DEFAULT_CLIMATE_SYSTEM_PROMPT, DEFAULT_CLIMATE_PROMPT_FORMAT
    df = pd.DataFrame({
        "uuid": ["org_1", "org_2"],
        "description": ["Solar panel manufacturer", "Restaurant chain"]
    })
    predictions = generate_predictions(
        df, 'description', 'uuid',
        model=CB_CD_MODEL_4OMINI,
        system_prompt=DEFAULT_CLIMATE_SYSTEM_PROMPT,
        prompt_format=DEFAULT_CLIMATE_PROMPT_FORMAT,
        prompt_name="climate_relevance_classification",
        max_workers=1,
        n_per_commit=50,
        use_cached_results=False,
    )
    print(predictions)