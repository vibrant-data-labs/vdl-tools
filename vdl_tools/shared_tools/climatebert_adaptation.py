import requests

import pandas as pd
from datasets import Dataset
from more_itertools import chunked
import torch

import vdl_tools.shared_tools.s3_model as s3m
from vdl_tools.shared_tools.database_cache.database_models.prompt import PromptResponse
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger


import json


ADAPTATION_MODEL_2023_ID = "7qk99z93"
CPI_ADAPTATION_MODEL_2024_ID = "owp290lq"
TW_ADAPTATION_MODEL_2024_ID = "5wold9vw"
BASE_API_URL = "https://model-{model_id}.api.baseten.co/production/predict"

# Stable pseudo-prompt: the ClimateBERT classifier has no prompt, but the cache
# keys rows by (prompt_id, given_id, text_id, model_name). Do not edit this
# string — changing it changes prompt_id and orphans every cached prediction.
ADAPTATION_PROMPT_STR = (
    "ClimateBERT adaptation/mitigation classifier hosted on Baseten. "
    "Classifies organization text as adaptation, mitigation, or both."
)


def send_chunk_to_remote_model(
    chunk,
    id_col,
    column_text,
    api_key,
    model_id=ADAPTATION_MODEL_2023_ID,
):
    """
    Send a chunk of data to the model and return the predictions
    """
    
    # Baseten model expects data to be formatted as a list of dictionaries
    # With the keys "id" and "text"
    chunk_dataset = (
        chunk[[id_col, column_text]]
        .rename(
            columns={id_col: "id", column_text: "text"},
        ).to_dict(orient="records")
    )

    resp = requests.post(
        BASE_API_URL.format(model_id=model_id),
        headers={"Authorization": f"Api-Key {api_key}"},
        json=chunk_dataset,
        timeout=120,
    )
    if not resp.ok:
        raise Exception(f"Request failed with status code {resp.status_code}\n{resp.text}")
    return resp.json()


def generate_predictions_adapt_mit_remote(
    df: pd.DataFrame,
    chunk_size: int,
    id_col: str,
    column_text: str,
    save_path,
    api_key: str,
    model_id=CPI_ADAPTATION_MODEL_2024_ID,
):
    """
    df
    chunk size: how many outputs to save in each loop in case there's an issue or new rows are added
    column text: column with concatenated description text to evaluate
    id col: column with the "permanent" org id (from the database)
    save path: where to save json file with predictions - that can be updated without running all predictions, it will
    just find the new ones
    model path: where the model is hosted
    returns a df with adaptation/mitigation prediction column

    """
    if save_path.exists():
        with open(save_path) as file:
            preds_saved = file.read()
            predictions = json.loads(preds_saved)
            preds_ids = predictions.keys()
        df = df.copy()[~df[id_col].isin(preds_ids)]
        if len(df) == 0:
            return predictions
    else:
        predictions = {}  # Dictionary to store predictions

    # Loop through chunks of the dataframe
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]  # Get the current chunk
        chunk_predictions = send_chunk_to_remote_model(
            chunk,
            id_col,
            column_text,
            api_key,
            model_id=model_id,
        )

    # Map the predictions to their corresponding indices and store in the dictionary
        for index, prediction in zip(chunk[id_col], chunk_predictions):
            predictions[index] = prediction
        # Save the predictions as a JSON file
        print('\n Saving predictions', len(predictions)-chunk_size, "to", len(predictions) )
        # make directory if it's not there
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'w') as f:
                json.dump(predictions, f)

    return predictions


class _BasetenPrediction:
    """Duck-types the OpenAI response object that `_build_success_row` expects
    (`model_dump_json()` + `output_text`) around a raw Baseten prediction."""

    def __init__(self, prediction):
        self.prediction = prediction
        self.output_text = (
            prediction if isinstance(prediction, str) else json.dumps(prediction)
        )

    def model_dump_json(self):
        return json.dumps({"prediction": self.prediction})


class AdaptationMitigationCache(PromptResponseCacheSQL):
    """SQL-backed cache for the Baseten ClimateBERT adaptation/mitigation model.

    Rows are keyed by (prompt_id, given_id, text_id, model_name) with
    model_name set to the Baseten model id and ``filter_by_model=True``, so
    predictions from e.g. the CPI and TW model deployments never collide and
    re-running an org whose text changed re-predicts just that org.

    Unlike the OpenAI caches this one keeps Baseten's batch semantics: cache
    misses are sent in chunks to the model endpoint rather than one request
    per text.
    """

    def __init__(
        self,
        session,
        api_key: str,
        model_id: str = CPI_ADAPTATION_MODEL_2024_ID,
    ):
        self.api_key = api_key
        super().__init__(
            session=session,
            prompt_str=ADAPTATION_PROMPT_STR,
            prompt_name="climatebert_adaptation_mitigation",
            prompt_description=(
                "Fine-tuned ClimateBERT classifier (Baseten-hosted) labeling "
                "org text as adaptation / mitigation / both."
            ),
            model=model_id,
            filter_by_model=True,
        )

    def get_completion(self, prompt_str, text, **kwargs):
        """Single-text path (used by `get_cache_or_run`): a batch of one."""
        return _BasetenPrediction(self._post_batch([("0", text)])[0])

    def _post_batch(self, given_ids_texts: list[tuple[str, str]]) -> list:
        payload = [{"id": g, "text": t} for g, t in given_ids_texts]
        resp = requests.post(
            BASE_API_URL.format(model_id=self.model),
            headers={"Authorization": f"Api-Key {self.api_key}"},
            json=payload,
            timeout=120,
        )
        if not resp.ok:
            raise Exception(
                f"Request failed with status code {resp.status_code}\n{resp.text}"
            )
        predictions = resp.json()
        if len(predictions) != len(payload):
            raise Exception(
                f"Baseten returned {len(predictions)} predictions "
                f"for {len(payload)} inputs"
            )
        return predictions

    @staticmethod
    def _extract_prediction(response_full, response_text):
        """Recover the original prediction value from a cached row."""
        try:
            full = (
                response_full
                if isinstance(response_full, dict)
                else json.loads(response_full)
            )
            return full["prediction"]
        except (TypeError, ValueError, KeyError):
            return response_text

    def bulk_predict(
        self,
        given_ids_texts: list[tuple[str, str]],
        chunk_size: int = 50,
        read_from_cache: bool = True,
        max_errors: int = 1,
    ) -> dict:
        """Return {given_id: prediction}, running the model only on cache misses.

        Misses are sent to Baseten in chunks of ``chunk_size``; each chunk is
        upserted and committed before the next, so an interrupted run resumes
        from the cache (same recovery property the JSON file gave us).
        """
        given_ids_texts = list({(str(g), t) for g, t in given_ids_texts})
        requested_given_ids = {g for g, _ in given_ids_texts}

        res = {}
        if read_from_cache:
            found_rows, unfound_ids_errors = self.get_prompt_response_obj_bulk(
                given_ids_texts, request_kwargs={},
            )
            res = {
                row.given_id: self._extract_prediction(row.response_full, row.response_text)
                for row in found_rows
            }
            unfound = []
            for given_id, text in given_ids_texts:
                text_id = PromptResponse.create_text_id(text)
                errors_for_id = unfound_ids_errors.get((given_id, text_id), 0)
                if (
                    (given_id, text_id) in unfound_ids_errors
                    and (errors_for_id == 0 or errors_for_id < max_errors)
                ):
                    unfound.append((given_id, text))
        else:
            unfound = given_ids_texts

        logger.info(
            "Adaptation/mitigation: %s cached, %s to run", len(res), len(unfound)
        )

        for chunk in chunked(unfound, chunk_size):
            try:
                predictions = self._post_batch(chunk)
            except Exception as ex:
                logger.error("Baseten chunk failed: %s", ex)
                self._upsert_error_rows([
                    self._build_error_row(
                        given_id, text, {"message": str(ex)}, request_kwargs={},
                    )
                    for given_id, text in chunk
                ])
                self.session.commit()
                continue

            success_rows = [
                self._build_success_row(
                    given_id, text, _BasetenPrediction(prediction), request_kwargs={},
                )
                for (given_id, text), prediction in zip(chunk, predictions)
            ]
            self._upsert_success_rows(success_rows)
            self.session.commit()
            for (given_id, _), prediction in zip(chunk, predictions):
                res[given_id] = prediction
            logger.info("Saved predictions %s to %s", len(res) - len(chunk), len(res))

        # The text_id-based lookup can match rows cached under other given_ids
        # (identical text on two orgs) — return only what was asked for.
        return {g: res[g] for g in requested_given_ids if g in res}


def generate_predictions_adapt_mit_remote_sql(
    df: pd.DataFrame,
    chunk_size: int,
    id_col: str,
    column_text: str,
    api_key: str,
    model_id=CPI_ADAPTATION_MODEL_2024_ID,
    session=None,
    read_from_cache: bool = True,
):
    """SQL-cached replacement for `generate_predictions_adapt_mit_remote`.

    Same signature minus ``save_path``: predictions live in the
    prompt_response table (keyed by text hash and Baseten model id) instead of
    a local JSON file, so a changed description re-predicts automatically and
    results are shared across machines. Returns {id: prediction} as before.
    """
    given_ids_texts = list(df[[id_col, column_text]].itertuples(index=False, name=None))
    with get_session(session=session) as session:
        cache = AdaptationMitigationCache(
            session=session,
            api_key=api_key,
            model_id=model_id,
        )
        return cache.bulk_predict(
            given_ids_texts,
            chunk_size=chunk_size,
            read_from_cache=read_from_cache,
        )


def _generate_predictions_adapt_mit(df: pd.DataFrame, chunk_size: int, id_col: str, column_text: str, save_path):
    """
    df
    chunk size: how many outputs to save in each loop in case there's an issue or new rows are added
    column text: column with concatenated description text to evaluate
    id col: column with the "permanent" org id (from the database)
    save path: where to save json file with predictions - that can be updated without running all predictions, it will
    just find the new ones
    model path: where the model is hosted
    returns a df with adaptation/mitigation prediction column

    """
    if save_path.exists():
        with open(save_path) as file:
            preds_saved = file.read()
            predictions = json.loads(preds_saved)
            preds_ids = predictions.keys()
        df = df.copy()[~df[id_col].isin(preds_ids)]
        if len(df) == 0:
            return predictions
    else:
        predictions = {}  # Dictionary to store predictions


    local_model, tokenizer = s3m.load_model('cwf_adaptation')

    # Loop through chunks of the dataframe
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]  # Get the current chunk

        # Generate predictions for the chunk using the model
        chunk_dataset = Dataset.from_pandas(chunk[[id_col, column_text]])
        model_inputs = tokenizer(chunk_dataset[column_text], padding=True, return_tensors="pt", truncation=True)
        output = local_model(**model_inputs)
        representation = torch.nn.functional.softmax(output.logits, dim=-1)
        labels = [x.argmax().item() for x in representation]
        label2id = {0: 'Adaptation', 1: 'Mitigation', 2: 'Dual'}
        chunk_predictions = [label2id[x] for x in labels]

    # Map the predictions to their corresponding indices and store in the dictionary
        for index, prediction in zip(chunk[id_col], chunk_predictions):
            predictions[index] = prediction
        # Save the predictions as a JSON file
        print('\n Saving predictions', len(predictions)-chunk_size, "to", len(predictions) )
        with open(save_path, 'w') as f:
                json.dump(predictions, f)



    return predictions