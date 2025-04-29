import requests

import pandas as pd
from datasets import Dataset
import torch

import vdl_tools.shared_tools.s3_model as s3m


import json


ADAPTATION_MODEL_2023_ID = "7qk99z93"
CPI_ADAPTATION_MODEL_2024_ID = "owp290lq"
TW_ADAPTATION_MODEL_2024_ID = "5wold9vw"
BASE_API_URL = "https://model-{model_id}.api.baseten.co/production/predict"


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