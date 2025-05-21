
from multiprocessing.pool import ThreadPool
from dotenv import load_dotenv
import json
import logging
import math
import os
from pathlib import Path
import re
import time

from more_itertools import chunked
import jsonlines
from openai import OpenAI
import pandas as pd
import tiktoken
from urllib3.exceptions import ReadTimeoutError

from vdl_tools.shared_tools.tools.logger import logger

logging.getLogger("openai").setLevel(logging.WARNING)

load_dotenv()
ATTEMPT_COUNT = 10

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
# get the fine-tuned model name
cb_ft_model = "ft:gpt-3.5-turbo-1106:vibrant-data-labs:prompt-climate-v2:8dkgn7K4"
cd_ft_model = "ft:gpt-3.5-turbo-1106:vibrant-data-labs:prompt-climate-cd:8syuyqk3"
cb_cd_model_4omini = 'ft:gpt-4o-mini-2024-07-18:vibrant-data-labs:cb-cd-just-conservative:9x1MMlJz'
cb_cd_model_4omini_tailwind = "ft:gpt-4o-mini-2024-07-18:vibrant-data-labs:cb-cd-tw-airtable:A9l2oRot"

# process text
# eliminate extra spaces
# truncate entries longer than max tokens (or chunking?)

EMBEDDING_CTX_LENGTH = 16000
EMBEDDING_ENCODING = "cl100k_base"

DEFAULT_SYSTEM_PROMPT = "You are a climate change expert."
DEFAULT_PROMPT_FORMAT = (
    "Categorize the following company descriptions as either pertinent (1) or irrelevant (0) to addressing the climate crisis: {text} -> \n#"
)


def replace_returns_with_spaces(text: str):
    """output_text = replace_returns_with_spaces(input_text)
    skip cells without descriptions
    print(output_text)"""
    if not isinstance(text, str):
        return " "

    text = re.sub(r"//", " ", text)  # Replace double slashes with spaces
    text = re.sub(r"\n+", " ", text)
    return text


def truncate_text_tokens(
        text: str, encoding_name=EMBEDDING_ENCODING, max_tokens=EMBEDDING_CTX_LENGTH
):
    """Truncate a string to have `max_tokens` according to the given encoding."""
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.encode(text)[:max_tokens]


def num_tokens_from_string(text: str, encoding_name=EMBEDDING_ENCODING):
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(text))
    return num_tokens


def decode_tokens(tok_list: list, encoding_name=EMBEDDING_ENCODING):
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.decode(tok_list)


def get_model_pred(
    text: str,
    model: str,
    system_prompt: str = None,
    prompt_format: str = None,
):
    system_prompt = system_prompt or DEFAULT_SYSTEM_PROMPT
    prompt_format = prompt_format or DEFAULT_PROMPT_FORMAT

    text = prompt_format.format(text=text)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        logprobs=True,
        top_logprobs=1
    )
    # check if the response is "1" or "0", if not, return probs as None
    # return the response as int and the probability
    pred_out = response.choices[0].message.content.strip()
    try:
        int(float(pred_out))
        pred_out = int(float(pred_out))
    except ValueError:
        logger.error("this is not a valid decimal number")
    if pred_out not in [1, 0]:
        return [pred_out, None]
    else:
        pred_out = int(pred_out)
        probs = math.exp(response.choices[0].logprobs.content[0].logprob)
    return [pred_out, round(probs, 3)]


def process_get_pred(text: str, model, system_prompt=None, prompt_format=None):
    in_text = replace_returns_with_spaces(text)
    out_list = truncate_text_tokens(
        in_text, encoding_name=EMBEDDING_ENCODING, max_tokens=EMBEDDING_CTX_LENGTH
    )
    out_text = decode_tokens(out_list, encoding_name=EMBEDDING_ENCODING)
    return get_model_pred(out_text, model, system_prompt, prompt_format)


def generate_predictions(
    df,
    chunk_size: int,
    column_text: str,
    save_path: str,
    model,
    idn: str,
    max_workers=8,
    label_override_filepath: str = None,
    use_cached_results: bool = True,
    system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    prompt_format: str = DEFAULT_PROMPT_FORMAT,
):
    """Generate predictions for a dataframe using a model and save them to a JSON file.
    
    Args:
        df (pd.DataFrame): The input dataframe containing the data to predict.
        chunk_size (int): The number of rows to process in each chunk.
        column_text (str): The name of the column containing the text to predict.
        save_path (str): The path to save the predictions as a JSON file.
        model: The model to use for generating predictions.
        idn (str): The name of the column containing unique identifiers for each row.
        max_workers (int, optional): The maximum number of worker threads to use. Defaults to 8.
        label_override_filepath (str, optional): Path to a file containing label overrides. Defaults to None.
        use_cached_results (bool, optional): Whether to use cached results if available. Defaults to True.
        system_prompt (str, optional): The system prompt to provide context for the model. Defaults to DEFAULT_SYSTEM_PROMPT.
        prompt_format (str, optional): The format string for the user prompt. Defaults to DEFAULT_PROMPT_FORMAT.
    
    Returns:
        dict: A dictionary containing predictions for each row in the dataframe.
    """
    save_path = Path(save_path)
    if save_path.exists() and use_cached_results:
        with jsonlines.open(save_path) as file:
            # Note using the uuid as the name of the id key--doesn't need to match idn
            predictions = {line["uuid"]: line["prediction"] for line in file}
            preds_ids = predictions.keys()

        df = df.copy()[~df[idn].isin(preds_ids)]

    else:
        predictions = {}  # Dictionary to store predictions


    to_predict = df[[idn, column_text]].values.tolist()
    # Loop through chunks of the dataframe
    for i, chunk in enumerate(chunked(to_predict, chunk_size)):

        ids, texts = zip(*chunk)
        to_run = [(text, model, system_prompt, prompt_format) for text in texts]
        n_run = i * chunk_size
        logger.info(
            "Running GPT Relevance on chunks %s - %s out of %s",
            n_run, n_run + chunk_size, len(to_predict)
        )
        try:
            with ThreadPool(processes=max_workers) as executor:
                results = list(executor.starmap(process_get_pred, to_run))

        except KeyboardInterrupt:
            logger.warn("Received KeyboardInterrupt, returning the currently scraped data...")
            break

        with jsonlines.open(save_path, mode="a") as file:
            for id_, response in zip(ids, results):
                # Save the prediction to the JSON file
                # Do it like this to ensure we don't have multiple entries for the same id
                predictions[id_] = response
                file.write({"uuid": id_, "prediction": response})

    if label_override_filepath:
        label_override = json.load(open(label_override_filepath))
        for id_, label in label_override.items():
            predictions[id_] = [label, None]
    return predictions


def send_request(df, chunk_size, column_text, save_path, model, idn="uid"):
    for i in range(ATTEMPT_COUNT):
        try:
            res = generate_predictions(df, chunk_size, column_text, save_path, model, idn=idn)
            return res
        except ReadTimeoutError as rte:
            if i == ATTEMPT_COUNT - 1:
                logger.error(f"Request failed, caught read timeout. Exiting")
                raise rte
            logger.info(f"Attempt: {i + 1}, caught read timeout, waiting...")
            time.sleep(2 + (10 - ATTEMPT_COUNT) ^ 2)


if __name__ == "__main__":
    df_path = "~/Workspace/VDL/shared-data/data/crunchbase/2023_07_20/organizations_search_terms2023Q1_sample_1000.xlsx"
    df_cft = pd.read_excel(df_path, engine="openpyxl")
    df_cft = df_cft[:20]
    column_text = "short_description"
    predictions = generate_predictions(
        df_cft,
        5,
        column_text,
        save_path="./preds_test.json",
        model=cb_ft_model,
        idn="uuid",
    )

    # map the 2 cols it to the dataframe
    df_cft['prediction'], df_cft['probability'] = zip(*df_cft['uuid'].map(predictions))
    # if the predictions uuid is a subset of the dataset, then map it with an exemption
    # df_cft['prediction'], df_cft['probability'] = zip(
    #    *df_cft['uuid'].map(lambda x: predictions.get(x, (None, None))))

    # to look at the ones with errors
    df_cft_errors = df_cft[~df_cft["prediction"].isin([0, 1])].copy()

    # to get the climate relevant predictions
    df_cft = df_cft[df_cft["prediction"] == 1].copy()

    # if you want to add a column with text length
    df_cft["len_text"] = df_cft[column_text].apply(
        lambda x: num_tokens_from_string(x, encoding_name=EMBEDDING_ENCODING)
    )
