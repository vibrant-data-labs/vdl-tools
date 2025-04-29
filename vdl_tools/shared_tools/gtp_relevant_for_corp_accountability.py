import pandas as pd
import openai
import os
from dotenv import load_dotenv
import re
import tiktoken
import time
import json

load_dotenv()
openai.organization = "org-sffiCNqfxG2z4aOrj4G8lhPx"
openai.api_key = os.environ['OPENAI_API_KEY']
# get the fine-tuned model name

gca_ft_model = "ft:babbage-002:vibrant-data-labs::81NFADDK"
#process text
#eliminate extra spaces
#truncate entries longer than max tokens (or chunking?)
EMBEDDING_CTX_LENGTH = 3900
EMBEDDING_ENCODING = "cl100k_base"

def replace_returns_with_spaces(text):
    """output_text = replace_returns_with_spaces(input_text)
    print(output_text)"""
    re.sub(r'//', ' ', text)
    return re.sub(r'\n+', ' ', text)

def truncate_text_tokens(text, encoding_name=EMBEDDING_ENCODING, max_tokens=EMBEDDING_CTX_LENGTH):
    """Truncate a string to have `max_tokens` according to the given encoding."""
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.encode(text)[:max_tokens]

def num_tokens_from_string(text, encoding_name=EMBEDDING_ENCODING):
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(text))
    return num_tokens

def decode_tokens(tok_list, encoding_name=EMBEDDING_ENCODING):
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.decode(tok_list)

def get_model_pred(text, model):
    response = openai.Completion.create(model=model,
                               prompt=text + " ->"+'\n#',#''\n\n###\n\n',
                               max_tokens=1, temperature=0,logprobs=2, stop="\n",
                                    top_p=1,
                                    frequency_penalty=0,
                                    presence_penalty=0,
                                    )
    time.sleep(1)
    return response['choices'][0]['text']


def process_get_pred(text, model):
    in_text = replace_returns_with_spaces(text)
    out_list = truncate_text_tokens(in_text, encoding_name=EMBEDDING_ENCODING, max_tokens=EMBEDDING_CTX_LENGTH)
    out_text = decode_tokens(out_list, encoding_name=EMBEDDING_ENCODING)
    return get_model_pred(out_text, model)


def generate_predictions(df, chunk_size, column_text, save_path, model):
    import json
    if save_path.exists():
        with open(save_path) as file:
            preds_test = file.read()
            predictions = json.loads(preds_test)
            preds_ids = predictions.keys()
        df = df.copy()[~df.id.isin(preds_ids)]

    else:
        predictions = {}  # Dictionary to store predictions

    # Loop through chunks of the dataframe
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]  # Get the current chunk

        # Generate predictions for the chunk using the model
        chunk_predictions = chunk[column_text].apply(lambda x: process_get_pred(x, model))

        # Map the predictions to their corresponding indices and store in the dictionary
        for index, prediction in zip(chunk.id, chunk_predictions):
            predictions[index] = prediction
        # Save the predictions as a JSON file
        print('\n Saving predictions', len(predictions)-chunk_size, "to", len(predictions) )
        with open(save_path, 'w') as f:
                json.dump(predictions, f)

    return predictions


import time
from urllib3.exceptions import ReadTimeoutError

ATTEMPT_COUNT = 10
def send_request(df, chunk_size, column_text, save_path, model):
    for i in range(ATTEMPT_COUNT):
        try:
            res = generate_predictions(df, chunk_size, column_text, save_path, model)
            return res
        except ReadTimeoutError as rte:
            if i == ATTEMPT_COUNT - 1:
                print(f'Request failed, caught read timeout. Exiting')
                raise rte
            print(f'Attempt: {i + 1}, caught read timeout, waiting...')
            time.sleep(2+(10-ATTEMPT_COUNT)^2)

if __name__ == "__main__":
    df_path = '/Users/larareichmann/Workspace/VDL/climate-landscape/climate_landscape/common/data/crunchbase/organizations_search_terms2023Q1_sample_1000.xlsx'
    df_cft = pd.read_excel(df_path, engine='openpyxl')
    df_cft = df_cft[:10]
    column_text = 'short_description'
    predictions = generate_predictions(df_cft, 3, column_text, '/Users/larareichmann/Workspace/VDL/climate-landscape/data/preds_test.json', model = gca_ft_model)

    df_cft['predictions'] = df_cft[column_text].apply(lambda x: process_get_pred(x))
    df_cft['len_text'] = df_cft[column_text].apply(lambda x: num_tokens_from_string(x, encoding_name=EMBEDDING_ENCODING))
    print(predictions, df_cft['predictions'])

