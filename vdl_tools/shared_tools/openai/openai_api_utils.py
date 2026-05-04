import logging

from openai import AsyncOpenAI, OpenAI
import tiktoken
import requests
import os
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA, SEED
from vdl_tools.shared_tools.tools.config_utils import get_configuration


logging.basicConfig(level=logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    api_key = get_configuration()['openai']["openai_api_key"]

CLIENT = OpenAI(max_retries=4,
                api_key=api_key)
ASYNC_CLIENT = AsyncOpenAI(api_key=api_key)
DEFAULT_CONTEXT_WINDOW = 400_000
DEFAULT_MAX_OUTPUT_TOKENS = 128_000

def is_reasoning_model(model):
    return model.startswith("gpt-5")


def get_num_tokens(text, model_name):
    """Return the number of tokens used by a text."""
    try:
        encoding = tiktoken.encoding_for_model(model_name)
    except KeyError:
        print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("o200k_base")
    # Ensure the input is a string (or handle None separately)
    if text is None:
        # Option 1: Return 0 for None
        return 0
        # Option 2: text = ""  # or convert to empty string
    elif not isinstance(text, str):
        text = str(text)

    return len(encoding.encode(text))


def truncate_text(text, model_name, max_tokens):
    """Truncate text to at most max_tokens using the model-appropriate encoding."""
    if not isinstance(text, str):
        return text
    try:
        encoding = tiktoken.encoding_for_model(model_name)
    except KeyError:
        encoding = tiktoken.get_encoding("o200k_base")
    tokens = encoding.encode(text)
    if len(tokens) <= max_tokens:
        return text
    return encoding.decode(tokens[:max_tokens])


def get_context_window(model_name):
    """Return (max_context_window, max_output_tokens) for a model.

    Handles fine-tuned models by matching their base model name.
    Returns (None, None) if the model is unknown.
    """
    if model_name in MODEL_DATA:
        d = MODEL_DATA[model_name]
        return d["max_context_window"], d["max_output_tokens"]
    if model_name.startswith("ft:"):
        base = model_name.split(":")[1]  # e.g. "gpt-4.1-mini-2025-04-14"
        for key, d in MODEL_DATA.items():
            if base.startswith(key):
                return d["max_context_window"], d["max_output_tokens"]
    print(
        "Unknown model %s, falling back to default context window %d",
        model_name,
        DEFAULT_CONTEXT_WINDOW
    )
    return DEFAULT_CONTEXT_WINDOW, DEFAULT_MAX_OUTPUT_TOKENS



def num_tokens_from_messages(messages, model="gpt-4o-mini-2024-07-18"):
    """Return the number of tokens used by a list of messages.
    https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: model not found. Using o200k_base encoding.")
        encoding = tiktoken.get_encoding("o200k_base")
    tokens_per_message = 3
    tokens_per_name = 1
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
    return num_tokens


def get_completion(
    prompt,
    model,
    text,
    return_all=False,
    max_text_tokens=None,
    **kwargs,
):
    """Call the OpenAI Responses API and return the parsed completion.

    Uses `CLIENT.responses.parse()` with the given prompt and user text.
    By default returns only the parsed output; if `return_all` is True,
    returns the full response object.

    Notes
    -----
    **Reasoning vs non-reasoning models**

    - **Reasoning models** (identified by `is_reasoning_model`: model name
      starts with "gpt-5") use the Responses API in instruction mode:
      the prompt is passed as `instructions` and only the latest user
      message is passed as `input`. This matches the expected format
      for reasoning/agent-style models.
    - **Non-reasoning models** receive the full conversation: the prompt
      as a system message and the user text as a user message, both
      passed together as `input` (messages list). No separate
      `instructions` field is used.

    Parameters
    ----------
    prompt : str
        System prompt or instructions for the model.
    model : str
        OpenAI model name (e.g. gpt-4o-mini, gpt-5-*).
    text : str
        User message content.
    return_all : bool, optional
        If True, return the full API response; otherwise return only
        `response.output_parsed`. Default is False.
    **kwargs
        Passed through to `responses.parse()` (e.g. response_format).

    Returns
    -------
    Parsed completion, or the full response object if `return_all` is True.
    """
    if max_text_tokens is not None:
        text = truncate_text(text, model_name=model, max_tokens=max_text_tokens)

    messages = [
        {
            "role": "system",
            "content": prompt,
        },
        {
            "role": "user",
            "content": text,
        },
    ]

    response_kwargs = {
        "model": model,
        "input": messages,
        **kwargs,
    }

    if is_reasoning_model(model):
        # Make the prompt_str the instructions
        response_kwargs["instructions"] = prompt
        last_message = messages[-1]['content']
        response_kwargs['input'] = last_message

    response = CLIENT.responses.parse(**response_kwargs)
    if return_all:
        return response
    return response.output_parsed



def contains_i_am(x):
    """Checks if the text contains "i am" or "i'm" which means the LLM is talking about themselves and is likely apologizing
    for not being able to generate a good summary.

    Examples
    [
        "I'm sorry, but the text provided does not contain relevant....",
        "I am unable to provide a summary as the text provided seems to be from a restricted page that requires login credentials.",
        "I'm sorry, but it seems like the provided URL does not contain...."
    ]
    """
    return "i am" in x.lower() or "i'm" in x.lower()


def get_embedding_response(
    texts,
    model_name: str,
):
    response = CLIENT.embeddings.create(
        input=texts,
        model=model_name,
    )
    return response


def get_embedding_response_nomic(
    texts: list[str],
    truss_api_key=None,
    dimensionality:int =768,
    task_type:str = 'classification',
):
    resp = requests.post(
            "https://model-2qjl9xlw.api.baseten.co/production/predict",
            headers={"Authorization": f"Api-Key {truss_api_key}"},
            json={
                'texts': texts,
                'dimensionality': dimensionality,
                'task_type': task_type,
            },
            timeout=60,
        )
    return resp.json()
