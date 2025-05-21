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


def num_tokens_from_messages(messages, model="gpt-4o-mini-2024-07-18"):
    """Return the number of tokens used by a list of messages.
    https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: model not found. Using o200k_base encoding.")
        encoding = tiktoken.get_encoding("o200k_base")
    if model in {
        "gpt-3.5-turbo-0125",
        "gpt-4-0314",
        "gpt-4-32k-0314",
        "gpt-4-0613",
        "gpt-4-32k-0613",
        "gpt-4o-mini-2024-07-18",
        "gpt-4o-2024-08-06"
        }:
        tokens_per_message = 3
        tokens_per_name = 1
    elif "gpt-3.5-turbo" in model:
        print("Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0125.")
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0125")
    elif "gpt-4o-mini" in model:
        print("Warning: gpt-4o-mini may update over time. Returning num tokens assuming gpt-4o-mini-2024-07-18.")
        return num_tokens_from_messages(messages, model="gpt-4o-mini-2024-07-18")
    elif "gpt-4o" in model:
        print("Warning: gpt-4o and gpt-4o-mini may update over time. Returning num tokens assuming gpt-4o-2024-08-06.")
        return num_tokens_from_messages(messages, model="gpt-4o-2024-08-06")
    elif "gpt-4" in model:
        print("Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.")
        return num_tokens_from_messages(messages, model="gpt-4-0613")
    else:
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not implemented for model {model}."""
        )
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
    return num_tokens


def _get_completion_kwargs(
    prompt,
    model,
    text,
    messages=None,
    temperature=0.2,
    max_tokens=2000,
    top_p=1,
    seed=SEED,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    response_format_type="text",
):

    model_name = MODEL_DATA[model]["model_name"]
    messages = messages or []
    if not messages and prompt:
        messages = [
            {
                "role": "system",
                "content": prompt,
            },
        ]

    messages.append({"role": "user", "content": text})


    kwargs = {
        "model": model_name,
        "temperature": temperature,
        "messages": messages,
        "max_tokens": max_tokens,
        "top_p": top_p,
        "seed": seed,
        "frequency_penalty": frequency_penalty,
        "presence_penalty": presence_penalty,
        "stop": stop,
        "response_format": {"type": response_format_type},
    }

    if model_name == "o3-mini":
        max_tokens = kwargs.pop("max_tokens")
        kwargs["max_completion_tokens"] = max_tokens

    return kwargs


async def get_completion_async(
    prompt,
    model,
    text,
    messages=None,
    temperature=0.2,
    max_tokens=2000,
    top_p=1,
    seed=SEED,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    response_format_type="text",
    return_all=True,
    dry_run=False,
):
    kwargs = _get_completion_kwargs(
        prompt,
        model,
        text,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
        seed=seed,
        frequency_penalty=frequency_penalty,
        presence_penalty=presence_penalty,
        stop=stop,
        response_format_type=response_format_type,
    )
    if dry_run:
        return kwargs

    completion = await ASYNC_CLIENT.chat.completions.create(**kwargs)
    if return_all:
        return completion
    return completion.choices[0].message.content


def get_completion(
    prompt,
    model,
    text,
    messages=None,
    temperature=0.2,
    max_tokens=2000,
    top_p=1,
    seed=SEED,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None,
    return_all=True,
    verbose=False,
    response_format_type="text",
):
    if not verbose:
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("openai").setLevel(logging.WARNING)

    kwargs = _get_completion_kwargs(
        prompt,
        model,
        text,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
        seed=seed,
        frequency_penalty=frequency_penalty,
        presence_penalty=presence_penalty,
        stop=stop,
    response_format_type=response_format_type
    )

    completion = CLIENT.chat.completions.create(**kwargs)
    if return_all:
        return completion
    return completion.choices[0].message.content



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
