import contextlib
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


# Models tiktoken does not recognize yet, already warned about. Counting runs
# per text, so without this the warning repeats once per call.
_ENCODING_WARNED: set[str] = set()


def get_num_tokens(text, model_name):
    """Return the number of tokens used by a text."""
    try:
        encoding = tiktoken.encoding_for_model(model_name)
    except KeyError:
        # o200k_base is the right encoding for the GPT-4o/5-era models, so the
        # count is correct — tiktoken's table just lags new releases. Warn once
        # per model rather than per call.
        if model_name not in _ENCODING_WARNED:
            _ENCODING_WARNED.add(model_name)
            print(
                f"Note: tiktoken has no encoding registered for {model_name}; "
                f"using o200k_base."
            )
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
        f"Unknown model {model_name}, falling back to default context window {DEFAULT_CONTEXT_WINDOW}"
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


@contextlib.contextmanager
def spy_openai(send: bool = True, printer=print, show_response: bool = True):
    """Temporarily print every ``CLIENT.responses.parse`` call's payload.

    Wrap any code that ends up calling the Responses API — ``get_completion``,
    or the taxonomy-mapping / judge / coherence caches (which all funnel
    through it) — and this prints the full request each call builds: the
    ``model``, every extra kwarg (``temperature``, ``reasoning``, ...), the
    ``text_format`` response class, and the ``input`` messages (or the bare
    user string + ``instructions`` for reasoning models). The patch is
    restored on exit.

    Parameters
    ----------
    send : bool
        ``True`` (default): print the request, call the real API, and
        (if ``show_response``) print ``response.output_parsed``.
        ``False``: print the request and raise *before* sending — no API
        call. Use this with a single call; inside a multi-call loop the
        raise is caught per call (recorded as an error) and the loop
        continues, so you'd see every request but get no results.
    printer : callable
        Where to send output. Defaults to ``print``; pass e.g.
        ``logger.info`` to route elsewhere.
    show_response : bool
        When ``send=True``, also print the parsed response. Default True.

    Examples
    --------
    >>> from vdl_tools.shared_tools.openai.openai_api_utils import (
    ...     get_completion, spy_openai,
    ... )
    >>> with spy_openai():
    ...     get_completion(prompt="...", model="gpt-4.1", text="...",
    ...                    text_format=MySchema)

    >>> # see the request without spending a call:
    >>> with spy_openai(send=False):
    ...     try:
    ...         cache.get_cache_or_run(given_id="x", text="...",
    ...                                read_from_cache=False, write_to_cache=False)
    ...     except RuntimeError:
    ...         pass
    """
    orig = CLIENT.responses.parse
    # ``parse`` is normally a class method (not an instance attribute), so
    # restoring means deleting the instance-attribute shadow we add below
    # — not reassigning, which would leave a stale bound method behind.
    had_own_parse = "parse" in vars(CLIENT.responses)

    def _wrapper(*args, **kwargs):
        lines = ["", "=" * 72, "OpenAI responses.parse — request", "=" * 72]
        lines.append(f"model       : {kwargs.get('model')}")
        for k, v in kwargs.items():
            if k in ("model", "input", "instructions", "text_format"):
                continue
            lines.append(f"{k:<12}: {v!r}")
        tf = kwargs.get("text_format")
        lines.append(f"text_format : {getattr(tf, '__name__', tf)}")
        if args:
            lines.append(f"(positional args: {args!r})")

        if kwargs.get("instructions") is not None:
            lines.append("\n--- instructions ---")
            lines.append(str(kwargs["instructions"]))

        inp = kwargs.get("input")
        if isinstance(inp, list):
            for m in inp:
                if isinstance(m, dict):
                    lines.append(f"\n--- input[{m.get('role', '?')}] ---")
                    lines.append(str(m.get("content", m)))
                else:
                    lines.append("\n--- input[?] ---")
                    lines.append(str(m))
        elif inp is not None:
            lines.append("\n--- input ---")
            lines.append(str(inp))
        lines.append("=" * 72)
        printer("\n".join(lines))

        if not send:
            raise RuntimeError("spy_openai(send=False): stopped before sending")

        resp = orig(*args, **kwargs)
        if show_response:
            printer(f"--- response.output_parsed ---\n"
                    f"{getattr(resp, 'output_parsed', resp)}\n{'=' * 72}")
        return resp

    CLIENT.responses.parse = _wrapper
    try:
        yield
    finally:
        if had_own_parse:
            CLIENT.responses.parse = orig
        else:
            # Remove our instance-attribute shadow so the original class
            # method shows through again.
            CLIENT.responses.__dict__.pop("parse", None)


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
