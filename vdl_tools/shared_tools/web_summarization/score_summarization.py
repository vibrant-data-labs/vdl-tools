import re
import json

from vdl_tools.shared_tools.web_summarization.logger import logger
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion


TEST_PROMPT = (
    """You are a helpful assistant charged with answering the following questions for each piece of text.
    Make sure the response for each question is only a 1 or a 0.
    Make sure the final response is a valid Python dictionary with the original questions in tact."""
)


def make_test_completion(
    summary_text: str,
    test_model: str = "gpt-4",
    max_tokens: int = 100,
    return_all: bool = False,
    dry_run: bool = False,
):
    """Runs a chat completion for the given summary and includes the instructions for scoring the summary.

    Scores across a number of questions that have been shown to be issues in the past.

        * Is the summary not in English?
        * Does the summary contain text mentioning their website?
        * Does the summary contain copyright text?
        * Does the summary ever speak in the first person?

    Parameters
    ----------
    summary_text : str
        The summary text to be scored
    test_model : str, optional
        The OpenAI GPT model to be used for scoring, by default "gpt-4"
    max_tokens : int, optional
        Maximum number of tokens for the response, by default 100
    return_all : bool, optional
        Whether to return the entire response from OpenAI rather than just the completion text,
        by default False
    dry_run : bool, optional
        Whether to return the kwargs for the call to OpenAI completion API without running the API call,
        by default False

    Returns
    -------
    str
        The score.
    """

    message = (
        f"SUMMARY: {summary_text}\n\n"
        'RESPONSE: "{"Does the summary contain `---`?": {{1|0}}, '
        '"Is the summary not in English?":  {{1|0}}, '
        '"Does the summary contain text mentioning their website?":  {{1|0}}, '
        '"Does the summary contain copyright text?":  {{1|0}}, '
        '"Does the summary ever speak in the first person?":  {{1|0}},} '
        "\nSTOP"
    )

    return get_completion(
        prompt=TEST_PROMPT,
        model=test_model,
        text=message,
        max_tokens=max_tokens,
        frequency_penalty=.5,
        top_p=1,
        temperature=0,
        return_all=return_all,
        dry_run=dry_run,
        stop="\nSTOP",
    )


def score_summary(
    summary_text: str,
    test_model: str = "gpt-4",
    max_tokens: int = 100,
):
    """Runs the scoring of a summary including parsing responses from OpenAI.

    Parameters
    ----------
    summary_text : str
        The summary text to be scored
    test_model : str, optional
        The OpenAI GPT model to be used for scoring, by default "gpt-4"
    max_tokens : int, optional
        Maximum number of tokens for the response, by default 100

    Returns
    -------
    dict
    """
    # Regex for capturing the python dictionary with a code block string
    # Sometimes GPT will send the response like:
    # ```json{.....}```
    # ```python{.....}```
    regex_code_block = r'```(?:python|json|\w|\W)([\s\S]*?)```'

    completion_text = make_test_completion(
        summary_text=summary_text,
        test_model=test_model,
        max_tokens=max_tokens,
        return_all=False,
        dry_run=False,
    )
    if completion_text.startswith("```"):
        try:
            completion_text = re.match(
                regex_code_block,
                completion_text,
            ).groups()[0].strip()

        except Exception as e:
            logger.error(e)
            return completion_text
    try:
        scores = json.loads(completion_text)
    except Exception as e:
        scores = completion_text

    return scores
