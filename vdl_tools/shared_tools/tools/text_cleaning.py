from collections import Counter
import re


def camel_to_snake(name):
    # This pattern matches a lowercase letter or digit followed by an uppercase letter,
    # and inserts an underscore between them.
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    # Convert the entire resulting string to lowercase
    return s1.lower()


def clean_scraped_text(
    text: str,
    clean_non_ascii: bool = True,
    replace_repetitive: bool = True,
    replace_whitespace: bool = True,
) -> str:
    """Turns whitespace other than newlines into standard spaces and deletes the
    remaining non-ascii characters (`clean_non_ascii_text`), collapses immediately
    repeated words (`replace_repetitive_text`), then collapses runs of the same
    whitespace character (`replace_repetitive_whitespace`).

    Parameters
    ----------
    text : str
        raw text

    Returns
    -------
    str
        clean text
    """
    if clean_non_ascii:
        text = clean_non_ascii_text(text)
    if replace_repetitive:
        text = replace_repetitive_text(text)
    if replace_whitespace:
        text = replace_repetitive_whitespace(text)
    return text


def clean_non_ascii_text(text: str) -> str:
    r"""Replaces every whitespace character except a newline with a standard space,
    then removes the remaining non-ascii characters.
    Then replaces 3 or more consecutive spaces with a single space.

    The spaces are replaced before the ascii encode so that non-breaking and other
    Unicode spaces keep the words on either side apart ("2017\xa0focused" becomes
    "2017 focused", not "2017focused").

    Parameters
    ----------
    text : str
        raw text

    Returns
    -------
    str
        clean text
    """
    spaced_text = re.sub(r"[^\S\n ]", " ", text)
    ascii_text = spaced_text.encode("ascii", "ignore").decode()
    return re.sub(r"([ ]{3,})", " ", ascii_text)


def replace_repetitive_text(text: str) -> str:
    pattern = re.compile(r'(\b[\w!:.@#$%^&*()-]+)(?:\s+\1)+')
    return pattern.sub(r"\1", text)


def replace_repetitive_whitespace(text: str) -> str:
    return re.sub(r'(\s)\1+', r'\1', text)


def repeated_character_counter(text, n_char_range=(2, 4)):

    counter = Counter()
    for n in range(*n_char_range):
        for i in range(len(text)-(n+1)):
            counter[text[i:i+n]] += 1
    return counter


def check_for_repeating_sequences(
    text,
    n_char_range=(2, 3),
    perc_threshold=0.20,
):
    """Last resort check for finding repeating sequences of characters in text.

    If the number of times that sequence appears divided the total length of the text is greater
    than the perc_threshold, then the text is flag as potentially having repeating sequences.

    Parameters
    ----------
    text : str
        text to check
    n_char_range : tuple, optional
        range of character sequences to check, by default (2, 4)
    perc_threshold : float, optional
        threshold for flagging text, by default 0.10

    Returns
    -------
    bool
        True if text is flagged, False otherwise
    Counter
        Counter object with the counts of the sequences
    """
    counter = repeated_character_counter(text, n_char_range=n_char_range)
    total_chars = len(text)
    most_frequent_count = counter.most_common(1)[0][1]
    most_frequent_length = len(counter.most_common(1)[0][0])
    if (most_frequent_count * most_frequent_length) / total_chars >= perc_threshold:
        return True, counter
    return False, counter
