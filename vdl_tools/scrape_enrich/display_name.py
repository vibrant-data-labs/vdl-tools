"""Choose the name to display for an organization, in readable casing.

A registry or filing name is rarely what an organization goes by: it is often in
all capitals, cut off at a field width, or carries a legal suffix, and a DBA may
be the name people actually use. This asks a model to pick one, using the
organization's website to decide, and to return it cased the way the
organization writes it.

``pick_display_names`` sends each organization's legal name, DBA, website domain,
website summary and the start of its scraped text through ``PromptResponseCacheSQL``,
by default to ``openai/gpt-oss-120b`` on the Vercel AI Gateway. It returns one
row per organization: the chosen name, which field it came from, a confidence and
a one-line reason. Organizations whose call failed, or whose answer does not
parse, are left out. The ranked rules the model follows are in ``PROMPT``. Every
column is named by the caller, so the frame can come from any source; only the
legal name is required.

Three things are fixed in code, after the model answers, because the model gets
them wrong: odd hyphens, quotes and spaces become plain ones, a legal suffix it
left on the end is dropped, and an apostrophe the scraped website text dropped
(``OBryan`` for a filed ``O'BRYAN``) is put back from the filed name.
``drop_legal_suffix`` is also what cleans the names of organizations that do not
go through the model.

For Giving Tuesday, run ``givingtuesday.name_lines.resolve_name_lines`` first, so
the legal name is the complete name and the DBA is cleaned.

``gpt-oss-120b`` was chosen over ``qwen3.8-27b`` and ``mistral-small`` on 400
Giving Tuesday orgs, 100 from each of four kinds (DBA, name joined from two lines,
all capitals, already mixed case). On the 90 where the three disagreed, judged by
hand, it was wrong least often and broke the fewest rules. Other sources have not
been measured.
"""

import re
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ValidationError

from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger

DEFAULT_MODEL = "openai/gpt-oss-120b"
DEFAULT_REASONING = {"effort": "low"}
DEFAULT_MAX_TEXT_WORDS = 1000
MAX_SIBLINGS = 8

PROMPT = """You choose the name an organization should be displayed under.

You get, when available: Legal name (the registered or filed name, often in all capitals and sometimes cut off), DBA, Website (the domain), and Website summary and Website text (the start of its scraped text).

Choose the name by these rules. When two rules conflict, the earlier one wins:
1. Never add a word that is not in the name fields, the website domain or the website text.
2. Never return an acronym on its own. An acronym inside a longer name the organization uses, such as "YMCA of Northfield", is fine.
3. Choose the name the organization uses publicly, which the website shows, over the filed name, but only if the website plausibly belongs to the same organization (shared words, the same place, or it says it was formerly or is part of the filed name). Otherwise use the filed name.
4. Use a DBA if the website shows the organization uses it, or if the website has no usable text (an error page, or nothing) and the DBA is a real name. A care-of line, a program or a person is not a name.
5. If the legal name looks cut off, complete it from words in the name fields or on the website.
6. Drop legal-form suffixes at the end of the name (Inc, Incorporated, LLC, Ltd, NFP, Corp).

Then write the chosen name with the capitalization the organization itself uses, as its website shows it, including brand casing. Keep mixed case as filed. If the filing is all capitals and the website does not show the casing, use normal capitalization: proper nouns capitalized, small words such as of and the in lowercase, acronyms such as YMCA kept. Do not leave a name in all capitals unless it is an acronym or the website writes it that way.

Examples:
- Legal name: EAST BAY PERFORMING ARTS | DBA: OAKLAND SYMPHONY | Website: www.oaklandsymphony.org -> "Oakland Symphony" (the website uses the DBA).
- Legal name: PUBLIC MEDIA NJ INC | DBA: NJPBS | Website: njpbs.org -> "NJ PBS" (the website's own spelling).
- Legal name: FAMILY SUPPORT CIRCLE INC | DBA: FSC | Website: www.familysupportcircle.org -> "Family Support Circle" (never the acronym alone).
- Legal name: YOUTH FRONTIERS INC | Website: www.youthfrontiers.org -> "Youth Frontiers" (casing fixed, suffix dropped).
- Legal name: transcenDANCE Youth Arts Project | Website: www.tdarts.org -> "transcenDANCE Youth Arts Project" (the filed casing is deliberate, so it is kept).
- Legal name: DISCOVERU | Website: www.discoverus.org, which writes "DiscoverU" -> "DiscoverU" (the all-capitals filing gets the website's casing).

Answer with the chosen name, its source, your confidence ("high", "medium" or "low"), and one short sentence explaining the choice. The source is "legal_name" if the name is the filed name (ignoring case and legal suffixes), "dba" only if it is the DBA field, and "website" if you took a different name from the website."""

SHARED_SITE_RULE = """If the input lists other organizations that use the same website, the website belongs to a network and this organization is one of its schools, campuses or chapters. Name this organization, not the network: keep the part of the filed name that tells it apart from the others listed, such as its campus, city or grade level. Rule 3 does not apply to the network's own name."""
SHARED_SITE_EXAMPLE = """- Legal name: RIVERBEND PREP ACADEMY - NORTH CAMPUS | Website: riverbendprep.org | Other organizations using this website: Riverbend Prep Academy - South Campus; Riverbend Prep Academy - East Campus -> "Riverbend Prep Academy - North Campus" (the website is the network's, so the campus stays in the name)."""
PROMPT_SHARED_SITE = PROMPT.replace(
    "\n\nThen write the chosen name",
    "\n\n" + SHARED_SITE_RULE + "\n\nThen write the chosen name",
    1,
).replace(
    "\n\nAnswer with the chosen name",
    "\n" + SHARED_SITE_EXAMPLE + "\n\nAnswer with the chosen name",
    1,
)

PROMPT_FILED_NAME = """You write the display name for an organization: its filed legal name, in readable casing.

You get, when available: Legal name (the registered or filed name, often in all capitals and sometimes cut off), DBA, Website (the domain), and Website summary and Website text (the start of its scraped text).

Keep every word of the filed name. Do not replace it with the website's name or a shorter form. Only do three things:
1. If the name looks cut off, complete it from words in the name fields or on the website. Never add a word that is not there.
2. Drop legal-form suffixes at the end of the name (Inc, Incorporated, LLC, Ltd, NFP, Corp).
3. Write it with normal capitalization: proper nouns capitalized, small words such as of and the in lowercase, acronyms such as YMCA kept, mixed case kept as filed. Do not leave a name in all capitals unless it is an acronym.

Answer with the name, its source (always "legal_name"), your confidence ("high", "medium" or "low"), and one short sentence explaining the choice."""


class NameChoice(BaseModel):
    display_name: str
    source: Literal["legal_name", "dba", "website"]
    confidence: Literal["high", "medium", "low"]
    reason: str


def _clean(value):
    return "" if value is None or pd.isna(value) else str(value).strip()


def _first_words(text, n):
    return " ".join(text.split()[:n])


PLAIN_CHARS = str.maketrans(
    {c: "-" for c in "\u2010\u2011\u2012\u2013\u2014\u2212"}
    | {"\u00a0": " ", "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"'}
)
LEGAL_SUFFIX_RE = re.compile(
    r"[,\s]+(inc|incorporated|llc|l\.l\.c|ltd|nfp|corp)\.?$", re.IGNORECASE
)
APOSTROPHE_WORD_RE = re.compile(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)+")


def normalize_name(name):
    """Plain hyphens, quotes and spaces: models sometimes return a non-breaking
    hyphen or a curly apostrophe, which stops the name matching its source."""
    return name.translate(PLAIN_CHARS).strip()


def drop_legal_suffix(name):
    """Drop legal-form suffixes at the end of a name: ``Acme Labs, Inc.`` -> ``Acme Labs``."""
    name = name.strip()
    while True:
        stripped = LEGAL_SUFFIX_RE.sub("", name).strip()
        if not stripped or stripped == name:
            return name
        name = stripped


def restore_apostrophes(name, *filed_names):
    """Put back an apostrophe the scraped text dropped, when a filed name has it.

    The website text loses apostrophes, so a model that copies its spelling
    writes ``OBryan`` for a filed ``O'BRYAN``. A word in ``name`` without an
    apostrophe that matches a filed word once its apostrophe is removed gets
    the apostrophe back at the same place. An acronym or a word with a digit
    before the apostrophe (``YMCA's``, ``4C's``) is left alone: there the
    apostrophe is a plural written wrong, and the model's plural is right.
    """
    filed_words = {}
    for filed in filed_names:
        filed = normalize_name(_clean(filed))
        for word in APOSTROPHE_WORD_RE.findall(filed):
            stem = word.split("'")[0]
            acronym_like = any(c.isdigit() for c in stem) or (
                len(stem) > 1 and stem.isupper() and not filed.isupper()
            )
            if not acronym_like:
                filed_words[word.replace("'", "").lower()] = word

    def restore(match):
        word = match.group(0)
        filed_word = filed_words.get(word.lower())
        if not filed_word:
            return word
        for i, char in enumerate(filed_word):
            if char == "'":
                word = word[:i] + "'" + word[i:]
        return word

    return re.sub(r"[A-Za-z0-9]+", restore, name)


def build_input_text(
    row,
    name_col,
    name_line_cols=(None, None),
    dba_col="DBA",
    dba_field_col=None,
    website_col=None,
    summary_col=None,
    text_col=None,
    siblings_col=None,
    max_siblings=MAX_SIBLINGS,
    max_text_words=DEFAULT_MAX_TEXT_WORDS,
):
    """The model input for one org: one labelled line per field that has a value.

    ``siblings_col`` holds a list of the names of the other organizations that use the
    same website; they are listed after the website, for the ``PROMPT_SHARED_SITE`` rule.
    """
    fields = [
        ("Legal name", name_col),
        ("Name line 1", name_line_cols[0]),
        ("Name line 2", name_line_cols[1]),
        ("DBA", dba_col),
        ("DBA field as filed", dba_field_col),
        ("Website", website_col),
    ]
    lines = []
    for label, col in fields:
        value = _clean(row.get(col)) if col else ""
        if value:
            lines.append(f"{label}: {value}")
    siblings = row.get(siblings_col) if siblings_col else None
    if siblings is not None and len(siblings):
        listed = "; ".join(list(siblings)[:max_siblings])
        more = f" (and {len(siblings) - max_siblings} more)" if len(siblings) > max_siblings else ""
        lines.append(f"Other organizations using this website: {listed}{more}")
    summary = _clean(row.get(summary_col)) if summary_col else ""
    if summary:
        lines.append(f"Website summary: {summary}")
    text = _first_words(_clean(row.get(text_col)), max_text_words) if text_col else ""
    if text:
        lines.append(f"Website text: {text}")
    return "\n".join(lines)


def pick_display_names(
    df,
    session,
    name_col,
    id_col="id",
    model=DEFAULT_MODEL,
    api_backend="vercel",
    max_workers=20,
    max_output_tokens=2000,
    max_errors=1,
    reasoning=DEFAULT_REASONING,
    dba_col=None,
    prompt=PROMPT,
    **input_kwargs,
):
    """One row per org: ``id`` plus the model's ``display_name``, ``source``,
    ``confidence`` and ``reason``.

    ``name_col`` and ``dba_col`` are the legal name and DBA columns. ``input_kwargs``
    are the other column arguments of :func:`build_input_text` (``website_col``,
    ``summary_col``, ``text_col``). The cache is keyed on the org id and the input
    text, so a rerun with the same inputs is free.

    ``prompt`` is the instruction: ``PROMPT``; ``PROMPT_SHARED_SITE`` for organizations whose
    input lists ``siblings_col``; or ``PROMPT_FILED_NAME`` to keep the filed name and only
    fix its casing, suffix and cut-off.

    ``reasoning`` is sent only when given; pass ``None`` for a model that does not
    take it. A response that does not parse as a ``NameChoice`` is left out, like a
    failed call.
    """
    cache = PromptResponseCacheSQL(
        session=session,
        prompt_str=prompt,
        model=model,
        api_backend=api_backend,
        filter_by_model=True,
    )
    ids_texts = [
        (
            str(row[id_col]),
            build_input_text(row, name_col=name_col, dba_col=dba_col, **input_kwargs),
        )
        for _, row in df.iterrows()
    ]
    request_kwargs = {"reasoning": reasoning} if reasoning else {}
    results = cache.bulk_get_cache_or_run(
        given_ids_texts=ids_texts,
        text_format=NameChoice,
        max_output_tokens=max_output_tokens,
        max_workers=max_workers,
        max_errors=max_errors,
        **request_kwargs,
    )
    filed = {str(row[id_col]): (row.get(name_col), row.get(dba_col)) for _, row in df.iterrows()}
    rows = []
    for given_id, result in results.items():
        try:
            choice = NameChoice.model_validate_json(result["response_text"])
        except ValidationError:
            continue
        rows.append(
            {
                id_col: given_id,
                **choice.model_dump(),
                "display_name": restore_apostrophes(
                    drop_legal_suffix(normalize_name(choice.display_name)), *filed[given_id]
                ),
            }
        )
    logger.info("Display names: %d of %d orgs have a usable answer", len(rows), len(ids_texts))
    return pd.DataFrame(rows, columns=[id_col, *NameChoice.model_fields])
