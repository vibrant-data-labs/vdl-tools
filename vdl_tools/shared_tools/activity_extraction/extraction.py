"""
Activity extraction engine: cached website text -> structured activity profile.
===============================================================================

One cached LLM call per organization turns the scraped pages for its website into a
profile: a prose summary plus a list of the organization's distinct activities, each
carrying plain-language evidence fields.

What the profile asks for is set by an ``ExtractionPrompt``: the instructions, the
Pydantic response schema, and the labels used to render it. ``general.py`` holds the
one prompt in use; the engine does not depend on it.

Schema contract
---------------
An extraction prompt's ``response_model`` must have ``summary: str``, ``coverage: str`` and
``activities: list[<model>]``, where each activity model has ``activity: str``,
``evidence: str`` and ``certainty: str``. Every other field is free, and is rendered and
exported in the order the model declares it.

Entry points
------------
``load_website_texts(urls)``
    Website text per URL through the shared scraper, junk sites excluded.
``build_inputs(orgs, prompt, website_texts)``
    Assembles the (cache key, prompt text) pairs.
``run_extraction_prompt(given_ids_texts, prompt, ...)``
    The cached model call, through the shared ``InstructorPRC`` cache.
``render_profile(profile, prompt)``
    Renders a profile as human-readable Description text.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel

from vdl_tools.shared_tools.tools.logger import logger

# The scraper combines a site's pages up to the summarization model's context window,
# which on a site like nature.org (39 pages, 9.5M chars) is far more than extraction
# needs: a profile needs breadth across pages, not the whole of any one of them.
MAX_PROMPT_CHARS = 300_000

# Activity fields rendered specially rather than as "Label: value" lines
_ACTIVITY_SPECIAL = ("activity", "evidence", "certainty")
# Profile fields that are not org-level evidence lines
_PROFILE_SPECIAL = ("summary", "activities", "coverage")


@dataclass(frozen=True)
class ExtractionPrompt:
    """What to extract: instructions, response schema, cache identity, render labels.

    ``prompt_name`` labels the cache prompt; the cache identity itself is the prompt
    text plus ``response_model``'s JSON schema (see ``InstructorPRC``). ``org_labels``
    maps org-level profile fields to the labels used in the rendered Description;
    fields not listed are labelled from their names.
    """

    text: str
    prompt_name: str
    response_model: type[BaseModel]
    model: str
    reasoning: dict | None = None
    org_labels: dict[str, str] = field(default_factory=dict)

    @property
    def activity_model(self) -> type[BaseModel]:
        """The Pydantic model of one activity entry."""
        return self.response_model.model_fields["activities"].annotation.__args__[0]

    def org_fields(self) -> list[str]:
        """Org-level evidence fields of the profile, in schema order."""
        return [f for f in self.response_model.model_fields if f not in _PROFILE_SPECIAL]


@dataclass
class OrgInput:
    """One organization to extract activities for."""

    uid: str
    name: str
    url: str | None = None


def _label(field_name: str) -> str:
    """'who_carries_it_out' -> 'Who carries it out'."""
    text = field_name.replace("_", " ")
    return text[:1].upper() + text[1:]


def clean_evidence(text: str) -> str:
    """Strip quote characters the model wrapped around the evidence phrase."""
    return str(text).strip().strip("“”‘’\"'").strip()


def _activity_block(index: int, activity: BaseModel) -> str:
    """One numbered activity: name, a line per evidence field, then the quote."""
    lines = [f"{index}. {activity.activity}"]
    lines += [
        f"   {_label(name)}: {getattr(activity, name)}"
        for name in type(activity).model_fields
        if name not in _ACTIVITY_SPECIAL
    ]
    lines.append(
        f"   Evidence: “{clean_evidence(activity.evidence)}” (certainty: {activity.certainty})"
    )
    return "\n".join(lines)


def render_profile(profile: BaseModel, prompt: ExtractionPrompt) -> str:
    """Render a profile as the org's Description text."""
    parts = [profile.summary, "", "Activities:"]
    parts += [_activity_block(i, a) for i, a in enumerate(profile.activities, start=1)]
    parts.append("")
    parts += [
        f"{prompt.org_labels.get(f, _label(f))}: {getattr(profile, f)}" for f in prompt.org_fields()
    ]
    parts.append(f"Source coverage: {profile.coverage}")
    return "\n".join(parts)


def load_website_texts(urls: list[str]) -> dict[str, str]:
    """Website text per URL through the shared scraper, junk sites excluded.

    ``scrape_websites_psql`` serves the pages already in the SQL scrape cache and
    scrapes only what is missing, and returns each site's pages combined into one
    prompt-ready text (``URL:`` / ``TEXT:`` blocks separated by ``----``). Sites whose
    text is parked, blocked, garbled, dead or empty are left out, using the same
    verdicts the website summarizer applies.

    Parameters
    ----------
    urls : list[str]
        Organization website URLs; blanks and duplicates are ignored.

    Returns
    -------
    dict[str, str]
        ``url -> combined text`` for every URL with usable text.
    """
    from vdl_tools.scrape_enrich.scraper.scrape_websites import scrape_websites_psql
    from vdl_tools.scrape_enrich.url_keys import extract_website_name
    from vdl_tools.shared_tools.web_summarization.website_summarization_psql import (
        is_summarizable,
    )

    urls = [u for u in dict.fromkeys(urls) if u and str(u).strip()]
    if not urls:
        return {}
    combined = scrape_websites_psql(urls)
    rows = {row["cleaned_home_key"]: row for row in combined.to_dict("records")} if len(combined) else {}
    texts = {}
    for url in urls:
        row = rows.get(extract_website_name(url))
        if row and is_summarizable(row["combined_text"], row.get("num_errors") or 0):
            texts[url] = row["combined_text"]
    return texts


def build_inputs(
    orgs: list[OrgInput],
    prompt: ExtractionPrompt,
    website_texts: dict[str, str] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, dict]]:
    """Assemble the (cache key, prompt text) pairs and per-org provenance.

    An organization without usable website text is skipped and reported; it is never
    back-filled from a description or other stale text.

    Parameters
    ----------
    orgs : list[OrgInput]
    prompt : ExtractionPrompt
    website_texts : dict, optional
        ``url -> text`` as `load_website_texts` returns; None calls it.

    Returns
    -------
    tuple[list[tuple[str, str]], dict[str, dict]]
        ``given_ids_texts`` as ``(uid, text)`` pairs, and ``meta[uid]`` with ``uid``,
        ``Organization``, ``Website`` and ``text_chars``.
    """
    if website_texts is None:
        website_texts = load_website_texts([o.url for o in orgs if o.url])
    given_ids_texts: list[tuple[str, str]] = []
    meta: dict[str, dict] = {}
    for org in orgs:
        text = website_texts.get(org.url) if org.url else None
        if not text:
            logger.warning("No usable website text for %s (%s); skipped", org.name, org.uid)
            continue
        text = text[:MAX_PROMPT_CHARS]
        given_ids_texts.append((org.uid, f"ORGANIZATION: {org.name}\n\n{text}"))
        meta[org.uid] = {
            "uid": org.uid,
            "Organization": org.name,
            "Website": org.url,
            "text_chars": len(text),
        }
    return given_ids_texts, meta


def run_extraction_prompt(
    given_ids_texts: list[tuple[str, str]],
    prompt: ExtractionPrompt,
    *,
    max_workers: int = 16,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    n_per_commit: int = 200,
) -> dict[str, BaseModel]:
    """Run one extraction prompt over many inputs through the shared structured cache.

    Parameters
    ----------
    given_ids_texts : list[tuple[str, str]]
        (cache key, prompt input text) pairs from `build_inputs`.
    prompt : ExtractionPrompt
        Supplies the prompt text, cache prompt name, response schema, model and
        reasoning effort.
    max_workers : int
        Parallel API calls.
    read_from_cache, write_to_cache : bool
        Cache behaviour, as for `InstructorPRC.bulk_get_cache_or_run`.
    n_per_commit : int
        Rows per database commit.

    Returns
    -------
    dict[str, BaseModel]
        cache key -> parsed ``prompt.response_model``. Inputs whose request failed are
        absent; `extract_frame` records them as ``request_failed``.
    """
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC

    if not given_ids_texts:
        return {}
    api_kwargs = {"reasoning": prompt.reasoning} if prompt.reasoning else {}
    with get_session() as session:
        cache = InstructorPRC(
            session=session,
            prompt_str=prompt.text,
            prompt_name=prompt.prompt_name,
            response_model=prompt.response_model,
            model=prompt.model,
            # Model identity is part of the key: a different model must never replay
            # another model's cached answers as its own.
            filter_by_model=True,
        )
        return cache.bulk_get_parsed(
            given_ids_texts,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            **api_kwargs,
        )
