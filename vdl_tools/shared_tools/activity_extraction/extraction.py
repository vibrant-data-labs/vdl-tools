"""
Activity extraction engine: cached website text -> structured activity profile.
===============================================================================

One cached LLM call per organization turns the scraped pages for its website
into a profile: a prose summary plus a list of the organization's distinct
activities, each carrying plain-language evidence fields.

What the profile asks for is set by an ``ExtractionPrompt``: the instructions,
the Pydantic response schema, and the labels used to render it. The prompts live
in ``activity_extraction.prompts``: ``general`` works for any organization, and
``nature`` carries the evidence the Nature taxonomy's four dimensions need. The
engine is the same for both.

Schema contract
---------------
An extraction prompt's ``response_model`` must have ``summary: str``, ``coverage: str`` and
``activities: list[<model>]``, where each activity model has ``activity: str``,
``evidence: str`` and ``certainty: str``. Every other field is free, and is
rendered and exported in the order the model declares it.

Main entry points
-----------------
``extract_activities(orgs, prompt, ...)``
    Runs the extraction prompt over cached website text, one cached call per org.
``run_extraction_prompt(given_ids_texts, prompt, ...)``
    The cached model call itself, through the shared ``InstructorPRC`` cache.
``render_profile(profile, prompt)``
    Renders a profile as human-readable Description text.
``orgs_from_frame(df)``
    Builds ``OrgInput`` rows from a frame with uid / Organization / Website.
``activity_frame(profiles)`` / ``profile_frame(profiles, prompt)``
    One row per activity / one row per organization.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from pydantic import BaseModel

from vdl_tools.shared_tools.activity_extraction.scraped_text import load_site_texts
from vdl_tools.shared_tools.tools.logger import logger

# make_group_text budgets against the model's full context window and reserves
# only 100 tokens for the answer, which overflows on a site like nature.org
# (39 pages, 9.5M chars). Cap the input well below the window instead: a
# summary needs breadth across pages, not the whole of any one of them.
MAX_PAGE_CHARS = 40_000
MAX_PROMPT_CHARS = 300_000

# Activity fields rendered specially rather than as "Label: value" lines
_ACTIVITY_SPECIAL = ("activity", "evidence", "certainty")
# Profile fields that are not org-level evidence lines
_PROFILE_SPECIAL = ("summary", "activities", "coverage")


@dataclass(frozen=True)
class ExtractionPrompt:
    """What to extract: instructions, response schema, cache identity, render labels.

    ``prompt_name`` labels the cache prompt; the cache identity itself is the
    prompt text plus ``response_model``'s JSON schema (see ``InstructorPRC``).
    Change ``prompt_name`` when the prompt changes meaning. ``org_labels`` maps org-level profile fields to the
    labels used in the rendered Description; fields not listed are labelled
    from their names.
    """

    name: str
    text: str
    prompt_name: str
    response_model: type[BaseModel]
    model: str = "gpt-5.4-mini"
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
    fallback_text: str | None = None  # used when no website text is cached
    liveness_action: str = ""  # from a liveness pass, carried to output


def _label(field_name: str) -> str:
    """'who_carries_it_out' -> 'Who carries it out'."""
    text = field_name.replace("_", " ")
    return text[:1].upper() + text[1:]


def cap_pages(pages: list[dict]) -> list[dict]:
    """Trim pages so a huge site can't overflow the model's context window.

    Each page is truncated first, so one enormous page cannot crowd out the
    rest; the page list is then cut at the total budget. Pages arrive in the
    scraper's order, with the home page and short structural paths first, so
    what survives is the descriptive material rather than story archives.
    """
    import hashlib

    capped, total = [], 0
    pages = sorted(
        pages, key=lambda p: (len(p["subpath"]), hashlib.md5(p["subpath"].encode()).hexdigest())
    )
    for page in pages:
        text = page["text"][:MAX_PAGE_CHARS]
        if total + len(text) > MAX_PROMPT_CHARS:
            break
        capped.append({**page, "text": text})
        total += len(text)
    return capped


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


def build_inputs(
    orgs: list[OrgInput],
    prompt: ExtractionPrompt,
    session=None,
    allow_archived_fallback: bool = False,
    site_texts: dict | None = None,
) -> tuple[list[tuple[str, str]], dict[str, dict]]:
    """Assemble the (cache key, prompt text) pairs and per-org provenance.

    Returns ``(given_ids_texts, meta)`` where ``meta[cache_key]`` records the
    uid, org name, text source, and character count behind that call.

    ``allow_archived_fallback`` is off by default and should stay off unless
    someone has decided, per organization, that stale text is acceptable. A
    prior description is typically a year-old summary of a summary: activity
    records built from it read exactly like records built from a live site,
    and describe in the present tense work that may have ended. An
    organization with no current text is skipped and reported, not quietly
    back-filled.
    """
    if site_texts is None:
        site_texts = load_site_texts([o.url for o in orgs if o.url], session=session)

    given_ids_texts: list[tuple[str, str]] = []
    meta: dict[str, dict] = {}
    for org in orgs:
        site = site_texts.get(org.url) if org.url else None
        if site and site.pages:
            # Current website text: the normal path
            from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text

            page_text = make_group_text(prompt.text, cap_pages(site.pages), model=prompt.model)
            text = f"ORGANIZATION: {org.name}\n\n{page_text}" if page_text else None
            source = site.source_table
            n_chars = site.n_chars
        elif org.fallback_text and allow_archived_fallback:
            # No cached page text: summarize the description already on file so
            # the org still gets an activity record set, flagged as such.
            text = (
                f"ORGANIZATION: {org.name}\n\n"
                f"URL: {org.url or 'unknown'}\nTEXT: {org.fallback_text}"
            )
            source = "existing_description"
            n_chars = len(org.fallback_text)
        else:
            logger.warning(
                "No current website text for %s (%s)%s",
                org.name,
                org.uid,
                "" if allow_archived_fallback else " — skipped (archived fallback is off)",
            )
            continue
        if not text:
            logger.warning("Empty prompt text for %s (%s)", org.name, org.uid)
            continue
        given_ids_texts.append((org.uid, text))
        meta[org.uid] = {
            "uid": org.uid,
            "Organization": org.name,
            "Website": org.url,
            "text_source": source,
            "text_chars": n_chars,
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
    session=None,
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
    session : sqlalchemy.orm.Session, optional
        Reuse an open session; otherwise one is opened for this call.

    Returns
    -------
    dict[str, BaseModel]
        cache key -> parsed ``prompt.response_model``. Inputs whose request failed
        are absent; `extract_frame` records them as ``request_failed``.
    """
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC

    if not given_ids_texts:
        return {}
    api_kwargs = {"reasoning": prompt.reasoning} if prompt.reasoning else {}
    with get_session(session=session) as active:
        cache = InstructorPRC(
            session=active,
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


def extract_activities(
    orgs: list[OrgInput],
    prompt: ExtractionPrompt,
    max_workers: int = 16,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    allow_archived_fallback: bool = False,
    # The cache runs one chunk at a time and blocks until every call in it
    # returns, so a chunk costs its slowest call and the pool idles meanwhile.
    # A big site can hold up nineteen others at the shared default of 20.
    # Summary responses are ~3KB (p95 6KB), so a 200-row commit is well under a
    # megabyte — the small default exists for judge responses, which are far
    # larger, and does not need to apply here.
    n_per_commit: int = 200,
    session=None,
) -> dict[str, tuple[BaseModel, dict]]:
    """Extract an activity profile for each organization with cached website text.

    Model and reasoning effort come from ``prompt``; use ``dataclasses.replace``
    to override them. Returns ``{uid: (profile, meta)}``.
    """
    given_ids_texts, meta = build_inputs(
        orgs,
        prompt,
        session=session,
        allow_archived_fallback=allow_archived_fallback,
    )
    logger.info(
        "Extracting activities (%s prompt) for %s organizations with %s",
        prompt.name,
        len(given_ids_texts),
        prompt.model,
    )
    profiles = run_extraction_prompt(
        given_ids_texts,
        prompt,
        max_workers=max_workers,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
        n_per_commit=n_per_commit,
        session=session,
    )
    return {meta[k]["uid"]: (p, meta[k]) for k, p in profiles.items() if k in meta}


def orgs_from_frame(df: pd.DataFrame) -> list[OrgInput]:
    """``OrgInput`` rows from a frame with ``uid``, ``Organization``, ``Website``.

    ``Description`` (the archived-fallback text) and ``liveness_action`` are
    read when present.
    """

    def _opt(row, col: str) -> str | None:
        value = row.get(col)
        return str(value) if value is not None and pd.notna(value) else None

    return [
        OrgInput(
            uid=str(row["uid"]),
            name=str(row["Organization"]),
            url=_opt(row, "Website"),
            fallback_text=_opt(row, "Description"),
            liveness_action=_opt(row, "liveness_action") or "",
        )
        for _, row in df.iterrows()
    ]


def activity_frame(profiles: dict[str, tuple[BaseModel, dict]]) -> pd.DataFrame:
    """One row per activity: uid, Organization, activity_index, then the activity fields."""
    return pd.DataFrame(
        [
            {
                "uid": uid,
                "Organization": meta["Organization"],
                "activity_index": i,
                **activity.model_dump(),
            }
            for uid, (profile, meta) in profiles.items()
            for i, activity in enumerate(profile.activities, start=1)
        ]
    )


def profile_frame(
    profiles: dict[str, tuple[BaseModel, dict]],
    prompt: ExtractionPrompt,
) -> pd.DataFrame:
    """One row per organization: rendered Description, summary, org-level fields, provenance."""
    return pd.DataFrame(
        [
            {
                "uid": uid,
                "Organization": meta["Organization"],
                "Website": meta.get("Website"),
                "Description": render_profile(profile, prompt),
                "summary": profile.summary,
                "n_activities": len(profile.activities),
                "coverage": profile.coverage,
                **{f: getattr(profile, f) for f in prompt.org_fields()},
                "text_source": meta.get("text_source", ""),
                "text_chars": meta.get("text_chars", 0),
            }
            for uid, (profile, meta) in profiles.items()
        ]
    )
