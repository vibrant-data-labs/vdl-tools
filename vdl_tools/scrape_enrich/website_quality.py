"""
website_quality.py
==================

Keep junk website content out of organization descriptions.

``Website Summary`` is one of the ``TEXT_FIELDS`` fed to the summary of
summaries, so a parked or squatted site does not stay in one column — it is
written into ``Summary`` and into every taxonomy input. ``gate_websites`` runs
inside the enrichment pipeline, between the website summaries being built and
the summary of summaries, and blanks the junk before it spreads.

Every rule here is deterministic. Classes that need judgement — the site
belongs to a different real organization, or to an ordinary business on a
resold domain — are deliberately not detected: no keyword list finds them, and
an LLM pass was measured to add about 20 catches per 1,500 orgs, ~80% of them
that first class. One-offs a rule would have to overfit to catch belong in the
curated decisions file instead.

It also resolves every website domain once (where it redirects, whether it is
up) into a domain cache that can be shared across projects (see ``gate_websites``).

Moved here from ed_tracker so every pipeline built on ``run_pipeline`` can use it.
"""

import concurrent.futures
import datetime as dt
import logging
import pathlib
import re

import httpx
import pandas as pd
from vdl_tools.portfolio_comparison.intake.normalize import resolve_redirect
from vdl_tools.scrape_enrich.scraper.async_scraper import BROWSER_HEADERS
from vdl_tools.scrape_enrich.url_keys import FREE_MAIL, website_domain
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.scrape_enrich.website_junk_rules import is_gambling, is_parked_summary
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesParsed

# httpx logs every request at INFO — 50k lines on a full run.
logging.getLogger("httpx").setLevel(logging.WARNING)

WEBSITE_COLS = ["Website_cb_cd", "Website"]
GT_UID_PREFIX = "givingtuesday_"

_EMAIL_RE = re.compile(r"^[^\s/@]+@([^\s/@]+\.[a-z]{2,})$", re.I)


# --- normalization ------------------------------------------------------------
def email_domain(url):
    """Domain of an email address typed into a website field, else ''."""
    if not isinstance(url, str):
        return ""
    match = _EMAIL_RE.match(re.sub(r"\s+", "", url))
    return match.group(1).lower() if match else ""


# --- liveness and redirects ---------------------------------------------------
# The question worth asking is "will the scraper get this page", so the probe
# sends what the scraper sends. Servers do discriminate: abetterchance.org
# returns 410 to a bare client and 403 to a browser, and 410 is the one we
# count as dead.
# Fast connect to fail quickly on dead links, slow read for live but slow
# servers — the scraper's split, instead of one flat timeout.
SCRAPER_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)
# The enrichment path scrapes with verify_ssl=True, so a site whose cert fails
# is one the scraper cannot read either.
VERIFY_SSL = True


def _answers(url, timeout=SCRAPER_TIMEOUT):
    """True/False, or None when it timed out and is worth one more try."""
    try:
        with httpx.stream(
            "GET", url, follow_redirects=True, timeout=timeout,
            verify=VERIFY_SSL, headers=BROWSER_HEADERS,
        ) as resp:
            return resp.status_code not in (404, 410)
    except httpx.TimeoutException:
        return None
    except Exception:
        return False


def url_alive(host, timeout=SCRAPER_TIMEOUT):
    """Whether the scraper would be able to read ``host``.

    vdl-tools' ``check_url_alive`` tries https only, with no headers, while the
    scraper sends a full browser fingerprint — so the two disagree about the
    same site. On 60 domains its cache called dead, matching the scraper
    recovered 25 and lost none; the ``http://`` fallback is most of that.
    403/503 is a bot wall, which counts as alive. Only the candidates that
    timed out are retried — retrying all four cost 40s on a www-only domain.
    """
    hosts = [host] if host.startswith("www.") else [host, f"www.{host}"]
    candidates = [f"{s}://{h}" for h in hosts for s in ("https", "http")]
    timed_out = []
    for url in candidates:
        answer = _answers(url, timeout)
        if answer:
            return True
        if answer is None:
            timed_out.append(url)
    return any(_answers(url, timeout) for url in timed_out)


def check_domain(domain):
    """Final domain after redirects, and whether the site is up."""
    return {
        "final_domain": website_domain(resolve_redirect(domain)) or domain,
        "alive": url_alive(domain),
        "checked": dt.date.today().isoformat(),
    }


CACHE_COLS = ["domain", "final_domain", "alive", "checked"]


def load_domain_cache(uri):
    """``domain -> {final_domain, alive, checked}`` ({} when absent).

    ``uri`` is a local path or an ``s3://`` URI. The cache is keyed by domain,
    not by project, so one file can serve every pipeline: a domain checked for
    ed_tracker is not checked again for climate-landscape.
    """
    try:
        df = read_dataframe(uri)
    except FileNotFoundError:
        return {}
    return {
        r.domain: {"final_domain": r.final_domain, "alive": bool(r.alive), "checked": r.checked}
        for r in df.itertuples(index=False)
    }


CACHE_VALID_DAYS = 90  # same validity window as vdl-tools' S3 Cache
SAVE_EVERY = 2000
MAX_WORKERS = 32


def _save_cache(cache, uri):
    """Write the cache, first merging in what another run saved meanwhile.

    A parquet file is rewritten whole, so two pipelines sharing the file would
    otherwise drop each other's new checks (last writer wins). Merging keeps the
    newest check per domain; only a write landing between this read and this
    write can still be lost, and a lost entry is simply checked again next run.
    """
    for domain, entry in load_domain_cache(uri).items():
        if domain not in cache or entry["checked"] > cache[domain]["checked"]:
            cache[domain] = entry
    write_dataframe(
        pd.DataFrame(
            [{"domain": d, **entry} for d, entry in sorted(cache.items())],
            columns=CACHE_COLS,
        ),
        uri,
    )


def resolve_domains(domains, uri, refresh=False, max_workers=MAX_WORKERS):
    """Check every domain not yet (validly) cached; return the whole cache."""
    cache = load_domain_cache(uri)
    cutoff = (dt.date.today() - dt.timedelta(days=CACHE_VALID_DAYS)).isoformat()
    todo = sorted(
        d for d in domains if refresh or d not in cache or cache[d]["checked"] < cutoff
    )
    logger.info(
        "Websites: %d domain(s), %d already cached, %d to check",
        len(domains), len(domains) - len(todo), len(todo),
    )
    if not todo:
        return cache
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(check_domain, d): d for d in todo}
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            cache[futures[future]] = future.result()
            if i % SAVE_EVERY == 0:
                _save_cache(cache, uri)
                logger.info("Websites: checked %d/%d", i, len(todo))
    _save_cache(cache, uri)
    return cache


def fresh_alive_check(domains, max_workers=MAX_WORKERS):
    """``url_alive`` right now, bypassing the 90-day cache.

    Only called on the domains that produced scraped text: a domain with no
    text loses its link regardless of this result, and a cached ``alive`` is
    not proof of anything today — Kingsley House has real scraped text but a
    fresh check gets a hard ``ConnectError``. Scoping to the chars>0 population
    is what keeps this affordable on every run.
    """
    domains = sorted({d for d in domains if d})
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(url_alive, d): d for d in domains}
        for future in concurrent.futures.as_completed(futures):
            out[futures[future]] = future.result()
    return out


def resolve_for_frame(df, domain_cache_uri, refresh=False):
    """Resolve every website domain in ``df``; returns the whole cache."""
    cols = [c for c in WEBSITE_COLS if c in df.columns]
    domains = {k for col in cols for k in df[col].map(website_domain)} - {""}

    cache = resolve_domains(domains, domain_cache_uri, refresh=refresh)
    ours = [(d, cache[d]) for d in domains if d in cache]
    logger.info(
        "Websites: %d domain(s) — %d redirect elsewhere, %d dead. Cache: %s",
        len(ours),
        sum(entry["final_domain"] != d for d, entry in ours),
        sum(not entry["alive"] for _, entry in ours),
        domain_cache_uri,
    )
    return cache


# --- evidence -----------------------------------------------------------------
# What the summariser actually read, keyed as the pipeline keys its summaries.
SCRAPE_KEY_CHUNK = 1000


def combined_texts(keys, session):
    """``extracted_website_key -> (combined_text, num_errors)`` from the scrape cache."""


    keys = sorted({k for k in keys if k})
    out = {}
    for i in range(0, len(keys), SCRAPE_KEY_CHUNK):
        chunk = keys[i:i + SCRAPE_KEY_CHUNK]
        for row in session.query(WebPagesParsed).filter(
            WebPagesParsed.cleaned_home_key.in_(chunk)
        ):
            out[row.cleaned_home_key] = (row.combined_text or "", row.num_errors or 0)
    return out


def _text_quality(text, errors):
    """ok | thin | parked | blocked | garbled | dead | empty, from the scraped text.

    The binary, parked and bot-wall rules (and the short-text gate on walls)
    live in ``text_quality``, which the scraper and the summarizer also use, so
    the three agree on what counts as junk.
    """
    from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality

    return classify_text_quality(text, errors)


def website_evidence(df, domain_cache, scraped=None, id_col="id"):
    """One row per org: everything known about its website before any judgement.

    ``scraped`` is :func:`combined_texts`; without it there is no text-quality
    verdict, since the summary describes the page rather than being it.
    """
    scraped = scraped or {}
    summary = df["Website Summary"].fillna("")
    site = df["Website"] if "Website" in df.columns else df["Website_cb_cd"]
    keys = df.get("extracted_website_key", pd.Series("", index=df.index)).fillna("")
    domain = site.map(website_domain)
    mail = site.map(email_domain)
    cached = domain.map(lambda d: domain_cache.get(d, {}))
    text = keys.map(lambda k: scraped.get(k))

    out = pd.DataFrame({
        id_col: df[id_col],
        "profile_name": df.get("profile_name", ""),
        "data_source": df.get("Data Source", ""),
        "website": site,
        "domain": domain,
        "final_domain": cached.map(lambda c: c.get("final_domain", "")),
        "alive": cached.map(lambda c: c.get("alive")),
        "email_kind": mail.map(
            lambda m: "" if not m else "free_mail" if m in FREE_MAIL else "own_domain"
        ),
        "scraped_chars": text.map(lambda t: len(t[0]) if t else None),
        "text_quality": text.map(
            lambda t: _text_quality(*t) if t else ""
        ),
        "has_summary": summary.str.strip().ne(""),
        "parked_summary": summary.map(is_parked_summary),
        "gambling": [
            is_gambling(page_summary, f"{name} {desc} {desc990}")
            for page_summary, name, desc, desc990 in zip(
                summary,
                df.get("profile_name", pd.Series("", index=df.index)).fillna(""),
                df.get("Description", pd.Series("", index=df.index)).fillna(""),
                df.get("Description_990", pd.Series("", index=df.index)).fillna(""),
            )
        ],
    })
    out["redirected"] = [
        bool(d and f and f != d) for d, f in zip(out["domain"], out["final_domain"])
    ]
    return out


# Qualities that say nothing about who owns the domain, so the link stays.
# Only the two verdicts whose OWN rule (remove_website, for junk content) needs
# a "don't also clear the link" exception carved out of it. "dead"/"empty" must
# stay out of this set: they are exactly the state the separate "site does not
# answer and nothing was ever scraped" rule targets, and including them here
# vetoed that rule for every org whose domain WAS attempted and came back
# empty — only the minority with no scrape row at all (quality == "") ever had
# their link actually cleared. "thin" never triggers remove_website either, so
# it was inert either way; left out for the set to document only what matters.
KEEP_URL_QUALITIES = {"garbled", "blocked"}


# --- the gate -----------------------------------------------------------------
# Sheet names in the review file, and the columns they are pasted into.
ACTIONS = ["remove_website", "remove_url", "fix_website"]


def decide_actions(evidence, manual=None, id_col="id"):
    """What to do about each org's website, most decisive rule first.

    Every rule here is deterministic. Classes that need judgement — the site
    belongs to a different real organization, or is an ordinary business on a
    resold domain — are deliberately not detected: no keyword list finds them,
    and guessing costs more than it gains. ``manual`` is ``{id: action}`` from
    the curated remove file and always wins.

    The public-facing link is shown only when BOTH scraped text exists and a
    fresh liveness check (not the 90-day cache) confirms the site — a cached
    ``alive`` does not mean the site is still up today, and text alone does not
    either, since it may have been scraped weeks ago. ``evidence["fresh_alive"]``
    is missing (``NaN``/``None``) for any domain the chars>0 population did not
    include, which this treats the same as a failed check.
    """
    manual = manual or {}
    fresh_alive_col = evidence["fresh_alive"] if "fresh_alive" in evidence else pd.Series(
        False, index=evidence.index
    )
    # A missing scrape row comes through as NaN, not None, once it is a
    # DataFrame column — and `not float("nan")` is False, so the raw column
    # would silently skip the "nothing was ever scraped" case below.
    chars_col = evidence["scraped_chars"].fillna(0)
    reasons, actions, corrections = [], [], []
    for org_id, kind, domain, quality, alive, chars, gambling, parked_summary, fresh_alive in zip(
        evidence[id_col].astype(str), evidence["email_kind"], evidence["domain"],
        evidence["text_quality"], evidence["alive"], chars_col,
        evidence["gambling"], evidence["parked_summary"], fresh_alive_col,
    ):
        if org_id in manual:
            action = "" if manual[org_id] == "keep" else manual[org_id]
            reason = "curated"
        elif quality == "parked" or parked_summary:
            action, reason = "remove_website", "the page is a domain-for-sale listing"
        elif quality == "garbled":
            action, reason = "remove_website", (
                "the scrape stored binary, not text — the site itself is fine and "
                "needs re-scraping"
            )
        elif quality == "blocked":
            action, reason = "remove_website", (
                "the scrape hit a bot wall, so the summary describes a challenge "
                "screen or was invented — the site itself is fine"
            )
        elif gambling:
            action, reason = "remove_website", (
                "the page is a gambling site and the org's own records are not about gaming"
            )
        elif kind == "free_mail":
            action, reason = "remove_website", "website field holds a free-mail address"
        elif kind == "own_domain":
            action, reason = "fix_website", "website field holds an email at the org's own domain"
        elif not chars or fresh_alive is not True:
            action, reason = "remove_url", (
                "nothing was ever scraped from it"
                if not chars else
                "a fresh check found the site down, so the link is not shown even "
                "though older text was scraped from it"
            )
        else:
            action, reason = "", ""
        reasons.append(reason)
        actions.append(action)
        # Stripping the local part is right whatever the page turns out to be.
        corrections.append(f"https://{domain}" if kind == "own_domain" and domain else "")
    return evidence.assign(reason=reasons, action=actions, corrected_website=corrections)


def apply_website_gate(df, decisions, id_col="id"):
    """Act on the decisions, in this run, before the summary of summaries.

    The summary text is never blanked by this rule — only ``remove_website``
    does that, for junk content. The link itself is stricter: it survives only
    when text was scraped from the domain AND a fresh check (done at decide
    time, not the 90-day cache) confirms the site is still up. Old text is
    not proof the site is up today, so this can hide a link while leaving its
    summary in place — Kingsley House is the case that forced this: real text,
    a fresh ``ConnectError``.
    """
    by_action = dict(zip(decisions[id_col].astype(str), decisions["action"]))
    # Independent of the summary verdict: an email in the website field is always
    # the wrong URL, whether or not its text also turns out to be junk.
    fixed = {
        str(i): url
        for i, url in zip(decisions[id_col], decisions["corrected_website"]) if url
    }
    # A page we could not read is not evidence against the domain: a garbled
    # scrape, a bot wall or a thin page says nothing about who owns it.
    keeps_url = {
        str(i) for i, quality in zip(decisions[id_col], decisions["text_quality"])
        if quality in KEEP_URL_QUALITIES
    }
    ids = df[id_col].astype(str)

    blank = ids.map(lambda i: by_action.get(i) == "remove_website")
    drop_url = ids.map(
        lambda i: by_action.get(i) in ("remove_website", "remove_url") and i not in keeps_url
    )
    fix = ids.map(lambda i: i in fixed) & ~drop_url

    df.loc[blank, "Website Summary"] = ""
    df.loc[drop_url, "Website"] = None
    df.loc[fix, "Website"] = ids[fix].map(fixed)

    logger.info(
        "Website quality: blanked %d summary/summaries, cleared %d link(s), "
        "corrected %d website(s)",
        int(blank.sum()), int(drop_url.sum()), int(fix.sum()),
    )
    return df


def write_website_review(decisions, path):
    """One sheet per action, plus the thin pages that were left alone."""
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        writer.book.strings_to_urls = False
        for action in ACTIONS:
            rows = decisions[decisions["action"] == action]
            rows.to_excel(writer, sheet_name=action, index=False)
        # Everything acted on is on a sheet above; this is what a person might
        # still want to look at — a site that answers but yielded little text.
        review = decisions[(decisions["action"] == "") & (decisions["text_quality"] == "thin")]
        review.to_excel(writer, sheet_name="review", index=False)
    logger.info(
        "Website quality: wrote %s (%s, review %d)",
        path,
        ", ".join(
            f"{a} {int((decisions['action'] == a).sum())}" for a in ACTIONS
        ),
        int(((decisions["action"] == "") & (decisions["text_quality"] == "thin")).sum()),
    )


# --- entrypoint ---------------------------------------------------------------
# The curated file the rules answer to. Written by hand, never regenerated —
# unlike the review xlsx, which is overwritten on every run.
DECISION_COLS = ["id", "action", "note"]
DECISIONS = {"remove_website", "remove_url", "fix_website", "keep"}


def _org_key(org_id):
    """An id with any GivingTuesday prefix removed: GT ids are ``givingtuesday_<ein>``
    and Candid ids the bare EIN, so the two forms name the same organization."""
    return str(org_id).strip().removeprefix(GT_UID_PREFIX)


def load_website_decisions(path, present=None):
    """``{id: action}`` from the curated decisions CSV at ``path`` ({} when None or absent).

    ``keep`` vetoes whatever the rules concluded, which is the only way to
    overrule them: every other rule here fires automatically. An EIN may be
    written bare or as ``givingtuesday_<ein>``; either form applies to every
    present org with that EIN, whichever form its id takes.
    """
    if path is None or not pathlib.Path(path).is_file():
        return {}
    path = pathlib.Path(path)
    df = pd.read_csv(path, dtype=str).fillna("")
    by_key = {}
    for org_id in present or ():
        by_key.setdefault(_org_key(org_id), []).append(org_id)
    decisions, ignored = {}, []
    for raw, action in zip(df.get("id", []), df.get("action", [])):
        raw, action = str(raw).strip(), str(action).strip().lower()
        if not raw or action not in DECISIONS:
            ignored.append((raw, action))
            continue
        if present is None:
            decisions[raw] = action
            continue
        matches = by_key.get(_org_key(raw))
        if not matches:
            ignored.append((raw, "no such organization"))
            continue
        for org_id in matches:
            decisions[org_id] = action
    if ignored:
        logger.warning(
            "Website quality: ignoring %d curated row(s) in %s: %s",
            len(ignored), path.name,
            ", ".join(f"{i!r} ({why})" for i, why in ignored[:8]),
        )
    logger.info("Website quality: %d curated decision(s) from %s", len(decisions), path)
    return decisions


def gate_websites(
    df,
    *,
    domain_cache_uri,
    decisions_path=None,
    review_path=None,
    id_col="id",
    resolve=True,
    dry_run=False,
    fresh_check=True,
):
    """Find the junk websites by rule and act on them, before they spread.

    Called from ``run_pipeline`` (and ed_tracker's ``process_enrich``) after the
    website summaries are built and before ``add_summary_of_summaries``. Returns ``(df, decisions)``.

    ``domain_cache_uri`` is the domain cache (local path or ``s3://`` URI; one
    file can be shared across projects). ``decisions_path`` is the hand-curated
    ``id,action,note`` CSV and ``review_path`` the xlsx written for review; both
    are skipped when None. Look all three up in the calling project's paths config.

    ``fresh_check`` reprobes every domain with scraped text, bypassing the
    90-day liveness cache, since the public link should only show when the
    site is confirmed up right now. Scoped to that chars>0 population, not
    the whole ~25k-domain corpus, to bound the added runtime.
    """

    cache = (
        resolve_for_frame(df, domain_cache_uri) if resolve
        else load_domain_cache(domain_cache_uri)
    )
    with get_session() as session:
        scraped = combined_texts(df.get("extracted_website_key", pd.Series(dtype=str)), session)

    evidence = website_evidence(df, cache, scraped, id_col=id_col)
    if fresh_check:
        live_domains = evidence.loc[evidence["scraped_chars"].fillna(0) > 0, "domain"]
        fresh = fresh_alive_check(live_domains)
        logger.info(
            "Website quality: fresh-checked %d domain(s) with scraped text, %d alive",
            len(fresh), sum(fresh.values()),
        )
        evidence["fresh_alive"] = evidence["domain"].map(fresh)
    manual = load_website_decisions(decisions_path, set(df[id_col].astype(str)))
    decisions = decide_actions(evidence, manual, id_col=id_col)

    if review_path:
        write_website_review(decisions, review_path)
    if dry_run:
        logger.warning("Website quality: dry run — nothing changed")
        return df, decisions
    return apply_website_gate(df, decisions, id_col=id_col), decisions
