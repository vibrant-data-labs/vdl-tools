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
up) into ``paths["website_domain_cache"]``.

Moved here from ed_tracker so every pipeline built on ``run_pipeline`` can use it.
"""

import concurrent.futures
import datetime as dt
import json
import logging
import pathlib
import re
from urllib.parse import urlparse

import httpx
import pandas as pd
from vdl_tools.portfolio_comparison.intake.normalize import PLATFORM_DOMAINS, resolve_redirect
from vdl_tools.shared_tools.tools.logger import logger

# httpx logs every request at INFO — 50k lines on a full run.
logging.getLogger("httpx").setLevel(logging.WARNING)

WEBSITE_COLS = ["Website_cb_cd", "Website"]
GT_UID_PREFIX = "givingtuesday_"

# A website field holding one of these identifies a mail provider, not an org.
FREE_MAIL = {
    "gmail.com", "yahoo.com", "aol.com", "hotmail.com", "outlook.com", "msn.com",
    "comcast.net", "att.net", "sbcglobal.net", "verizon.net", "bellsouth.net",
    "cox.net", "juno.com", "earthlink.net", "icloud.com", "me.com", "mac.com",
    "live.com", "ymail.com", "protonmail.com",
}

_BROKEN_SCHEME_RE = re.compile(r"^\s*https?:\s*/*\s*")  # "https: eoydc.org"
_WWW_RE = re.compile(r"^w{2,}\d*\.")  # "wwww.", "www2."
_EMAIL_RE = re.compile(r"^[^\s/@]+@([^\s/@]+\.[a-z]{2,})$", re.I)


# --- normalization ------------------------------------------------------------
def normalize_website(url):
    """Bare host used to identify a website, or '' when it identifies nothing."""
    if not isinstance(url, str):
        return ""
    # Hosts never contain whitespace; GT has "www. tworiversymca. org".
    url = re.sub(r"\s+", "", url.lower())
    url = _BROKEN_SCHEME_RE.sub("", url)
    if not url:
        return ""
    host = urlparse(url if "://" in url else f"http://{url}").netloc
    host = host.rsplit("@", 1)[-1]  # "info@bookwormgardens.org" -> the domain
    host = _WWW_RE.sub("", host).split(":")[0].rstrip("/")
    # A platform domain identifies a platform and a key without a dot ("na",
    # "none" — both in the GT data) identifies nothing.
    if "." not in host or host in PLATFORM_DOMAINS or host in FREE_MAIL:
        return ""
    return host


def email_domain(url):
    """Domain of an email address typed into a website field, else ''."""
    if not isinstance(url, str):
        return ""
    match = _EMAIL_RE.match(re.sub(r"\s+", "", url))
    return match.group(1).lower() if match else ""


# --- liveness and redirects ---------------------------------------------------
# The question worth asking is "will the scraper get this page", so the probe
# sends what the scraper sends. Copied from the client in
# vdl_tools/scrape_enrich/scraper/async_scraper.py __aenter__, which builds it
# inline rather than exporting it; replace this with an import if that changes.
# Servers do discriminate: abetterchance.org returns 410 to a bare client and
# 403 to a browser, and 410 is the one we count as dead.
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}
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
            verify=VERIFY_SSL, headers=SCRAPER_HEADERS,
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
        "final_domain": normalize_website(resolve_redirect(domain)) or domain,
        "alive": url_alive(domain),
        "checked": dt.date.today().isoformat(),
    }


def load_domain_cache(paths):
    """``domain -> {final_domain, alive, checked}`` ({} when absent)."""
    path = pathlib.Path(str(paths.get("website_domain_cache", "")))
    return json.loads(path.read_text()) if path.is_file() else {}


CACHE_VALID_DAYS = 90  # same validity window as vdl-tools' S3 Cache
SAVE_EVERY = 500
MAX_WORKERS = 32


def _save_cache(cache, path):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, indent=1, sort_keys=True))
    tmp.replace(path)


def resolve_domains(domains, path, refresh=False, max_workers=MAX_WORKERS):
    """Check every domain not yet (validly) cached; return the whole cache."""
    path = pathlib.Path(path)
    cache = load_domain_cache({"website_domain_cache": path})
    cutoff = (dt.date.today() - dt.timedelta(days=CACHE_VALID_DAYS)).isoformat()
    todo = sorted(
        d for d in domains if refresh or d not in cache or cache[d]["checked"] < cutoff
    )
    logger.info(
        "Websites: %d domain(s), %d already cached, %d to check",
        len(domains), len(domains) - len(todo), len(todo),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(check_domain, d): d for d in todo}
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            cache[futures[future]] = future.result()
            if i % SAVE_EVERY == 0:
                _save_cache(cache, path)
                logger.info("Websites: checked %d/%d", i, len(todo))
    _save_cache(cache, path)
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


def resolve_for_frame(df, paths, refresh=False):
    """Resolve every website domain in ``df``; returns the whole cache."""
    cols = [c for c in WEBSITE_COLS if c in df.columns]
    domains = {k for col in cols for k in df[col].map(normalize_website)} - {""}

    cache = resolve_domains(domains, paths["website_domain_cache"], refresh=refresh)
    ours = [(d, cache[d]) for d in domains if d in cache]
    logger.info(
        "Websites: %d domain(s) — %d redirect elsewhere, %d dead. Cache: %s",
        len(ours),
        sum(entry["final_domain"] != d for d, entry in ours),
        sum(not entry["alive"] for _, entry in ours),
        paths["website_domain_cache"],
    )
    return cache


# --- evidence -----------------------------------------------------------------
# What the summariser actually read, keyed as the pipeline keys its summaries.
SCRAPE_KEY_CHUNK = 1000


def combined_texts(keys, session):
    """``extracted_website_key -> (combined_text, num_errors)`` from the scrape cache."""
    from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
        WebPagesParsed,
    )

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
    site = df["Website"] if "Website" in df.columns else df["Website_cb_cd"]
    keys = df.get("extracted_website_key", pd.Series("", index=df.index)).fillna("")
    domain = site.map(normalize_website)
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
        "has_summary": df.get("Website Summary", "").fillna("").str.strip().ne(""),
        "parked_summary": df.get(
            "Website Summary", pd.Series("", index=df.index)
        ).fillna("").map(_is_parked_summary),
        "gambling": [
            _is_gambling(summary, f"{name} {desc} {desc990}")
            for summary, name, desc, desc990 in zip(
                df.get("Website Summary", pd.Series("", index=df.index)).fillna(""),
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


# --- junk rules ---------------------------------------------------------------
# Which evidence a rule may read depends on whether the page was readable.
#
# Readable page (a casino, a marketplace): the summary is a faithful rendering
# of it, so gambling and parked may be detected from the summary.
#
# Unreadable page (binary, a bot wall, an empty page): the summarizer sometimes
# invents a description from the org's NAME instead of reporting it saw
# nothing, so the summary is evidence of nothing. Those rules read the scraped
# page text only. A fabrication is always shaped like the org — a squatted
# nonprofit yields an invented nonprofit description, never an invented casino
# — which is why this asymmetry holds rather than being a coincidence.
# Category words every gambling site carries. Deliberately NOT specific game or
# brand names: those would fit this dataset rather than the problem.
GAMBLING_PHRASES = [
    "online gambling", "gambling platform", "online gaming platform", "slot game",
    "slot machine", "live casino", "online casino", "casino game", "sports betting",
    "betting platform", "online betting", "online lottery", "online poker",
    "igaming", "wagering",
]
# If the org's own records mention gaming, a gaming website is correct — that is
# what separates a real casino company from a squatted nonprofit domain.
# "lottery" is missing on purpose: in an education dataset it means admissions.
# Amistad Academy's summary says "blind lottery admissions", Lynn Public Schools
# "lottery systems" for enrollment. Adding single words took 66 flags to 218,
# almost all of them schools.
OWN_GAMING_WORDS = ["casino", "gaming", "gambling", "wager", "lottery", "betting"]

# Marketplace names, unambiguous enough to match against summary prose.
PARKED_SUMMARY_MARKERS = [
    "expireddomains", "hugedomains", "sedo.com", "squadhelp", "afternic",
    "domain marketplace", "domain for sale", "domains for sale",
]

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

TEXT_CHARS = 700


def _clip(value, limit=TEXT_CHARS):
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).strip()[:limit]


def _location(row):
    """Where the entity is, from whichever column this stage of the pipeline has.

    ``City`` is produced by geotagging, which runs after this gate, so the raw
    ``hq_address`` is what is actually available here: "College Point, New York"
    from Crunchbase, "5 VERTI DRIVE WINSLOW ME 04901" from a 990.
    """
    for col in ("hq_address", "City"):
        value = _clip(row.get(col), 120)
        if not value:
            continue
        return re.sub(r",?\s*(United States|North America)", "", value).strip(" ,")
    return ""


def _is_parked_summary(summary):
    """The summarizer naming a marketplace the page copy does not.

    handwritingcounts.com renders as "Fresh Listings Daily / Powerful Domain
    Insights", which matches no marker, but the summary says "primarily
    associated with ExpiredDomains.com and GoDaddy".
    """
    text = (summary or "").lower()
    return any(m in text for m in PARKED_SUMMARY_MARKERS)


def _is_gambling(summary, records):
    """A gambling site on a domain whose owner has nothing to do with gambling.

    ``records`` should include the org's name as well as its descriptions: 852
    orgs (all Crunchbase) have no description at all, and for those the name is
    the only thing standing between a real gaming company and a blanked summary.
    """
    text = (summary or "").lower()
    if not any(phrase in text for phrase in GAMBLING_PHRASES):
        return False
    own = (records or "").lower()
    return not any(word in own for word in OWN_GAMING_WORDS)


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


def load_website_decisions(paths, present=None):
    """``{id: action}`` from ``paths["website_quality_decisions"]``.

    ``keep`` vetoes whatever the rules concluded, which is the only way to
    overrule them: every other rule here fires automatically. Ids are matched as
    typed and, for a bare EIN, under the ``givingtuesday_`` prefix as well, since
    both forms get written by hand.
    """
    path = pathlib.Path(str(paths.get("website_quality_decisions", "")))
    if not path.is_file():
        return {}
    df = pd.read_csv(path, dtype=str).fillna("")
    decisions, ignored = {}, []
    for raw, action in zip(df.get("id", []), df.get("action", [])):
        raw, action = str(raw).strip(), str(action).strip().lower()
        if not raw or action not in DECISIONS:
            ignored.append((raw, action))
            continue
        key = raw if present is None or raw in present else f"{GT_UID_PREFIX}{raw}"
        if present is not None and key not in present:
            ignored.append((raw, "no such organization"))
            continue
        decisions[key] = action
    if ignored:
        logger.warning(
            "Website quality: ignoring %d curated row(s) in %s: %s",
            len(ignored), path.name,
            ", ".join(f"{i!r} ({why})" for i, why in ignored[:8]),
        )
    logger.info("Website quality: %d curated decision(s) from %s", len(decisions), path)
    return decisions


def gate_websites(df, paths, id_col="id", resolve=True, dry_run=False, fresh_check=True):
    """Find the junk websites by rule and act on them, before they spread.

    Called from ``run_pipeline`` (and ed_tracker's ``process_enrich``) after the
    website summaries are built and before ``add_summary_of_summaries``. Returns ``(df, decisions)``.

    ``fresh_check`` reprobes every domain with scraped text, bypassing the
    90-day liveness cache, since the public link should only show when the
    site is confirmed up right now. Scoped to that chars>0 population, not
    the whole ~25k-domain corpus, to bound the added runtime.
    """
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    cache = resolve_for_frame(df, paths) if resolve else load_domain_cache(paths)
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
    manual = load_website_decisions(paths, set(df[id_col].astype(str)))
    decisions = decide_actions(evidence, manual, id_col=id_col)

    if paths.get("website_quality_review"):
        write_website_review(decisions, paths["website_quality_review"])
    if dry_run:
        logger.warning("Website quality: dry run — nothing changed")
        return df, decisions
    return apply_website_gate(df, decisions, id_col=id_col), decisions
