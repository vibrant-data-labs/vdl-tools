"""Turn Firecrawl markdown into the text the summarizer reads.

Firecrawl returns the whole rendered page as markdown: navigation menus as
bulleted link lists, duplicated responsive headers, footers, image markup and
a URL inside every link. The rules here strip that down to what in-house text
always was, decide whether a page is junk before the stripping can hide the
evidence, and build one org's combined text from its pages. Why each rule
exists, with the measurements behind it, is in section 5 of
``docs/FIRECRAWL_ADAPTER_PLAN.md``; the numbers this version was checked
against are in this package's README under "Phase 0 rescore".

Order of operations. The caller has already applied the status gate: only a
page whose ``metadata.statusCode`` is 2xx reaches ``page_text``.

Per page, ``page_text(markdown)``:

1. ``markdown_to_plain``: images and URLs out, link labels kept. Only the
   markers read it.
2. Markers on that plain text: garbled, parked, bot walls, and a page that is
   nothing but a browser blocked-frame error. Browser chrome (blocked-frame
   error pages, captcha widgets and notices) is removed first, since it is
   not the site's text and a reCAPTCHA form on a short page reads as a wall.
   The markers run once, before cleaning: dropping menu-only lines deletes
   labels such as "Buy this domain" and lets a parked page through, and
   running the wall markers again on cleaned text calls real pages walls once
   cleaning has made them short.
3. ``clean_markdown``: the stored, versioned per-page text (``parsed_html``).
4. The thin check, on the cleaned text, by length alone.

Per org, ``org_text(source, pages)``:

1. Home page first, then subpages by ``deterministic_len_key``; only pages
   whose verdict is ``ok`` or ``thin`` are combined.
2. Cross-page dedupe: each normalized line is kept at its first occurrence.
3. ``make_group_text``, unchanged, then a cap of ``ORG_TOKEN_CAP`` tokens cut
   from the end.
4. Verdict: a junk home page (parked, blocked, garbled) makes the org junk;
   otherwise the length check on the combined text. The markers already ran
   on every page, so they are not run again on the combined text.

Every function is pure and orders its output without iterating a set, so the
same pages give byte-identical text on every run and machine. That is what
lets the hash-keyed LLM cache hit. Any change to a rule's output bumps
``CLEANER_VERSION``, which changes ``EXTRACTOR`` on every row it produces.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

from vdl_tools.scrape_enrich.scraper.text_quality import THIN_TEXT_CHARS, classify_text_quality
from vdl_tools.shared_tools.openai.openai_api_utils import get_num_tokens, truncate_text
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text
from vdl_tools.shared_tools.web_summarization.make_page_text import MODEL, _make_full_url, make_group_text
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import deterministic_len_key

CLEANER_VERSION = 1
# Cleaner, dedupe and cap ship under one name and are bumped together, so the
# LLM caches bust once per version rather than once per tweak.
EXTRACTOR = f"firecrawl-md-v{CLEANER_VERSION}"

# Per-org cap on the combined text, in tokens of the summarizer's encoding.
# In-house combined text has a median near 2.5k tokens; 8k covers the largest
# real organizations and bounds the pathological ones (one pilot site was
# 1.01 million tokens raw).
ORG_TOKEN_CAP = 8000

# What scrape_websites_psql counts the combined text against when no summary
# prompt is given. With the cap above the prompt never changes the output.
COUNTING_PROMPT = "test prompt " * 100

# Pages with these verdicts feed the combined text; the same two verdicts are
# the only ones summarize_scraped_df lets reach the LLM.
COMBINABLE_VERDICTS = ("ok", "thin")
# A home page with one of these verdicts makes the whole org that verdict.
JUNK_VERDICTS = ("parked", "blocked", "garbled")


# ---------------------------------------------------------------------------
# Markdown patterns
# ---------------------------------------------------------------------------

# The target of a link or image: (url) or (url "title"), one level of nested
# parentheses allowed, as in Wikipedia-style URLs. A target never spans lines,
# which also keeps an unclosed "(" from scanning to the end of a huge page.
_TARGET = r"\((?:[^()\n]|\([^()\n]*\))*\)"
_IMAGE = re.compile(r"!\[[^\]]*\]" + _TARGET)
_LINK = re.compile(r"\[([^\]]*)\]" + _TARGET)
# A line that is nothing but links: a menu item, a breadcrumb, a footer row,
# an image link whose image is already gone. An optional list marker in front,
# separators between. Labels may not span lines here, so a multi-line link is
# never taken for a menu.
_LINE_LINK = r"\[[^\]\n]*\]" + _TARGET
_MENU_LINE = re.compile(
    rf"^[ \t]*(?:(?:[-*+]|\d+[.)])[ \t]+)?(?:{_LINE_LINK}[ \t]*(?:[|·•/,–-][ \t]*)?)+$"
)
# Bare and autolinked URLs, including the JSON-escaped https:\/\/ form that
# inline script data leaves in the text.
_BARE_URL = re.compile(r"<?https?:(?:\\?/){2}[^\s<>()\[\]\"]+>?", re.IGNORECASE)
# Line-start markup: headings, quotes, list bullets, table pipes, rules.
_LINE_MARKUP = re.compile(r"^[ \t]*[#>*\-+|=]+[ \t]*", re.MULTILINE)
# Backslash escapes the HTML-to-markdown converter adds: ERR\_BLOCKED, \| and so on.
_ESCAPE = re.compile(r"\\([\\`*_{}\[\]()#+\-.!|<>~])")
# Any whitespace but a newline: non-breaking and thin spaces included. Half the
# phase 2 pages carry them, and clean_scraped_text deletes non-ASCII characters
# without first turning these into spaces, which glued words together
# ("formed in 2017focused").
_NON_NEWLINE_SPACE = re.compile(r"[^\S\n]")
# The converter's <br>: a backslash at the end of a line, often alone on it.
_HARD_BREAK = re.compile(r"[ \t]*\\+[ \t]*$", re.MULTILINE)
# Bold and underline markers. Single * and _ stay: they are also bullets and
# word characters.
_EMPHASIS = re.compile(r"\*\*|__")
_BLANK_OR_MARKUP_ONLY = re.compile(r"^[ \t#>*\-+|=_.·•]*$")

# Chrome's error page for a frame or navigation it refused. Firecrawl's browser
# runs an ad blocker, so the page shows up in two shapes: inside a real page,
# where an embedded frame (shop.app, an analytics loader) was refused, and as
# the whole page, when a parked domain redirects to an ad host. Phase 2: 363
# pages carried the ERR_BLOCKED_BY_CLIENT version, 356 of them real pages,
# plus ERR_BLOCKED_BY_RESPONSE ("refused to connect", a frame policy) and a
# "blocked by Chromium" version. All share one shape:
#
#     host                                    optional, same host as below
#     # host is blocked
#     one to a few short message lines        "This page has been blocked by an extension"
#     ERR_BLOCKED_BY_<REASON>
#     Reload                                  optional
#     a message line repeated                 optional
_BLOCKED_HEADING = re.compile(r"^#{1,6}[ \t]*(\S+)[ \t]+is blocked$", re.IGNORECASE)
_BLOCKED_CODE = re.compile(r"^ERR\\?_BLOCKED\\?_BY\\?_[A-Z_\\]+$", re.IGNORECASE)
_RELOAD = re.compile(r"^Reload$", re.IGNORECASE)
_BLOCK_MAX_MESSAGE_LINES = 4
_BLOCK_MAX_MESSAGE_CHARS = 200

# Captcha widgets and notices rendered into the page: a form's "reCAPTCHA /
# Recaptcha requires verification. / I'm not a robot", "protected by
# reCAPTCHA" footers, cookie-banner entries for _grecaptcha, hCaptcha's
# equivalents. 1,389 phase 2 pages carry the widget; on a short page the
# "captcha" wall marker took it for a challenge screen (8 real pages, two of
# them home pages). Challenge screens are caught by their own wording
# ("checking the site connection security", "verify you are human"), which
# these lines never contain.
_CAPTCHA_CHROME = re.compile(r"recaptcha|hcaptcha|^\W*(?:captcha|i'?m not a robot|i am not a robot)\W*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Per page
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PageText:
    """One page after the gate and the cleaner."""

    verdict: str   # classify_text_quality vocabulary: ok, thin, parked, blocked, garbled, empty
    text: str      # cleaned text, stored as parsed_html; kept for junk pages too


def _next_nonblank(lines: list[str], start: int) -> int | None:
    for k in range(start, len(lines)):
        if lines[k].strip():
            return k
    return None


def strip_blocked_frames(markdown: str | None) -> tuple[str, int]:
    """``markdown`` without Chrome's blocked-frame error pages, and how many were removed."""
    if not markdown:
        return "", 0
    lines = markdown.split("\n")
    drop = [False] * len(lines)
    n_blocks = 0
    i = 0
    while i < len(lines):
        heading = _BLOCKED_HEADING.match(lines[i].strip())
        if not heading:
            i += 1
            continue
        host, messages, code_at = heading.group(1), [], None
        k = _next_nonblank(lines, i + 1)
        while k is not None and len(messages) < _BLOCK_MAX_MESSAGE_LINES:
            line = lines[k].strip()
            if _BLOCKED_CODE.match(line):
                code_at = k
                break
            if len(line) > _BLOCK_MAX_MESSAGE_CHARS:
                break
            messages.append(line)
            k = _next_nonblank(lines, k + 1)
        if code_at is None or not messages:
            i += 1
            continue
        start, end = i, code_at
        above = i - 1
        while above >= 0 and not lines[above].strip():
            above -= 1
        if above >= 0 and lines[above].strip() == host:
            start = above
        below = _next_nonblank(lines, end + 1)
        if below is not None and _RELOAD.match(lines[below].strip()):
            end = below
            below = _next_nonblank(lines, end + 1)
        if below is not None and lines[below].strip() in messages:
            end = below
        for x in range(start, end + 1):
            drop[x] = True
        n_blocks += 1
        i = end + 1
    if not n_blocks:
        return markdown, 0
    return "\n".join(line for line, d in zip(lines, drop) if not d), n_blocks


def strip_browser_chrome(markdown: str | None) -> tuple[str, int]:
    """``markdown`` without blocked-frame error pages and captcha widget lines,
    and how many blocked-frame error pages were removed."""
    text, n_frames = strip_blocked_frames(markdown)
    lines = text.split("\n")
    kept = [line for line in lines if not _CAPTCHA_CHROME.search(line.strip())]
    return (text if len(kept) == len(lines) else "\n".join(kept)), n_frames


def markdown_to_plain(markdown: str | None) -> str:
    """Markdown as plain text for the markers: images and URLs removed, link
    labels kept, line markup removed. Not stored and not summarized."""
    if not markdown:
        return ""
    text = _NON_NEWLINE_SPACE.sub(" ", markdown)
    text = _IMAGE.sub(" ", text)
    text = _LINK.sub(r"\1", text)
    text = _BARE_URL.sub(" ", text)
    text = _ESCAPE.sub(r"\1", text)
    text = _LINE_MARKUP.sub("", text)
    return re.sub(r"[ \t]+", " ", text).strip()


def clean_markdown(markdown: str | None) -> str:
    """The per-page text the summarizer reads (``parsed_html``), version ``CLEANER_VERSION``.

    1. Turn non-breaking and other Unicode spaces into plain spaces.
    2. Drop browser chrome: blocked-frame error pages, captcha widgets and notices.
    3. Drop image markup; alt text is file names and the URL is dozens of tokens.
    4. Drop menu-only lines: lines that are nothing but links.
    5. Collapse every remaining ``[label](url)`` to ``label``.
    6. Drop bare URLs, hard line breaks, bold and underline markers, and the
       converter's backslash escapes.
    7. Drop blank and markup-only lines, run ``clean_scraped_text`` (the
       normalization in-house text already passes through), strip each line.
    """
    if not markdown:
        return ""
    text = _NON_NEWLINE_SPACE.sub(" ", markdown)
    text, _ = strip_browser_chrome(text)
    text = _IMAGE.sub("", text)
    text = "\n".join(line for line in text.split("\n") if not _MENU_LINE.match(line))
    text = _LINK.sub(r"\1", text)
    text = _BARE_URL.sub("", text)
    text = _HARD_BREAK.sub("", text)
    text = _EMPHASIS.sub("", text)
    text = _ESCAPE.sub(r"\1", text)
    text = "\n".join(line for line in text.split("\n") if not _BLANK_OR_MARKUP_ONLY.match(line))
    text = clean_scraped_text(text)
    lines = (line.strip() for line in text.split("\n"))
    return "\n".join(line for line in lines if line and not _BLANK_OR_MARKUP_ONLY.match(line))


def length_verdict(text: str | None) -> str:
    """empty, thin or ok by length alone, with ``text_quality``'s threshold."""
    stripped = (text or "").strip()
    if not stripped:
        return "empty"
    return "thin" if len(stripped) < THIN_TEXT_CHARS else "ok"


def marker_verdict(markdown: str | None) -> str | None:
    """garbled, parked or blocked from markers on the page's plain text, else None."""
    text, n_frames = strip_browser_chrome(_NON_NEWLINE_SPACE.sub(" ", markdown or ""))
    plain = markdown_to_plain(text)
    verdict = classify_text_quality(plain)
    if verdict in ("garbled", "parked"):
        return verdict
    # The error page is the page, not a frame inside it, when almost nothing
    # is left once it is removed.
    if n_frames and len(plain) < THIN_TEXT_CHARS:
        return "blocked"
    if verdict == "blocked":
        return "blocked"
    return None


def page_text(markdown: str | None) -> PageText:
    """Gate, then clean, one 2xx page's markdown."""
    cleaned = clean_markdown(markdown)
    return PageText(marker_verdict(markdown) or length_verdict(cleaned), cleaned)


# ---------------------------------------------------------------------------
# Per org
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OrgPage:
    """One of an org's pages from this run, after ``page_text``."""

    subpath: str     # '/' for the home page, else the subpath as stored
    is_home: bool
    verdict: str
    text: str


@dataclass(frozen=True)
class OrgText:
    """An org's combined text and what it was built from."""

    text: str
    verdict: str
    subpaths: list = field(default_factory=list)   # pages whose text is in ``text``, in order (source_keys)
    tokens: int = 0                                 # after the cap
    tokens_before_cap: int = 0
    extractor: str = EXTRACTOR

    @property
    def truncated(self) -> bool:
        return self.tokens_before_cap > self.tokens


def _page_order(page: OrgPage):
    return (0 if page.is_home else 1, deterministic_len_key(page.subpath))


def _line_key(line: str) -> str:
    return re.sub(r"\s+", " ", _LINE_MARKUP.sub("", line)).strip().lower()


def dedupe_lines(texts: Iterable[str]) -> list[str]:
    """Each text with every line already seen, in it or an earlier text, removed.

    Lines are compared lowercased, with whitespace collapsed and line-start
    markup ignored, so "# About Us" on one page and "About us" on the next are
    the same line. The texts must already be in combine order.
    """
    seen: set[str] = set()
    out = []
    for text in texts:
        kept = []
        for line in (text or "").split("\n"):
            key = _line_key(line)
            if not key or key in seen:
                continue
            seen.add(key)
            kept.append(line)
        out.append("\n".join(kept))
    return out


def count_tokens(text: str | None) -> int:
    """Tokens in the summarizer's encoding."""
    return get_num_tokens(text or "", MODEL)


def cap_tokens(text: str | None, max_tokens: int = ORG_TOKEN_CAP) -> str:
    """``text`` cut from the end to at most ``max_tokens`` tokens, then back to the
    last whitespace so the cut never splits a word."""
    text = text or ""
    cut = truncate_text(text, MODEL, max_tokens)
    if cut == text:
        return text
    boundary = max(cut.rfind("\n"), cut.rfind(" "))
    return (cut[:boundary] if boundary > 0 else cut).rstrip()


def org_text(
    source: str,
    pages: Iterable[OrgPage],
    prompt_str: str = COUNTING_PROMPT,
    max_tokens: int = ORG_TOKEN_CAP,
) -> OrgText:
    """One org's combined text from this run's pages.

    ``source`` is the home URL ``make_group_text`` puts in front of each page.
    Page order does not matter on input; it is fixed here.
    """
    pages = sorted(pages, key=_page_order)
    home = next((p for p in pages if p.is_home), None)
    usable = [p for p in pages if p.verdict in COMBINABLE_VERDICTS and p.text]
    records = []
    for page, text in zip(usable, dedupe_lines(p.text for p in usable)):
        if not text:
            continue
        # make_group_text sorts by subpath length; an empty subpath keeps the
        # home page first even next to a one-character subpage path, and gives
        # the same "URL:" header as '/'.
        records.append(({
            "type": "index" if page.is_home else "page",
            "source": source,
            "subpath": "" if page.is_home else page.subpath,
            "text": text,
        }, page.subpath))
    combined = (make_group_text(prompt_str, [r for r, _ in records]) or "") if records else ""
    tokens_before = count_tokens(combined)
    combined = cap_tokens(combined, max_tokens)
    # make_group_text skips short and repetitive pages and the cap can cut the
    # last ones off, so the lineage lists only the pages whose header survived.
    subpaths = [sp for r, sp in records if f"URL: {_make_full_url(r)}\nTEXT: " in combined]
    if home is not None and home.verdict in JUNK_VERDICTS:
        verdict = home.verdict
    else:
        verdict = length_verdict(combined)
    return OrgText(
        text=combined,
        verdict=verdict,
        subpaths=subpaths,
        tokens=count_tokens(combined),
        tokens_before_cap=tokens_before,
    )
