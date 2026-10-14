"""Reusable stage rules a pipeline spec composes into its ordered stage list.

Each factory returns a ``StageRule``: a function from ``Ctx`` to ``Stage``.
The factories cover the shapes both VDL pipelines share (a pull, a text
check, a relevance classifier with manual overrides, a label classifier, a
texts-after-scraping check, a taxonomy mapping, a cleaning step made of named
rules, membership in a published set, and the app's default visibility).
Anything pipeline-specific goes in through the arguments: column names,
thresholds, the wording of a reason.
"""
from __future__ import annotations

from collections.abc import Callable

from vdl_tools.shared_tools.why_not.spec import (
    FAIL,
    HIDDEN,
    NOTE,
    PASS,
    SKIP,
    UNKNOWN,
    Ctx,
    Stage,
    StageRule,
)

Reason = Callable[[Ctx], str | None]


def money(v) -> str:
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "unknown"


def _date(v) -> str:
    return str(v or "")[:10]


def pull_stage(
    label: str,
    *,
    key: str = "pull",
    flag: str = "in_pull",
    downstream_flags: tuple[str, ...] = ("in_meta",),
    verdict_flag: str = "has_relevance_text",
    pull_detail: Callable[[Ctx], list[str]] | None = None,
    not_pulled_detail: str = "not returned by the pull",
) -> StageRule:
    """Did the org come back from the source pull? Explains orgs that left the pull since."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, PASS)
        if ctx.flag(flag):
            s.detail = pull_detail(ctx) if pull_detail else [f"{ctx.v('source')} record {ctx.row['uid']}"]
        elif any(ctx.flag(f) for f in downstream_flags):
            s.status = NOTE
            pulled, enriched = _date(ctx.lineage.get("pull_created_at")), _date(ctx.lineage.get("meta_created_at"))
            s.detail.append(f"not in the current pull ({pulled or 'date unknown'}) but it was in the pull the "
                            f"enrichment ran on ({enriched or 'date unknown'}), so it is still in the tracker today")
            s.detail.append("it will drop out at the next full run unless the source search returns it again "
                            "(description, HQ or funding dates may have changed)")
        else:
            s.status = FAIL
            if ctx.flag(verdict_flag):
                pulled = _date(ctx.lineage.get("pull_created_at"))
                s.detail.append(f"not in the current pull{' (' + pulled + ')' if pulled else ''}, but an earlier "
                                f"pull did include it (verdict dated {ctx.v('relevance_verdict_date') or 'unknown'}): "
                                "it has since dropped out of the source search results")
            else:
                s.detail.append(not_pulled_detail)
        return s
    return rule


def text_stage(
    label: str,
    *,
    key: str = "text",
    flag: str = "has_relevance_text",
    pull_flag: str = "in_pull",
    downstream_flag: str = "in_meta",
    description_len_col: str = "description_len",
    min_description_length: int = 100,
    rerun_hint: str = "the next enrichment run will classify it",
) -> StageRule:
    """Was there text to classify? A pull newer than the enrichment means 'not yet', not 'no'."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        has = ctx.v(flag)
        if not (ctx.flag(pull_flag) or has):
            return s
        if has is None:
            s.status, s.detail = UNKNOWN, ["relevance verdicts not available"]
        elif has:
            s.status = PASS
            src = ctx.v("relevance_text_source") or "text"
            dated = f", verdict dated {ctx.v('relevance_verdict_date')}" if ctx.v("relevance_verdict_date") else ""
            s.detail.append(f"used the {src} ({int(ctx.v('relevance_text_len') or 0)} chars){dated}")
        elif ctx.lineage.get("enrichment_behind_pull") and not ctx.flag(downstream_flag):
            s.status = UNKNOWN
            s.detail.append(f"not classified yet: the pull is dated {_date(ctx.lineage.get('pull_created_at'))} but "
                            f"the enrichment stage last ran {_date(ctx.lineage.get('meta_created_at'))} on an "
                            f"earlier pull; {rerun_hint}")
        else:
            s.status = FAIL
            dlen = int(ctx.v(description_len_col) or 0)
            if dlen < min_description_length:
                s.detail.append(f"description has {dlen} chars (< {min_description_length}) and no website "
                                "summary could be scraped, so there was nothing to classify")
            else:
                s.detail.append(f"description has {dlen} chars, so text existed; the classifier call returned "
                                "no usable 0/1 (prediction error) and the org was dropped")
        return s
    return rule


def relevance_stage(
    label: str,
    *,
    key: str = "relevance",
    requires: str = "text",
    override_col: str = "relevance_override",
    override_source: str = "the manual override list",
) -> StageRule:
    """Classifier verdict, manual override, or an unusable answer."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        if not ctx.passed(requires):
            return s
        pred, prob, override = ctx.v("relevance_pred"), ctx.v("relevance_prob"), ctx.v(override_col)
        raw = ctx.v("relevance_raw")
        verdict = None
        if pred is not None:
            verdict = (f"classifier said {'relevant' if int(pred) == 1 else 'NOT relevant'}"
                       + (f" (confidence {float(prob):.2f})" if prob is not None else ""))
        elif raw is not None:
            verdict = (f"classifier returned an unusable answer {str(raw)!r}; the pipeline treats that as an "
                       "error and drops the org")
        if override is not None and int(override) == 0:
            s.status, s.detail = FAIL, [f"manually labelled NOT relevant in {override_source} (the override wins)"]
        elif override is not None and int(override) == 1:
            s.status, s.detail = PASS, [f"manually labelled relevant in {override_source}"]
        elif pred is None:
            s.status = FAIL if raw is not None else UNKNOWN
            s.detail = [] if raw is not None else ["no verdict recorded"]
        else:
            s.status = PASS if int(pred) == 1 else FAIL
        if verdict:
            s.detail.append(verdict)
        txt = ctx.v("relevance_text")
        if txt:
            src = ctx.v("relevance_text_source") or "text"
            s.detail.append(f"text seen ({src}): \"{str(txt)[:300].strip()}{'...' if len(str(txt)) > 300 else ''}\"")
        return s
    return rule


def label_stage(
    label: str,
    *,
    key: str,
    col: str,
    keep: tuple[str, ...],
    requires: str = "relevance",
    source_col: str | None = None,
    downstream_flag: str = "in_meta",
    missing_detail: str = "no prediction recorded",
) -> StageRule:
    """A classifier whose label must be in ``keep`` (e.g. adaptation / mitigation / both)."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        if not ctx.passed(requires):
            return s
        value = ctx.v(col)
        if value is None:
            s.status = UNKNOWN
            s.detail.append("in the enriched data but no label recorded" if ctx.flag(downstream_flag) else missing_detail)
        elif str(value).lower() in {k.lower() for k in keep}:
            s.status = PASS
            s.detail.append(f"{value}" + (f" (from the {ctx.v(source_col)})" if source_col and ctx.v(source_col) else ""))
        else:
            s.status = FAIL
            s.detail.append(f"classified '{value}'; only {' / '.join(keep)} are kept")
        return s
    return rule


def texts_stage(
    label: str,
    *,
    key: str = "texts",
    flag: str = "in_meta",
    requires: tuple[str, ...] = ("relevance",),
    tolerate_unknown: tuple[str, ...] = (),
    text_len_cols: dict[str, str] | None = None,
) -> StageRule:
    """Did any text survive scraping? Pass when the org reached the enriched set."""
    cols = text_len_cols or {"description": "description_len", "website summary": "website_summary_len",
                             "LinkedIn about": "about_linkedin_len"}

    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        prior_ok = all(ctx.passed(r) for r in requires)
        prior_unknown = any((ctx.prev(r) or Stage(r, "", SKIP)).status == UNKNOWN for r in tolerate_unknown)
        if not (prior_ok or (prior_unknown and ctx.flag(flag))):
            return s
        if ctx.flag(flag):
            srcs = [n for n, c in cols.items() if (ctx.v(c) or 0) > 0]
            s.status, s.detail = PASS, [f"text sources: {', '.join(srcs) or 'none recorded'}"]
        elif prior_ok:
            s.status, s.detail = FAIL, ["no description, website summary or LinkedIn text survived scraping"]
        return s
    return rule


def taxonomy_stage(
    label: str,
    *,
    key: str = "taxonomy",
    flag: str = "in_meta",
    levels: dict[str, tuple[str, ...]],
    nomatch: tuple[str, ...] = ("No Match",),
    drop: bool = False,
    drop_detail: str = "no taxonomy match; the cleaning step removes such orgs",
    drop_when_missing: dict[str, str] | None = None,
    nomatch_detail: str = "No Match on every taxonomy: stays in the tracker but appears under no filter",
) -> StageRule:
    """Taxonomy assignment.

    ``drop`` makes "nothing matched anywhere" a FAIL; ``drop_when_missing``
    maps a level name to the detail to report when that level alone is empty
    (a pipeline that drops orgs with a pillar but no sub-pillar). Otherwise an
    unmatched org is a NOTE.
    """
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        if not ctx.flag(flag):
            return s
        matched_any, missing = False, []
        for name, cols in levels.items():
            vals = [str(ctx.v(c)) for c in cols if ctx.v(c) not in (None, "", "[]")]
            vals = [v for v in vals if v not in nomatch]
            s.detail.append(f"{name}: {' > '.join(vals) if vals else 'not mapped'}")
            matched_any = matched_any or bool(vals)
            if not vals:
                missing.append(name)
        required_missing = [m for m in missing if m in (drop_when_missing or {})]
        if required_missing and matched_any:
            s.status, s.detail = FAIL, [drop_when_missing[required_missing[0]], *s.detail]
        elif matched_any:
            s.status = PASS
        elif drop or required_missing:
            s.status, s.detail = FAIL, [drop_detail if drop else drop_when_missing[required_missing[0]], *s.detail]
        else:
            s.status, s.detail = NOTE, s.detail + [nomatch_detail]
        return s
    return rule


def rules_stage(
    label: str,
    *,
    key: str = "cleaning",
    flag: str = "in_meta",
    reasons: list[Reason],
    pass_detail: str = "passed every cleaning filter",
) -> StageRule:
    """A cleaning step: every reason that fires is a detail line; any reason means FAIL."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        if not ctx.flag(flag):
            return s
        fired = [r for r in (fn(ctx) for fn in reasons) if r]
        s.status = FAIL if fired else PASS
        s.detail = fired or [pass_detail]
        return s
    return rule


def membership_stage(
    label: str,
    *,
    key: str,
    flag: str,
    upstream_flag: str = "in_meta",
    pass_detail: Callable[[Ctx], str] | None = None,
    unexplained_detail: str = ("not in the published set and no removal recorded: the artifacts are probably "
                               "from different runs (compare the dates in the header)"),
) -> StageRule:
    """In the published set? When not, point at the cleaning reasons or flag the vintage gap."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        present = ctx.v(flag)
        if present is None:
            s.status, s.detail = UNKNOWN, ["artifact not available"]
        elif present:
            s.status, s.detail = PASS, [pass_detail(ctx) if pass_detail else "present"]
        elif ctx.flag(upstream_flag):
            s.status = FAIL
            prior_fail = next((st for st in ctx.stages if st.status == FAIL), None)
            merged = ctx.v("merged_into")
            if merged:
                s.detail = [f"merged into `{merged}` by the dedup step (same normalized name); its funding and "
                            "texts live on that record"]
            elif prior_fail is not None:
                s.detail = [f"removed at '{prior_fail.key}' (above)"]
            else:
                s.detail = [unexplained_detail]
        return s
    return rule


def visible_stage(
    label: str,
    *,
    key: str = "visible",
    flag: str = "in_network",
    visible: Callable[[Ctx], tuple[bool, str]],
) -> StageRule:
    """Shown under the app's default filters? ``visible`` returns (shown, explanation)."""
    def rule(ctx: Ctx) -> Stage:
        s = Stage(key, label, SKIP)
        if not ctx.flag(flag):
            return s
        shown, why = visible(ctx)
        s.status, s.detail = (PASS if shown else HIDDEN), [why]
        return s
    return rule
