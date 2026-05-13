"""
Tier 1 analysis — diagnose where in the taxonomy errors concentrate
====================================================================

Reads a judge's scored xlsx/csv (and, where available, the matching
verdicts + per-row classification files) and writes a markdown report
covering the seven analysis sections you'd otherwise build by hand
during a prompt-iteration cycle:

    1. Pillar × Level error table          (judge + per-row)
    2. Per-Sub-Pillar error breakdown      (judge + per-row)
    3. Top "bad"-firing nodes by level     (judge only)
    4. Top "weak"-firing nodes by level    (judge only)
    5. Failure-reason clustering           (judge only)
    6. Pillar / Sector alignment breakdown (verdicts)
    7. NoMatch entities in this sample     (per-row only)

Designed for the **cheap inner-iteration loop**: judge-driven, no
full-pool classification required. When the judge-only signal
saturates (or to find zero-match Solutions at scale), run the
companion Tier-2 tool ``analyze_full_coverage.py`` against a full-
pool classification output.

Inputs are auto-detected from the standard
``hierarchical_taxonomy_judge`` output naming, so a single
``--scored`` flag suffices for the common case::

    python analyze_judge_results.py --scored sample200_seed42_quality_scored.xlsx

The companion verdicts (`*_quality_<pillar|sector>_verdicts.xlsx`)
and per-row classification (`<stem-without-_quality_scored>.xlsx`)
files are discovered from the stem. Pass them explicitly with
``--verdicts`` / ``--per-row`` when the naming differs.

Output: a markdown file written next to the scored input, with name
derived by replacing ``_quality_scored`` with
``_quality_analysis.md``. Pass ``--html`` to also render an HTML
copy via the local ``render_report``-style template (embedded — no
external CSS).

The tool works with any project whose classifier/judge follow the
shared engine conventions: OE, OELoC, Drawdown, hopper_dean.
"""

from __future__ import annotations

import argparse
import ast
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# I/O helpers (CSV / XLSX dispatch by extension)
# ---------------------------------------------------------------------------

def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_excel(path)


# ---------------------------------------------------------------------------
# Auto-detect verdict + per-row paths from the scored stem
# ---------------------------------------------------------------------------

def _find_companion_paths(scored_path: Path) -> tuple[Path | None, Path | None, str]:
    """Locate the verdicts and per-row files that pair with ``scored_path``.

    Standard naming (from ``hierarchical_taxonomy_judge``):
        scored:    {stem}_quality_scored.{ext}
        verdicts:  {stem}_quality_{pillar|sector}_verdicts.{ext}
        per-row:   {stem}.{ext}

    Returns ``(verdicts_path, per_row_path, top_label)`` where
    ``top_label`` is "pillar" or "sector" inferred from which verdicts
    naming exists, defaulting to "pillar" when neither is found.
    """
    stem = scored_path.stem
    suffix = scored_path.suffix
    base = stem.replace("_quality_scored", "")

    # Try pillar-style first, then sector-style.
    verdicts_pillar = scored_path.with_name(f"{base}_quality_pillar_verdicts{suffix}")
    verdicts_sector = scored_path.with_name(f"{base}_quality_sector_verdicts{suffix}")
    if verdicts_pillar.exists():
        verdicts_path: Path | None = verdicts_pillar
        top_label = "pillar"
    elif verdicts_sector.exists():
        verdicts_path = verdicts_sector
        top_label = "sector"
    else:
        verdicts_path = None
        top_label = "pillar"

    per_row_candidate = scored_path.with_name(f"{base}{suffix}")
    per_row_path = per_row_candidate if per_row_candidate.exists() else None

    return verdicts_path, per_row_path, top_label


# ---------------------------------------------------------------------------
# Schema detection
# ---------------------------------------------------------------------------

# Possible id / name column names across the four known projects.
# Listed in priority order — first match wins.
ID_COL_CANDIDATES = ("uid", "abstract_id")
NAME_COL_CANDIDATES = ("Name", "title")
DESCRIPTION_COL_CANDIDATES = ("Description", "abstract")


def _detect_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    """Return the first candidate column present in ``df``."""
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(
        f"None of the expected columns {candidates} found in dataframe; "
        f"available columns: {list(df.columns)[:20]}{'...' if len(df.columns) > 20 else ''}"
    )


def _level_order_from_scored(scored_df: pd.DataFrame) -> list[str]:
    """Return the levels in walk order, inferring from common conventions.

    The scored xlsx has a ``level`` column with values like
    "Pillar", "SubPillar", "Solution", "SubTerm" — or "Sector",
    "SectorCluster", "Solution", "Activity" — or "Pillar",
    "Sub-Pillar", "Solution", "Sub-Term" (hopper_dean). We don't
    know the project-specific ordering from data alone, so use a
    priority list with the broadest of the known projects' shapes.
    """
    levels_present = set(scored_df["level"].dropna().unique())
    candidate_orders = [
        # OE / LoC style
        ["Pillar", "SubPillar", "Solution", "SubTerm"],
        # Drawdown style
        ["Sector", "SectorCluster", "Solution", "Activity"],
        # hopper_dean style
        ["Pillar", "Sub-Pillar", "Solution", "Sub-Term"],
        # LoC 3-level style
        ["Pillar", "Solution", "SubTerm"],
    ]
    for order in candidate_orders:
        if levels_present <= set(order):
            return [lvl for lvl in order if lvl in levels_present]
    # Unknown taxonomy — fall back to first-appearance order.
    seen: list[str] = []
    for lvl in scored_df["level"]:
        if pd.notna(lvl) and lvl not in seen:
            seen.append(lvl)
    return seen


def _parse_repr_list(cell: Any) -> list[str]:
    """Decode a repr()-encoded list cell to a list of strings."""
    if cell is None:
        return []
    if isinstance(cell, float) and pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s or s == "[]":
        return []
    try:
        v = ast.literal_eval(s)
    except (ValueError, SyntaxError):
        # Plain string cell (not a list).
        return [s] if s else []
    if isinstance(v, str):
        return [v]
    if not isinstance(v, (list, tuple)):
        return []
    return [str(x).strip() for x in v if str(x).strip()]


# ---------------------------------------------------------------------------
# Per-entity context — what pillar / sub-pillar does each entity belong to?
# ---------------------------------------------------------------------------

def _build_entity_context(
    per_row_df: pd.DataFrame,
    level_order: list[str],
    id_col: str,
) -> dict[str, dict[str, list[str]]]:
    """Return ``{entity_id: {level_label: [matched values]}}``.

    For each entity, aggregate the unique non-empty values across the
    level columns in ``per_row_df``. Handles both single-string cells
    (per-row format) and repr-encoded list cells (collapsed format).
    """
    # Identify per-row columns corresponding to each level. The
    # classifier output uses different label conventions for the
    # level column vs the per-row column header (e.g. "SubPillar"
    # vs "Sub-Pillar" in OE). Try a few permutations.
    def find_col(level_label: str) -> str | None:
        for cand in (level_label,
                     level_label.replace("Sub", "Sub-"),
                     level_label.replace("Sub-", "Sub"),
                     level_label.replace("Cluster", "Cluster")):
            if cand in per_row_df.columns:
                return cand
        return None

    level_cols = {lvl: find_col(lvl) for lvl in level_order}

    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, r in per_row_df.iterrows():
        eid = r[id_col]
        for lvl, col in level_cols.items():
            if col is None:
                continue
            v = r.get(col)
            for s in _parse_repr_list(v):
                out[eid][lvl].add(s)
    return {eid: {lvl: sorted(vals) for lvl, vals in d.items()} for eid, d in out.items()}


# ---------------------------------------------------------------------------
# Section 1 — Pillar × Level error breakdown
# ---------------------------------------------------------------------------

SCORE_NUMERIC = {"good": 1.0, "weak": 0.5, "bad": 0.0}


def pillar_level_breakdown(
    scored_df: pd.DataFrame,
    entity_ctx: dict[str, dict[str, list[str]]],
    level_order: list[str],
    id_col: str,
    top_level: str,
) -> pd.DataFrame:
    """Aggregate per-match scores by (entity's top-level node, match's level).

    For each scored row, attribute the match to all of the entity's
    top-level pillars / sectors. Aggregate to produce
    (top_node, level) → counts + mean.
    """
    rows: list[dict[str, Any]] = []
    bucket: dict[tuple[str, str], Counter] = defaultdict(Counter)
    for _, r in scored_df.iterrows():
        eid = r[id_col]
        lvl = r["level"]
        score = r["score"]
        if pd.isna(score):
            continue
        if lvl == top_level:
            # The match itself is at the top level — attribute to its
            # own name.
            tops = [str(r["match"]).strip()]
        else:
            tops = entity_ctx.get(eid, {}).get(top_level, [])
        for top in tops:
            bucket[(top, lvl)][score] += 1

    for (top, lvl), counts in bucket.items():
        n = sum(counts.values())
        if n == 0:
            continue
        good = counts["good"]; weak = counts["weak"]; bad = counts["bad"]
        rows.append({
            top_level: top,
            "level": lvl,
            "n_matches": n,
            "good": int(good),
            "weak": int(weak),
            "bad": int(bad),
            "pct_good": round(100 * good / n, 1),
            "pct_bad": round(100 * bad / n, 1),
            "mean_score": round((good + 0.5 * weak) / n, 3),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Sort: by top_level alphabetical, then level walk order.
    level_pri = {lvl: i for i, lvl in enumerate(level_order)}
    df["_level_pri"] = df["level"].map(level_pri).fillna(99)
    df = df.sort_values([top_level, "_level_pri"]).drop(columns=["_level_pri"])
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Section 2 — Per Sub-Pillar / second-level error breakdown
# ---------------------------------------------------------------------------

def second_level_breakdown(
    scored_df: pd.DataFrame,
    entity_ctx: dict[str, dict[str, list[str]]],
    level_order: list[str],
    id_col: str,
    min_n: int = 3,
) -> pd.DataFrame:
    """For each second-level node, aggregate scored matches at that
    level and below.

    "Second level" is the second entry in ``level_order`` (Sub-Pillar
    / SectorCluster). Matches at the top level are excluded — we want
    the per-Sub-Pillar quality of its OWN matches plus its children.
    Filters out very-small buckets (``< min_n`` matches).
    """
    if len(level_order) < 2:
        return pd.DataFrame()
    top_level = level_order[0]
    second_level = level_order[1]

    bucket: dict[str, Counter] = defaultdict(Counter)
    for _, r in scored_df.iterrows():
        if r["level"] == top_level:
            continue  # skip top-level matches
        eid = r[id_col]
        if r["level"] == second_level:
            attrs = [str(r["match"]).strip()]
        else:
            attrs = entity_ctx.get(eid, {}).get(second_level, [])
        score = r["score"]
        if pd.isna(score):
            continue
        for a in attrs:
            bucket[a][score] += 1

    rows: list[dict[str, Any]] = []
    for node, counts in bucket.items():
        n = sum(counts.values())
        if n < min_n:
            continue
        good = counts["good"]; weak = counts["weak"]; bad = counts["bad"]
        rows.append({
            second_level: node,
            "n_matches": n,
            "good": int(good),
            "weak": int(weak),
            "bad": int(bad),
            "pct_good": round(100 * good / n, 1),
            "pct_bad": round(100 * bad / n, 1),
            "mean_score": round((good + 0.5 * weak) / n, 3),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("mean_score").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Section 3 / 4 — Top failure nodes (bad / weak counts per match name)
# ---------------------------------------------------------------------------

def top_failure_nodes(
    scored_df: pd.DataFrame,
    level_order: list[str],
    score_filter: str,
    n_per_level: int = 10,
) -> dict[str, pd.DataFrame]:
    """Return ``{level → DataFrame(match, count, sample_reasons)}``.

    Lists the most-often-``score_filter``-scored match names per
    level, with up to 3 sample reasons per node so the user can see
    *why* the judge complained.
    """
    out: dict[str, pd.DataFrame] = {}
    for level in level_order:
        sub = scored_df[(scored_df["level"] == level) & (scored_df["score"] == score_filter)]
        if sub.empty:
            continue
        counts = sub.groupby("match").size().sort_values(ascending=False).head(n_per_level)
        rows: list[dict[str, Any]] = []
        for match, n in counts.items():
            reasons = sub[sub["match"] == match]["reason"].dropna().head(3).tolist()
            rows.append({
                "match": match,
                f"n_{score_filter}": int(n),
                "sample_reasons": " | ".join(str(r).replace("\n", " ")[:200] for r in reasons),
            })
        out[level] = pd.DataFrame(rows)
    return out


# ---------------------------------------------------------------------------
# Section 5 — Failure-reason clustering
# ---------------------------------------------------------------------------

# Simple regex patterns matching the dominant "complaint shapes" we
# observe in judge reasons across all four projects. Order matters —
# first match wins per reason.
REASON_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("no_mention",          re.compile(r"\bno (?:mention|reference)\b", re.I)),
    ("does_not_mention",    re.compile(r"\b(?:does|did) not mention\b", re.I)),
    ("not_specified",       re.compile(r"\bnot (?:specified|specific)\b", re.I)),
    ("does_not_specify",    re.compile(r"\b(?:does|did) not specify\b", re.I)),
    ("no_evidence",         re.compile(r"\bno (?:evidence|indication)\b", re.I)),
    ("not_described",       re.compile(r"\bnot (?:described|explicit)\b", re.I)),
    ("not_explicitly",      re.compile(r"\bnot explicitly\b", re.I)),
    ("only_focuses_on",     re.compile(r"\b(?:only|primarily) (?:focuses|focused) on\b", re.I)),
    ("qualifier_mismatch",  re.compile(r"\b(?:qualifier|specific) (?:does not match|not (?:supported|named))\b", re.I)),
    ("generic_language",    re.compile(r"\b(?:generic|broad|vague) (?:phrase|language|reference)\b", re.I)),
    ("different_focus",     re.compile(r"\b(?:different|other) (?:focus|activity|domain)\b", re.I)),
    ("too_loose",           re.compile(r"\b(?:too )?(?:loose|tenuous|stretched|inferential)\b", re.I)),
    ("uses_not_provides",   re.compile(r"\b(?:uses|consumes).*\b(?:not|rather than).*\b(?:provides|supplies|sells)\b", re.I)),
    ("title_only",          re.compile(r"\b(?:title|name) (?:alone|only)\b", re.I)),
]


def cluster_failure_reasons(
    scored_df: pd.DataFrame,
    score_filter: str = "bad",
) -> pd.DataFrame:
    """Bucket bad/weak match reasons by canonical complaint shape."""
    sub = scored_df[scored_df["score"] == score_filter]
    if sub.empty:
        return pd.DataFrame(columns=["complaint", "n", "pct"])
    counts: Counter = Counter()
    for reason in sub["reason"].fillna(""):
        matched = False
        for label, pat in REASON_PATTERNS:
            if pat.search(reason):
                counts[label] += 1
                matched = True
                break
        if not matched:
            counts["other"] += 1
    rows = [
        {"complaint": label, "n": n, "pct": round(100 * n / len(sub), 1)}
        for label, n in counts.most_common()
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Section 6 — Pillar / Sector alignment verdicts
# ---------------------------------------------------------------------------

def verdict_breakdown(verdicts_df: pd.DataFrame) -> pd.DataFrame:
    if verdicts_df.empty:
        return pd.DataFrame(columns=["verdict", "n", "pct"])
    counts = verdicts_df["verdict"].value_counts(dropna=False).reset_index()
    counts.columns = ["verdict", "n"]
    counts["pct"] = (100 * counts["n"] / counts["n"].sum()).round(1)
    return counts


def wrong_or_mixed_samples(
    verdicts_df: pd.DataFrame,
    name_col: str,
    chosen_col: str,
    no_match_value: str,
    n: int = 10,
) -> pd.DataFrame:
    sub = verdicts_df[verdicts_df["verdict"].isin(
        {"wrong", "mixed", no_match_value}
    )]
    cols = [name_col, chosen_col, "verdict", "reason"]
    cols = [c for c in cols if c in sub.columns]
    return sub[cols].head(n).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Section 7 — NoMatch entities in this sample
# ---------------------------------------------------------------------------

def nomatch_entities(
    per_row_df: pd.DataFrame,
    id_col: str,
    name_col: str,
    description_col: str,
    top_level: str,
    n: int = 20,
) -> tuple[int, pd.DataFrame]:
    """Locate entities whose top-level classification is empty.

    Tries the per-row file's ``deepest_match`` column first (an
    "NoMatch" value indicates the walk halted before any pillar).
    Falls back to filtering rows where the top-level column is null.
    Returns ``(total_count, sample DataFrame)``.
    """
    if "deepest_match" in per_row_df.columns:
        nm_mask = per_row_df["deepest_match"] == "NoMatch"
    else:
        # Find the top-level column name (might be hyphenated).
        top_col = None
        for cand in (top_level,
                     top_level.replace("Sub", "Sub-"),
                     top_level.replace("Sub-", "Sub")):
            if cand in per_row_df.columns:
                top_col = cand
                break
        if top_col is None:
            return 0, pd.DataFrame()
        # Treat "[]" / NaN / empty as no-match.
        def _empty(v):
            if v is None:
                return True
            if isinstance(v, float) and pd.isna(v):
                return True
            s = str(v).strip()
            return s == "" or s == "[]"
        nm_mask = per_row_df[top_col].apply(_empty)

    nm = per_row_df[nm_mask].drop_duplicates(subset=[id_col])
    total = len(nm)
    if total == 0:
        return 0, pd.DataFrame()
    sample = nm.sample(n=min(n, total), random_state=42)
    cols = [c for c in (id_col, name_col, description_col) if c in sample.columns]
    out = sample[cols].copy()
    if description_col in out.columns:
        out[description_col] = out[description_col].astype(str).str[:240]
    return total, out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Markdown formatting helpers
# ---------------------------------------------------------------------------

def _md_table(df: pd.DataFrame, empty_msg: str = "_(none)_", cell_limit: int = 200) -> str:
    if df.empty:
        return empty_msg
    cols = list(df.columns)
    header = "| " + " | ".join(cols) + " |"
    sep = "|" + "|".join(["---"] * len(cols)) + "|"
    body = "\n".join(
        "| " + " | ".join(
            str(r[c]).replace("|", "\\|").replace("\n", " ")[:cell_limit]
            for c in cols
        ) + " |"
        for _, r in df.iterrows()
    )
    return "\n".join([header, sep, body])


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def build_report(
    scored_path: Path,
    scored_df: pd.DataFrame,
    verdicts_df: pd.DataFrame | None,
    per_row_df: pd.DataFrame | None,
    level_order: list[str],
    id_col: str,
    name_col: str,
    description_col: str | None,
    top_level: str,
    verdict_top_label: str,
) -> str:
    parts: list[str] = []
    parts.append(f"# Judge-results analysis: `{scored_path.name}`\n")
    parts.append(
        f"Generated by `analyze_judge_results.py`. Re-run to regenerate "
        f"after re-judging.\n"
    )

    # Provenance / coverage
    n_entities_scored = scored_df[id_col].nunique()
    n_matches = len(scored_df)
    bad = (scored_df["score"] == "bad").sum()
    weak = (scored_df["score"] == "weak").sum()
    good = (scored_df["score"] == "good").sum()
    mean = round((good + 0.5 * weak) / n_matches, 3) if n_matches else 0.0
    parts.append("## Provenance\n")
    parts.append(f"- Scored file: `{scored_path.name}`")
    if verdicts_df is not None:
        parts.append(f"- Verdicts file: present ({len(verdicts_df)} entity verdicts)")
    else:
        parts.append(f"- Verdicts file: NOT FOUND (section 6 omitted)")
    if per_row_df is not None:
        parts.append(f"- Per-row file: present ({len(per_row_df)} rows, {per_row_df[id_col].nunique()} entities)")
    else:
        parts.append(f"- Per-row file: NOT FOUND (sections 1, 2, 7 omitted)")
    parts.append(f"- Level order: {' → '.join(level_order)}")
    parts.append(
        f"- Headline: **{n_matches} matches scored** across **{n_entities_scored} entities** "
        f"→ {100*good/n_matches:.1f}% good, {100*weak/n_matches:.1f}% weak, "
        f"{100*bad/n_matches:.1f}% bad, mean = **{mean}**\n"
    )

    # Build entity context if per_row available
    entity_ctx: dict[str, dict[str, list[str]]] = {}
    if per_row_df is not None:
        entity_ctx = _build_entity_context(per_row_df, level_order, id_col)

    # ----- Section 1: Pillar × Level error breakdown -----
    parts.append(f"## 1. {top_level} × Level error breakdown\n")
    if per_row_df is None:
        parts.append("_(skipped — per-row file required to attribute matches to a top-level node)_\n")
    else:
        pl = pillar_level_breakdown(scored_df, entity_ctx, level_order, id_col, top_level)
        parts.append(_md_table(pl))
        parts.append("")
        if not pl.empty:
            worst = pl.sort_values("mean_score").head(3)
            parts.append("**Worst-performing (lowest mean):**")
            for _, r in worst.iterrows():
                parts.append(
                    f"- `{r[top_level]}` × `{r['level']}` "
                    f"— mean {r['mean_score']}, "
                    f"{r['pct_good']}% good, {r['pct_bad']}% bad, n={r['n_matches']}"
                )
            parts.append("")

    # ----- Section 2: Second-level (Sub-Pillar) breakdown -----
    second_level = level_order[1] if len(level_order) > 1 else None
    parts.append(f"## 2. Per-{second_level} error breakdown\n" if second_level else "## 2. (n/a — no second level)\n")
    if per_row_df is None or not second_level:
        parts.append("_(skipped)_\n")
    else:
        sp = second_level_breakdown(scored_df, entity_ctx, level_order, id_col)
        parts.append(_md_table(sp))
        parts.append("")
        if not sp.empty:
            parts.append("**Cleanest (highest mean) / worst (lowest mean) buckets**:")
            top3 = sp.tail(3).iloc[::-1]
            bot3 = sp.head(3)
            for label, rows in [("worst", bot3), ("best", top3)]:
                for _, r in rows.iterrows():
                    parts.append(
                        f"- {label}: `{r[second_level]}` "
                        f"— mean {r['mean_score']}, n={r['n_matches']}"
                    )
            parts.append("")

    # ----- Section 3: Top bad-firing nodes -----
    parts.append("## 3. Top bad-firing nodes (by level)\n")
    bad_nodes = top_failure_nodes(scored_df, level_order, score_filter="bad")
    if not bad_nodes:
        parts.append("_(no bad-scored matches in this sample)_\n")
    for level, df in bad_nodes.items():
        parts.append(f"### {level}\n")
        parts.append(_md_table(df))
        parts.append("")

    # ----- Section 4: Top weak-firing nodes -----
    parts.append("## 4. Top weak-firing nodes (by level)\n")
    weak_nodes = top_failure_nodes(scored_df, level_order, score_filter="weak")
    if not weak_nodes:
        parts.append("_(no weak-scored matches in this sample)_\n")
    for level, df in weak_nodes.items():
        parts.append(f"### {level}\n")
        parts.append(_md_table(df))
        parts.append("")

    # ----- Section 5: Failure-reason clustering -----
    parts.append("## 5. Failure-reason clustering (bad matches)\n")
    fc = cluster_failure_reasons(scored_df, score_filter="bad")
    parts.append(_md_table(fc, empty_msg="_(no bad-scored matches)_"))
    parts.append("")
    if not fc.empty:
        parts.append(
            "_Patterns flag dominant complaint shapes the judge wrote. "
            "Multiple `no_mention` / `does_not_mention` clusters typically "
            "indicate node names whose distinguishing tokens never appear "
            "in descriptions; `qualifier_mismatch` indicates "
            "qualifier-lock violations; `uses_not_provides` indicates "
            "the user-vs-provider conflation pattern._\n"
        )

    # ----- Section 5b: Failure-reason clustering for weak -----
    parts.append("## 5b. Failure-reason clustering (weak matches)\n")
    fcw = cluster_failure_reasons(scored_df, score_filter="weak")
    parts.append(_md_table(fcw, empty_msg="_(no weak-scored matches)_"))
    parts.append("")

    # ----- Section 6: Verdict breakdown -----
    parts.append(f"## 6. {verdict_top_label} alignment verdicts\n")
    if verdicts_df is None or verdicts_df.empty:
        parts.append("_(skipped — verdicts file not found)_\n")
    else:
        vb = verdict_breakdown(verdicts_df)
        vb_md = vb.copy()
        if not vb_md.empty:
            vb_md["pct"] = vb_md["pct"].astype(str) + "%"
        parts.append(_md_table(vb_md, empty_msg="_(no verdicts)_"))
        parts.append("")

        # Wrong / mixed / no-match samples
        chosen_col = f"chosen_{verdict_top_label.lower()}s"
        no_match_value = f"no_{verdict_top_label.lower()}_expected"
        wrong = wrong_or_mixed_samples(
            verdicts_df, name_col, chosen_col, no_match_value
        )
        parts.append(f"### Samples: wrong / mixed / {no_match_value}\n")
        parts.append(_md_table(wrong))
        parts.append("")

    # ----- Section 7: NoMatch entities in this sample -----
    parts.append("## 7. NoMatch entities in this sample\n")
    if per_row_df is None or description_col is None:
        parts.append("_(skipped — per-row file or description column required)_\n")
    else:
        total_nm, nm_sample = nomatch_entities(
            per_row_df, id_col, name_col, description_col, top_level
        )
        parts.append(
            f"- **{total_nm} NoMatch entities** in this sample "
            f"({100 * total_nm / per_row_df[id_col].nunique():.1f}% of the sample pool)\n"
        )
        if total_nm > 0:
            parts.append("Random sample (description preview, 240 chars):\n")
            parts.append(_md_table(nm_sample))
            parts.append("")
            parts.append(
                "_If NoMatch entities are >5% of the sample, run "
                "`analyze_full_coverage.py` against the full-pool "
                "classification to see whether the rate generalizes "
                "(and whether there are systematic 'should-have-matched' "
                "missed entities)._\n"
            )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Optional HTML rendering — mirrors the local render_report.py template
# ---------------------------------------------------------------------------

_HTML_CSS = """
:root { --fg: #1a1a1a; --muted: #5a5a5a; --bg: #ffffff; --line: #e1e4e8;
        --soft: #f6f8fa; --accent: #0a4d8c; --code-bg: #f6f8fa; }
* { box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
       font-size: 15px; line-height: 1.55; color: var(--fg); background: var(--bg);
       max-width: 1100px; margin: 2.5rem auto; padding: 0 1.5rem 4rem; }
h1, h2, h3 { font-weight: 600; margin-top: 2rem; margin-bottom: 0.6rem; line-height: 1.25; }
h1 { font-size: 1.85rem; border-bottom: 1px solid var(--line); padding-bottom: 0.4rem; margin-top: 0; }
h2 { font-size: 1.35rem; border-bottom: 1px solid var(--line); padding-bottom: 0.3rem; margin-top: 2.4rem; }
h3 { font-size: 1.1rem; color: var(--accent); margin-top: 1.6rem; }
p { margin: 0.6rem 0; }
ul, ol { padding-left: 1.4rem; } li { margin: 0.18rem 0; }
code { font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
       font-size: 0.88em; background: var(--code-bg); padding: 0.13em 0.36em; border-radius: 4px; }
table { border-collapse: collapse; margin: 0.9rem 0 1.4rem; font-size: 0.9em; }
th, td { border: 1px solid var(--line); padding: 0.4rem 0.7rem; text-align: left; vertical-align: top;
         font-variant-numeric: tabular-nums; }
th { background: var(--soft); font-weight: 600; }
tr:nth-child(2n) td { background: #fafbfc; }
strong { font-weight: 600; }
em { color: var(--muted); }
"""

_HTML_TEMPLATE = """<!doctype html><html lang=en><head><meta charset=utf-8>
<title>{title}</title><meta name=viewport content="width=device-width, initial-scale=1">
<style>{css}</style></head><body>{body}</body></html>
"""


def render_html(md_text: str, html_path: Path, title: str) -> None:
    try:
        import markdown
    except ImportError:
        print("  [warn] 'markdown' package not installed — skipping HTML render")
        return
    body = markdown.markdown(
        md_text, extensions=["tables", "fenced_code", "toc", "sane_lists"],
    )
    html = _HTML_TEMPLATE.format(title=title, css=_HTML_CSS, body=body)
    html_path.write_text(html, encoding="utf-8")
    print(f"Wrote HTML to {html_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    scored_path: Path,
    verdicts_path: Path | None = None,
    per_row_path: Path | None = None,
    output_path: Path | None = None,
    write_html: bool = False,
    levels_override: list[str] | None = None,
    method_filter: str = "new",
) -> None:
    if not scored_path.exists():
        raise FileNotFoundError(f"Scored file not found: {scored_path}")

    auto_verdicts, auto_per_row, top_label = _find_companion_paths(scored_path)
    if verdicts_path is None:
        verdicts_path = auto_verdicts
    if per_row_path is None:
        per_row_path = auto_per_row

    print(f"Loading scored: {scored_path.name}")
    scored_df = _read_table(scored_path)
    if "level" not in scored_df.columns or "score" not in scored_df.columns:
        raise ValueError(
            f"Scored file must have 'level' and 'score' columns; "
            f"got {list(scored_df.columns)}"
        )
    # Backward-compat: the legacy dual-method judge wrote a 'method'
    # column with 'new' / 'old' rows in the same file. Default to
    # filtering to 'new' so we don't conflate two methods' scores;
    # users wanting to analyze the old method can pass --method old.
    if "method" in scored_df.columns:
        n_before = len(scored_df)
        scored_df = scored_df[scored_df["method"] == method_filter].reset_index(drop=True)
        print(
            f"  Filtered scored to method='{method_filter}': "
            f"{n_before} → {len(scored_df)} rows"
        )

    id_col = _detect_col(scored_df, ID_COL_CANDIDATES)
    name_col = _detect_col(scored_df, NAME_COL_CANDIDATES)
    print(f"  Detected id_col={id_col}, name_col={name_col}")

    if levels_override:
        level_order = levels_override
    else:
        level_order = _level_order_from_scored(scored_df)
    print(f"  Level order: {level_order}")
    top_level = level_order[0]

    verdicts_df = None
    if verdicts_path and verdicts_path.exists():
        print(f"Loading verdicts: {verdicts_path.name}")
        verdicts_df = _read_table(verdicts_path)
        # The verdicts file's chosen_<X>s column hints at the verdict label.
        chosen_cols = [c for c in verdicts_df.columns if c.startswith("chosen_")]
        if chosen_cols:
            # e.g., chosen_pillars → Pillar
            inferred = chosen_cols[0].replace("chosen_", "").rstrip("s")
            top_label = inferred
    verdict_top_label = top_label.capitalize() if top_label else "Pillar"
    print(f"  Verdict top label: {verdict_top_label}")

    per_row_df = None
    description_col: str | None = None
    if per_row_path and per_row_path.exists():
        print(f"Loading per-row: {per_row_path.name}")
        per_row_df = _read_table(per_row_path)
        try:
            description_col = _detect_col(per_row_df, DESCRIPTION_COL_CANDIDATES)
            print(f"  Detected description_col={description_col}")
        except KeyError:
            description_col = None

    report_md = build_report(
        scored_path=scored_path,
        scored_df=scored_df,
        verdicts_df=verdicts_df,
        per_row_df=per_row_df,
        level_order=level_order,
        id_col=id_col,
        name_col=name_col,
        description_col=description_col,
        top_level=top_level,
        verdict_top_label=verdict_top_label,
    )

    if output_path is None:
        output_path = scored_path.with_name(
            scored_path.stem.replace("_quality_scored", "_quality_analysis") + ".md"
        )
        if output_path == scored_path:  # no _quality_scored token in stem
            output_path = scored_path.with_suffix("").with_name(
                scored_path.stem + "_analysis.md"
            )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_md, encoding="utf-8")
    print(f"\nWrote report to {output_path} ({len(report_md)} chars)")

    if write_html:
        html_path = output_path.with_suffix(".html")
        render_html(report_md, html_path, title=f"Analysis: {scored_path.name}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description=(__doc__ or "").splitlines()[1] if __doc__ else None
    )
    p.add_argument(
        "--scored", type=Path, required=True,
        help="Path to the judge's scored xlsx/csv file.",
    )
    p.add_argument(
        "--verdicts", type=Path, default=None,
        help="Path to the verdicts xlsx/csv. Auto-detected from --scored if omitted.",
    )
    p.add_argument(
        "--per-row", type=Path, default=None,
        help="Path to the per-row classification xlsx/csv. Auto-detected from --scored if omitted.",
    )
    p.add_argument(
        "--output", type=Path, default=None,
        help="Markdown report output path. Default: alongside --scored with _quality_analysis.md suffix.",
    )
    p.add_argument(
        "--html", action="store_true",
        help="Also write an HTML render of the markdown report.",
    )
    p.add_argument(
        "--levels", type=str, default=None,
        help=(
            "Comma-separated level names in walk order, e.g. "
            "'Pillar,SubPillar,Solution,SubTerm'. Overrides auto-"
            "detection if the data has unusual level labels."
        ),
    )
    p.add_argument(
        "--method", choices=["new", "old"], default="new",
        help=(
            "When the scored file has a 'method' column (legacy dual-"
            "judge output), filter to this method before analyzing. "
            "Default 'new'. Single-method scored files (the standard "
            "output of the current judge engine) don't have this column "
            "and the flag is ignored."
        ),
    )
    args = p.parse_args()

    levels_override = (
        [s.strip() for s in args.levels.split(",")] if args.levels else None
    )
    main(
        scored_path=args.scored,
        verdicts_path=args.verdicts,
        per_row_path=args.per_row,
        output_path=args.output,
        write_html=args.html,
        levels_override=levels_override,
        method_filter=args.method,
    )
