"""
Taxonomy overlap / nesting analysis for hierarchical mapping outputs.
=====================================================================

Given the PER-ROW output of the hierarchical taxonomy mapping engine
(`hierarchical_taxonomy_mapping.py`) and the same level spec the walk used,
this module measures, for every pair of taxonomy terms at a level, how much
the sets of entities matched to them overlap:

  jaccard      = both / (n_a + n_b - both)   symmetric overlap, 0..1
  containment  = both / n_small              share of the SMALLER term's
                                             entities that also carry the
                                             larger term ("nestedness")
  lift         = containment / P(large)      association net of prevalence
  ceiling      = max containment the walk permits (see below)

Jaccard finds pairs that overlap as near-equals; containment finds the
asymmetric case Jaccard misses, where a small term sits almost entirely
inside a big one. Containment is read against the larger term's base rate
(its prevalence = what containment would be by chance). The walk offers a
term only to entities that matched its PARENT, so containment between terms
under different parents has a structural ceiling — the share of the smaller
term's entities that matched the other parent at all. Containment is NOT
comparable between same-parent and cross-parent pairs without it; use lift
for cross-parent judgments.

Why: high containment between siblings flags taxonomy REDUNDANCY candidates;
high containment/lift across parents flags real-world CO-PRACTICE (entities
that do one thing typically also do the other). Both matter when evaluating
a taxonomy design.

Input contract
--------------
* ``per_row`` — the engine's per-row frame: one row per (entity, leaf path),
  level columns named by each level's ``output_col``, entity id in ``id_col``
  (usually "uid"). Unmatched entities appear as all-null rows and are
  ignored. The COLLAPSED frame (repr-list cells, level-``name`` columns) is
  NOT supported.
* ``levels`` — the same list-of-dicts level spec the walk used
  (idx / name / sheet / key_col / output_col / parent_filters / ...).

Term identity is the full ancestor path, not the bare name — several
taxonomies reuse names under different parents (e.g. 112 of 1,032 Drawdown
Activity names), and merging those would corrupt every metric. Display
labels stay bare names, qualified with "(parent)" only where names collide.

Typical use (a project driver, runnable from PyCharm) is ONE call:

    import vdl_tools.shared_tools.taxonomy_mapping.analyze_taxonomy_overlap as ato

    ato.run_overlap_analysis(MAPPING_FILE, TAXONOMY_FILE, MY_LEVELS,
                             REPORT_DIR, xlsx_path=PAIRS_XLSX,
                             file_prefix="mytax_")

(the individual steps — compute_overlap, make_provenance,
write_overlap_report — remain public for callers that need to intervene,
e.g. custom taxonomy loading is just taxonomy_tables=my_tables)

Outputs per analyzed level: an interactive containment-vs-Jaccard scatter
and a nesting dumbbell (html; pass png=True for .png copies rendered via
vl_convert), one xlsx of full pair tables (with a
provenance sheet), and one skimmable summary report (md + html). Every
artifact names the exact taxonomy and mapping files it was computed from —
the mapping file itself does not record which taxonomy version produced it,
so the analyst must confirm the mapping run postdates the taxonomy file.

This module deliberately does NOT import hierarchical_taxonomy_mapping —
that module pulls OpenAI/SQL-cache config at import time, and this analysis
must run with only pandas / numpy / altair installed.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd

# Internal separator for path-based term ids. Only used inside this module;
# labels shown to readers never contain it.
SEP = " > "

# A pair counts as "nested" when at least this share of the smaller term's
# entities also carry the larger term.
DEFAULT_NESTED_MIN = 0.60

# Chart scale caps. Big taxonomies produce thousands of nested pairs (the
# Drawdown Activity level has ~4,800 — a full dumbbell would be ~100k px
# tall). Truncation is always announced in the chart title AND on the
# console; never silent.
DEFAULT_MAX_DUMBBELL_ROWS = 60
DEFAULT_MAX_SCATTER_PAIRS = 4000

# Fixed 11-color palette (light theme) for coloring terms by their group
# (an ancestor level). Groups beyond the palette fold into grey.
DEFAULT_PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4",
                   "#008300", "#e34948", "#4a3aa7", "#a15c2f", "#0b8ca8",
                   "#b552c9"]
OVERFLOW_GREY = "#9a998f"   # also the cross-group scatter dot color

# Color pairs a reader cannot reliably tell apart (colorblind ΔE < 8 or
# normal-vision ΔE < 15, measured 2026-08 on a light surface). Valid for
# DEFAULT_PALETTE + OVERFLOW_GREY ONLY — re-measure if the palette changes.
# assign_group_colors() keeps these pairs off the same dumbbell row.
UNSAFE_COLOR_PAIRS = {
    frozenset(("#008300", "#a15c2f")), frozenset(("#eb6834", "#008300")),
    frozenset(("#e34948", "#a15c2f")), frozenset(("#e87ba4", "#0b8ca8")),
    frozenset(("#eb6834", "#e34948")), frozenset(("#1baf7a", "#e87ba4")),
    frozenset(("#1baf7a", "#e34948")), frozenset(("#008300", "#e34948")),
    frozenset(("#eb6834", "#a15c2f")), frozenset(("#eb6834", "#eda100")),
    frozenset(("#2a78d6", "#0b8ca8")), frozenset(("#e87ba4", "#e34948")),
    frozenset(("#eb6834", "#e87ba4")), frozenset(("#1baf7a", "#0b8ca8")),
    frozenset(("#b552c9", "#2a78d6")), frozenset(("#b552c9", "#0b8ca8")),
    frozenset(("#1baf7a", "#9a998f")), frozenset(("#e87ba4", "#9a998f")),
    frozenset(("#0b8ca8", "#9a998f")),
}

INK, MUTED, GRID = "#0b0b0b", "#52514e", "#d9d8d3"
SIZE_RANGE_DUMBBELL = [20, 400]     # dot area (px^2) for entity-count encoding
SIZE_RANGE_SCATTER = [5, 260]


# ---------------------------------------------------------------------------
# Level-spec helpers
# ---------------------------------------------------------------------------

def normalize_levels(levels: list[dict]) -> list[dict]:
    """Fill the optional child_filter_* defaults of a level spec.

    Duplicated from hierarchical_taxonomy_mapping.normalize_levels (importing
    that module pulls OpenAI/SQL config at import time) — keep in sync.
    """
    out = []
    for lvl in levels:
        lvl = dict(lvl)
        lvl.setdefault("child_filter_col", lvl["key_col"])
        lvl.setdefault("child_filter_value_col", lvl["key_col"])
        out.append(lvl)
    return out


def load_definitions(path: Path, levels: list[dict]) -> dict[int, pd.DataFrame]:
    """Read each level's sheet from a taxonomy workbook, keeping the term
    name, its parent columns, and the literal ``Definition`` column.

    Mirrors the engine's load_taxonomy() contract (same dropna rule). Callers
    whose taxonomy needs custom loading (e.g. One Earth synthesizes its
    Sub-Term table from several sheets) should pass their own pre-loaded
    tables to compute_overlap() instead.
    """
    tables = {}
    for lvl in normalize_levels(levels):
        df = pd.read_excel(path, sheet_name=lvl["sheet"])
        df = df.dropna(subset=[lvl["key_col"], "Definition"]).reset_index(drop=True)
        tables[lvl["idx"]] = df
    return tables


# ---------------------------------------------------------------------------
# Term identity and display labels
# ---------------------------------------------------------------------------

def _disambiguated_labels(terms: pd.DataFrame, name_col: str,
                          anc_cols: list[str]) -> pd.Series:
    """Bare term names, qualified with ancestors only where names collide.

    A name shared by several terms (same name under different parents) gets
    "Name (parent)"; if that still collides, more ancestors are added
    ("Name (grandparent > parent)") until every label is unique.
    """
    labels = terms[name_col].astype(str).copy()
    for depth in range(1, len(anc_cols) + 1):
        dup = labels.duplicated(keep=False)
        if not dup.any():
            break
        qual = terms[anc_cols[-depth:]].astype(str).agg(SEP.join, axis=1)
        labels = labels.where(~dup, terms[name_col].astype(str)
                              + " (" + qual + ")")
    return labels


def _term_frame(per_row: pd.DataFrame, levels: list[dict], level_idx: int,
                id_col: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(entity–term memberships, per-term attribute table) for one level.

    Term identity = the full ancestor path (term_id), so same-named terms
    under different parents stay separate. Rows in the per-row frame are
    paths; membership is deduped to one (entity, term) pair. NoMatch rows
    (all-null level columns) fall out in the dropna.
    """
    lv = levels[level_idx]
    anc_cols = [levels[i]["output_col"] for i in range(level_idx)]
    name_col = lv["output_col"]
    need = [id_col] + anc_cols + [name_col]
    d = per_row.dropna(subset=need)[need].copy()
    for c in anc_cols + [name_col]:
        d[c] = d[c].astype(str)
    d["term_id"] = d[anc_cols + [name_col]].agg(SEP.join, axis=1)

    members = d[[id_col, "term_id"]].drop_duplicates()

    terms = (d.drop_duplicates("term_id")[["term_id"] + anc_cols + [name_col]]
             .reset_index(drop=True))
    terms["name"] = terms[name_col]
    terms["label"] = _disambiguated_labels(terms, name_col, anc_cols)
    # immediate parent: full path prefix (identity) and bare name (display)
    terms["parent_id"] = (terms[anc_cols].agg(SEP.join, axis=1)
                          if anc_cols else "")
    terms["parent_name"] = terms[anc_cols[-1]] if anc_cols else ""
    terms["anc_values"] = terms[anc_cols].values.tolist() if anc_cols else \
        [[] for _ in range(len(terms))]
    return members, terms


def _merge_definitions(terms: pd.DataFrame, lv: dict, parent_lv: dict | None,
                       table: pd.DataFrame | None, level_name: str) -> pd.Series:
    """Definition per term_id, from a taxonomy table in the engine's shape.

    Merge by bare name when names are unique in the table; otherwise by
    (name, immediate parent). Anything still ambiguous stays NaN with a
    printed warning — never guess.
    """
    if table is None:
        return pd.Series(np.nan, index=terms.index)
    key = lv["key_col"]
    defs = table.dropna(subset=[key]).copy()
    defs["_def"] = defs["Definition"].astype(str).str.strip()
    if defs[key].astype(str).is_unique:
        mapping = defs.set_index(defs[key].astype(str))["_def"]
        return terms["name"].map(mapping)
    if parent_lv is None:
        print(f"  WARNING [{level_name}]: duplicate names in the taxonomy "
              f"table and no parent level to disambiguate — definitions "
              f"left blank for the duplicates")
        mapping = defs.drop_duplicates(key, keep=False).set_index(
            defs.drop_duplicates(key, keep=False)[key].astype(str))["_def"]
        return terms["name"].map(mapping)
    # (name, immediate parent) merge. The table's immediate-parent column is
    # named by the parent level's child_filter_col and, for every current
    # taxonomy, holds the same values as the per-row parent column.
    pcol = parent_lv["child_filter_col"]
    two = defs.drop_duplicates([pcol, key])
    still_dup = two.duplicated([pcol, key], keep=False)
    if still_dup.any():
        bad = two.loc[still_dup, key].astype(str).unique().tolist()
        print(f"  WARNING [{level_name}]: ambiguous even by (name, parent); "
              f"definitions left blank for: {bad}")
        two = two[~still_dup]
    mapping = two.set_index([two[pcol].astype(str), two[key].astype(str)])["_def"]
    keys = pd.MultiIndex.from_arrays(
        [terms["parent_name"].astype(str), terms["name"].astype(str)])
    return pd.Series(mapping.reindex(keys).values, index=terms.index)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def overlap_pairs(per_row: pd.DataFrame, levels: list[dict], level_idx: int, *,
                  id_col: str = "uid", nested_min: float = DEFAULT_NESTED_MIN,
                  taxonomy_tables: dict[int, pd.DataFrame] | None = None,
                  color_level: int | None = None) -> pd.DataFrame:
    """All co-occurring term pairs at one level, with overlap metrics.

    Returns one row per pair, oriented so ``small`` is the less common term,
    sorted by containment descending. Columns include display labels
    (small/large), parents, the coloring group (see color_level), all
    metrics, the structural ceiling, and preformatted tooltip sentences.

    color_level: index of the ancestor level whose value colors/groups each
    term in the charts (default: the immediate parent; the level itself at
    level 0, which colors a pair by its smaller term).
    """
    levels = normalize_levels(levels)
    lv = levels[level_idx]
    if color_level is None:
        color_level = max(level_idx - 1, 0)
    members, terms = _term_frame(per_row, levels, level_idx, id_col)

    # binary entity x term matrix and co-occurrence counts
    m = pd.crosstab(members[id_col], members["term_id"]).clip(upper=1)
    co = m.T.values @ m.values
    n = np.diag(co).copy()
    ids = list(m.columns)
    n_entities = len(m)

    # per-term size distribution over ALL terms in use at this level (the
    # pair table alone can't provide this — terms with no co-occurring
    # partner never appear in it). Carried on the frame via .attrs for the
    # summary report.
    term_stats = {"n_terms": int(len(n)),
                  "n_size1": int((n == 1).sum()),
                  "n_size2": int((n == 2).sum()),
                  "n_size3": int((n == 3).sum())}

    # iterate only the nonzero upper-triangle cells (thousands, not millions,
    # at Activity scale)
    iu, ju = np.nonzero(np.triu(co, 1))
    rows = []
    for i, j in zip(iu, ju):
        both = int(co[i, j])
        # orient the pair so `small` is the less common term
        (si, sn), (li, ln) = (((i, int(n[i])), (j, int(n[j])))
                              if n[i] <= n[j] else
                              ((j, int(n[j])), (i, int(n[i]))))
        rows.append(dict(
            small_id=ids[si], large_id=ids[li], n_small=sn, n_large=ln,
            both=both,
            jaccard=both / (sn + ln - both),
            containment=both / sn,      # share of the small term inside the large
            reverse=both / ln,          # share of the large term inside the small
            base_rate=ln / n_entities,  # P(large) = containment expected by chance
        ))
    pairs = pd.DataFrame(rows)
    pairs.attrs["color_level"] = color_level   # write_overlap_report derives
    # terms that overlap with NO other term at this level — they appear in
    # no pair row, so the summary must carry them explicitly. Sorted by size
    # descending: a LARGE isolated term is genuinely distinctive; a tiny one
    # is just too thin to overlap.
    paired_ids = (set(pairs.small_id) | set(pairs.large_id)
                  if not pairs.empty else set())
    size_of = dict(zip(ids, (int(v) for v in n)))
    iso = terms[~terms.term_id.isin(paired_ids)]
    term_stats["isolated"] = sorted(
        [(lbl, size_of[tid]) for tid, lbl in zip(iso.term_id, iso.label)],
        key=lambda t: (-t[1], t[0]))
    term_stats["n_isolated"] = len(term_stats["isolated"])
    pairs.attrs["term_stats"] = term_stats
    if pairs.empty:
        return pairs
    pairs["lift_over_chance"] = pairs.containment / pairs.base_rate
    pairs["nested"] = pairs.containment >= nested_min

    # attach term attributes (labels, parents, group, definition)
    t = terms.set_index("term_id")
    parent_lv = levels[level_idx - 1] if level_idx > 0 else None
    table = (taxonomy_tables or {}).get(level_idx)
    t["definition"] = _merge_definitions(terms, lv, parent_lv, table,
                                         lv["name"]).values

    # coloring group = the color_level-th component of each term's path,
    # displayed with the same collision handling as term labels
    comp = (t["name"] if color_level == level_idx
            else pd.Series([v[color_level] for v in t["anc_values"]],
                           index=t.index))
    gkey = (t.index.to_series() if color_level == level_idx
            else t.index.to_series().str.split(SEP).str[:color_level + 1]
            .str.join(SEP))
    gframe = pd.DataFrame({"gkey": gkey, "gname": comp}).drop_duplicates("gkey")
    ganc = [levels[i]["output_col"] for i in range(color_level)]
    if ganc and gframe.gname.duplicated().any():
        parts = gframe.gkey.str.split(SEP)
        gframe["glabel"] = np.where(
            gframe.gname.duplicated(keep=False),
            gframe.gname + " (" + parts.str[max(color_level - 1, 0)] + ")",
            gframe.gname)
    else:
        gframe["glabel"] = gframe.gname
    glabel_of = gframe.set_index("gkey")["glabel"]
    t["group"] = gkey.map(glabel_of).values

    for side in ("small", "large"):
        sid = pairs[f"{side}_id"]
        pairs[side] = sid.map(t["label"]).values
        pairs[f"parent_{side}"] = sid.map(t["parent_name"]).values
        pairs[f"group_{side}"] = sid.map(t["group"]).values
        pairs[f"def_{side}"] = sid.map(t["definition"]).values

    # structural ceiling: entities of the smaller term that even matched the
    # larger term's parent (the walk only offers a term under a matched
    # parent). Level 0 has no gate.
    if level_idx == 0:
        pairs["ceiling"] = 1.0
        pairs["kind"] = "Top-level pair"
        pairs["ceiling_txt"] = "none — top level has no parent gate"
    else:
        pmembers, _ = _term_frame(per_row, levels, level_idx - 1, id_col)
        parent_ents = pmembers.groupby("term_id")[id_col].apply(set)
        term_ents = members.groupby("term_id")[id_col].apply(set)
        p_of = t.index.to_series().str.split(SEP).str[:-1].str.join(SEP)
        pname = levels[level_idx - 1]["name"]
        psmall = pairs.small_id.map(p_of)
        plarge = pairs.large_id.map(p_of)
        same = (psmall == plarge)
        pairs["ceiling"] = [
            1.0 if s else
            len(term_ents[sm] & parent_ents.get(pl, set())) / ns
            for s, sm, pl, ns in zip(same, pairs.small_id, plarge, pairs.n_small)
        ]
        pairs["kind"] = np.where(same, f"Same {pname}", f"Different {pname}s")
        pairs["ceiling_txt"] = np.where(
            pairs.ceiling >= 1.0,
            f"none — both terms sit under the same {pname}",
            pd.Series(pairs.ceiling).map("{:.0%}".format)
            + " is the most this pair could reach (" + pairs.small
            + " entities that also matched " + plarge.map(
                lambda p: p.split(SEP)[-1] if p else "").values + ")")
    pairs["capped_by_parent"] = pairs.ceiling < nested_min

    # preformatted tooltip sentences (Vega tooltips show fields verbatim)
    pairs["pair_label"] = pairs.small + "  ⊂  " + pairs.large
    pairs["fwd_txt"] = (pairs.containment.map("{:.0%}".format) + " of "
                        + pairs.small + " (n=" + pairs.n_small.astype(str)
                        + ") also carry " + pairs.large)
    pairs["rev_txt"] = (pairs.reverse.map("{:.0%}".format) + " of "
                        + pairs.large + " (n=" + pairs.n_large.astype(str)
                        + ") also carry " + pairs.small)
    pairs["chance_txt"] = (pairs.base_rate.map("{:.0%}".format)
                           + " expected by chance ("
                           + pairs.lift_over_chance.map("{:.1f}".format)
                           + "× over)")
    out = (pairs.sort_values("containment", ascending=False)
           .reset_index(drop=True))
    out.attrs["term_stats"] = term_stats
    out.attrs["color_level"] = color_level
    return out


def compute_overlap(per_row: pd.DataFrame, levels: list[dict], *,
                    id_col: str = "uid",
                    nested_min: float = DEFAULT_NESTED_MIN,
                    taxonomy_tables: dict[int, pd.DataFrame] | None = None,
                    level_indices: list[int] | None = None,
                    color_level: dict[int, int] | None = None,
                    ) -> dict[int, pd.DataFrame]:
    """overlap_pairs() for several levels; returns {level_idx: pair table}.

    Defaults to every level where the mapping used at least two terms.
    color_level maps analyzed level idx -> ancestor idx used for chart
    coloring (see overlap_pairs).
    """
    levels = normalize_levels(levels)
    if level_indices is None:
        level_indices = [lv["idx"] for lv in levels
                         if per_row[lv["output_col"]].nunique() >= 2]
    out = {}
    for idx in level_indices:
        out[idx] = overlap_pairs(
            per_row, levels, idx, id_col=id_col, nested_min=nested_min,
            taxonomy_tables=taxonomy_tables,
            color_level=(color_level or {}).get(idx))
    return out


# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

def assign_group_colors(pairs: pd.DataFrame, *,
                        palette: list[str] = DEFAULT_PALETTE,
                        unsafe_pairs=frozenset(UNSAFE_COLOR_PAIRS),
                        overflow_color: str = OVERFLOW_GREY) -> dict[str, str]:
    """One color per group, so the two groups on any dumbbell row are
    distinguishable.

    Greedy, most-constrained-first: groups ordered by how many OTHER groups
    they share a nested row with (ties: by nested-row count, then name); each
    takes the least-used palette color that is neither identical nor
    hard-to-tell-apart (unsafe_pairs) vs any already-colored row-neighbour.
    Groups that can't be placed — or beyond the palette — go grey.
    Deterministic for a given pair table.
    """
    nested = pairs[pairs.nested]
    counts = pd.concat([nested.group_small, nested.group_large]).value_counts()
    shares_a_row: dict[str, set] = {g: set() for g in counts.index}
    for a, b in zip(nested.group_small, nested.group_large):
        if a != b:
            shares_a_row[a].add(b)
            shares_a_row[b].add(a)
    order = sorted(counts.index,
                   key=lambda g: (-len(shares_a_row[g]), -counts[g], g))
    assigned: dict[str, str] = {}
    for g in order:
        neighbour_colors = {assigned[o] for o in shares_a_row[g]
                            if o in assigned}
        usable = [c for c in palette
                  if c not in neighbour_colors
                  and not any(frozenset((c, nb)) in unsafe_pairs
                              for nb in neighbour_colors)]
        if not usable:
            assigned[g] = overflow_color
            continue
        used = list(assigned.values())
        assigned[g] = min(usable,
                          key=lambda c: (used.count(c), palette.index(c)))
    n_grey = sum(1 for c in assigned.values() if c == overflow_color)
    if n_grey:
        print(f"  note: {n_grey} of {len(assigned)} groups folded to grey "
              f"(palette has {len(palette)} colors)")
    return assigned


def warn_on_lookalike_rows(pairs: pd.DataFrame, group_colors: dict[str, str],
                           level_name: str,
                           unsafe_pairs=frozenset(UNSAFE_COLOR_PAIRS)) -> None:
    """Drift guard for FIXED palettes: warn if a nested row pairs two groups
    whose colors are identical or hard to tell apart."""
    nested = pairs[pairs.nested]
    clashes = sorted({tuple(sorted((a, b)))
                      for a, b in zip(nested.group_small, nested.group_large)
                      if a != b and a in group_colors and b in group_colors
                      and (group_colors[a] == group_colors[b]
                           or frozenset((group_colors[a], group_colors[b]))
                           in unsafe_pairs)})
    if clashes:
        print(f"  WARNING [{level_name}]: look-alike colors on a row — "
              f"re-solve the palette: {clashes}")


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def _pair_tooltip(with_definitions: bool, group_name: str) -> list:
    tips = [alt.Tooltip("pair_label:N", title="pair"),
            alt.Tooltip("fwd_txt:N", title="forward"),
            alt.Tooltip("rev_txt:N", title="reverse"),
            alt.Tooltip("both:Q", title="entities in both"),
            alt.Tooltip("chance_txt:N", title="chance"),
            alt.Tooltip("ceiling_txt:N", title="structural cap"),
            alt.Tooltip("jaccard:Q", title="Jaccard", format=".2f"),
            alt.Tooltip("group_small:N", title=f"{group_name} (smaller)"),
            alt.Tooltip("group_large:N", title=f"{group_name} (larger)")]
    if with_definitions:
        tips += [alt.Tooltip("def_small:N", title="definition (smaller)"),
                 alt.Tooltip("def_large:N", title="definition (larger)")]
    return tips


def _provenance_line(provenance: dict) -> str:
    return (f"taxonomy: {provenance['taxonomy_file']}  ·  "
            f"mapping: {provenance['mapping_file']}")


def _titled(base: str, subset_label: str | None) -> str:
    """Put the cohort label in FRONT of a chart/report title.

    When a driver runs the same analysis over several entity subsets
    (for-profit vs nonprofit, one region at a time), the titles would
    otherwise be identical and the charts only distinguishable by filename.
    """
    return f"{subset_label} · {base}" if subset_label else base


def nesting_scatter(pairs: pd.DataFrame, level_name: str, group_name: str,
                    group_colors: dict[str, str], provenance: dict, *,
                    nested_min: float = DEFAULT_NESTED_MIN,
                    group_is_self: bool = False,
                    max_pairs: int = DEFAULT_MAX_SCATTER_PAIRS,
                    subset_label: str | None = None) -> alt.LayerChart:
    """Containment vs Jaccard, one dot per co-occurring pair; the shaded box
    marks the nested region.

    Dot color: a pair whose two terms share a group takes that group's color;
    a pair spanning two groups is grey (the tooltip names both). With
    group_is_self (level 0), every dot takes its smaller term's color.
    Dot area = entities in the smaller term.

    subset_label names the entity cohort in the title when the analysis
    covers a subset (e.g. "For-profit entities only").
    """
    shown = pairs.copy()
    n_all = len(shown)
    if len(shown) > max_pairs:
        # rank by shared entities, not containment — pure-containment ranking
        # floods the cap with n=1 pairs (100% containment on one entity)
        shown = shown.nlargest(max_pairs, "both")
        print(f"  note [{level_name}]: scatter shows top {max_pairs} of "
              f"{n_all} pairs by shared entities")
    CROSS = f"Cross-{group_name} pair"
    if group_is_self:
        shown["dot_col"] = shown.group_small
        color_title = f"{group_name} (smaller of the pair)"
    else:
        shown["dot_col"] = np.where(shown.group_small == shown.group_large,
                                    shown.group_small, CROSS)
        color_title = f"{group_name} (same-{group_name} pairs)"
    present = sorted(set(shown.dot_col) - {CROSS})
    domain = present + ([CROSS] if (shown.dot_col == CROSS).any() else [])
    col_range = [group_colors.get(g, OVERFLOW_GREY) for g in present] + (
        [OVERFLOW_GREY] if CROSS in domain else [])

    band = alt.Chart(pd.DataFrame([{"x": 0, "x2": 0.40,
                                    "y": nested_min, "y2": 1.0}])).mark_rect(
        color="#2a78d6", opacity=0.07).encode(x="x:Q", x2="x2:Q",
                                              y="y:Q", y2="y2:Q")
    diag = alt.Chart(pd.DataFrame({"v": [0, 1]})).mark_line(
        color="#c9c8c3", strokeDash=[4, 3], strokeWidth=1, clip=True).encode(
        x=alt.X("v:Q", scale=alt.Scale(domain=[0, 0.8])), y="v:Q")

    # clicking a legend entry highlights that category (shift-click adds
    # more; click empty legend space to reset)
    legend_sel = alt.selection_point(fields=["dot_col"], bind="legend")
    has_defs = shown["def_small"].notna().any()
    pts = alt.Chart(shown).mark_circle(stroke="#fcfcfb", strokeWidth=0.5).encode(
        x=alt.X("jaccard:Q", title="Jaccard  (symmetric overlap)",
                scale=alt.Scale(domain=[0, 0.8])),
        y=alt.Y("containment:Q",
                title="Containment  (share of the smaller term inside the larger)",
                scale=alt.Scale(domain=[0, 1])),
        color=alt.Color("dot_col:N", title=color_title,
                        scale=alt.Scale(domain=domain, range=col_range),
                        legend=alt.Legend(orient="right", symbolType="circle",
                                          titleFontSize=10.5, labelFontSize=10,
                                          labelLimit=260)),
        size=alt.Size("n_small:Q", title=["entities in", "the smaller term"],
                      scale=alt.Scale(type="sqrt", range=SIZE_RANGE_SCATTER),
                      legend=alt.Legend(orient="right", titleFontSize=10,
                                        labelFontSize=10,
                                        symbolFillColor="#9db8d8")),
        opacity=alt.condition(legend_sel, alt.value(0.55), alt.value(0.06)),
        tooltip=_pair_tooltip(has_defs, group_name),
    ).add_params(legend_sel)
    notes = alt.Chart(pd.DataFrame([
        {"x": 0.015, "y": 0.975,
         "t": f"NESTED — over {nested_min:.0%} of the small term inside the big one"},
        {"x": 0.53, "y": 0.44, "t": "equal-size overlap"},
    ])).mark_text(align="left", fontSize=10.5, color=MUTED).encode(
        x="x:Q", y="y:Q", text="t:N")

    subtitle_main = (f"one dot = one of {n_all} co-occurring {level_name} "
                     f"pairs;  dot area = entities in the smaller term;  "
                     + ("colored by the smaller term's group  " if group_is_self
                        else f"grey = the two terms sit in different "
                             f"{group_name}s  ")
                     + "(hover for stats and definitions)")
    if len(shown) < n_all:
        subtitle_main = (f"top {len(shown)} of {n_all} pairs by containment;  "
                         + subtitle_main.split(";  ", 1)[1])
    return (band + diag + pts + notes).properties(
        width=520, height=460,
        title=alt.TitleParams(
            _titled(f"{level_name} nesting — containment vs Jaccard",
                    subset_label),
            subtitle=[subtitle_main, _provenance_line(provenance)],
            fontSize=13.5, subtitleFontSize=10.5, anchor="start"),
    )


def nesting_dumbbell(pairs: pd.DataFrame, level_name: str, group_name: str,
                     group_colors: dict[str, str], provenance: dict, *,
                     nested_min: float = DEFAULT_NESTED_MIN,
                     max_rows: int = DEFAULT_MAX_DUMBBELL_ROWS,
                     subset_label: str | None = None) -> alt.LayerChart | None:
    """Every nested pair as a row, sorted by containment (strongest first).

      filled dot = the SMALLER term: share of it also carrying the larger
      hollow dot = the LARGER term: the reverse share
      grey tick  = chance (prevalence of the larger term)

    Dot color = that term's group, area = its entity count. Returns None when
    no pair clears nested_min.

    subset_label names the entity cohort in the title when the analysis
    covers a subset (e.g. "For-profit entities only").
    """
    top = pairs[pairs.nested].copy()
    if top.empty:
        return None
    n_nested = len(top)
    if len(top) > max_rows:
        # rank by shared entities, not containment — pure-containment ranking
        # floods the cap with n=1 pairs (100% containment on one entity);
        # rows are still DISPLAYED sorted by containment
        top = (top.nlargest(max_rows, "both")
               .sort_values("containment", ascending=False))
        print(f"  note [{level_name}]: dumbbell shows top {max_rows} of "
              f"{n_nested} nested pairs by shared entities")
    top["pair"] = top.small + "   ⊂   " + top.large
    order = top["pair"].tolist()   # already sorted by containment desc

    warn_on_lookalike_rows(top.assign(nested=True), group_colors, level_name)
    present = sorted(set(top.group_small) | set(top.group_large))
    color_scale = alt.Scale(domain=present,
                            range=[group_colors.get(g, OVERFLOW_GREY)
                                   for g in present])

    # One row per DOT, single layer: two layers sharing a scale would drop
    # the shared legend as soon as one sets legend=None (Vega-Lite quirk).
    # Group color rides on `fill` (so legend swatches are colored) and is
    # repeated on `stroke`; hollow = fillOpacity 0 keeps the colored outline.
    tip_cols = ["pair_label", "fwd_txt", "rev_txt", "both", "chance_txt",
                "ceiling_txt", "jaccard", "group_small", "group_large",
                "def_small", "def_large"]
    smaller = top[["pair"] + tip_cols].assign(
        x=top.containment, dot_group=top.group_small, dot_n=top.n_small,
        role="smaller term")
    larger = top[["pair"] + tip_cols].assign(
        x=top.reverse, dot_group=top.group_large, dot_n=top.n_large,
        role="larger term")
    dots = pd.concat([larger, smaller], ignore_index=True)  # filled drawn last

    opacity_scale = alt.Scale(domain=["smaller term", "larger term"],
                              range=[1, 0])
    size_scale = alt.Scale(type="sqrt", range=SIZE_RANGE_DUMBBELL,
                           domain=[1, int(max(top.n_small.max(),
                                              top.n_large.max()))])
    y_enc = alt.Y("pair:N", sort=order, title=None,
                  axis=alt.Axis(labelLimit=560, labelFontSize=10.5))
    has_defs = top["def_small"].notna().any()
    tips = _pair_tooltip(has_defs, group_name)

    link = alt.Chart(top.assign(pair=top.small + "   ⊂   " + top.large)
                     ).mark_rule(stroke="#c9c8c3", strokeWidth=2).encode(
        x=alt.X("reverse:Q",
                title="share of one term's entities that also carry the other",
                scale=alt.Scale(domain=[0, 1])),
        x2="containment:Q", y=y_enc, tooltip=tips)
    chance = alt.Chart(top.assign(pair=top.small + "   ⊂   " + top.large)
                       ).mark_tick(thickness=2, size=15,
                                   color=OVERFLOW_GREY).encode(
        x="base_rate:Q", y=y_enc, tooltip=tips)
    legend_sel = alt.selection_point(fields=["dot_group"], bind="legend")
    dot_marks = alt.Chart(dots).mark_point(strokeWidth=2).encode(
        x="x:Q", y=y_enc,
        opacity=alt.condition(legend_sel, alt.value(1.0), alt.value(0.15)),
        fill=alt.Fill("dot_group:N", scale=color_scale, title=group_name,
                      legend=alt.Legend(orient="top", columns=3, labelLimit=250,
                                        titleFontSize=10.5, labelFontSize=10,
                                        symbolType="circle",
                                        symbolStrokeWidth=0)),
        stroke=alt.Stroke("dot_group:N", scale=color_scale, legend=None),
        fillOpacity=alt.FillOpacity("role:N", scale=opacity_scale, legend=None),
        size=alt.Size("dot_n:Q", scale=size_scale, title="entities in the term",
                      legend=alt.Legend(orient="top", direction="horizontal",
                                        titleFontSize=10.5, labelFontSize=10,
                                        symbolFillColor="#9db8d8",
                                        symbolStrokeWidth=0)),
        tooltip=tips).add_params(legend_sel)
    lab = alt.Chart(top.assign(pair=top.small + "   ⊂   " + top.large)
                    ).mark_text(align="left", dx=16, fontSize=10,
                                color=MUTED).encode(
        x="containment:Q", y=y_enc,
        text=alt.Text("containment:Q", format=".0%"))

    shown_note = (f"top {len(top)} of {n_nested} nested pairs "
                  f"by shared entities"
                  if len(top) < n_nested else
                  f"all {n_nested} pairs over {nested_min:.0%} contained")
    return (link + chance + dot_marks + lab).properties(
        # 21px rows keep the chart compact; SIZE_RANGE_DUMBBELL is capped so
        # the largest dot still fits a row without spilling
        width=320, height=max(120, 21 * len(top)),
        title=alt.TitleParams(
            _titled(f"{level_name} nesting — {shown_note}", subset_label),
            subtitle=["filled dot = the SMALLER term;  hollow dot = the "
                      "LARGER term;  grey tick = chance.  Dot color = that "
                      f"term's {group_name}, area = its entity count;  sorted "
                      "by containment — click a legend entry to highlight "
                      "(hover for definitions).",
                      _provenance_line(provenance)],
            fontSize=13.5, subtitleFontSize=10.5, anchor="start"),
    )


# ---------------------------------------------------------------------------
# Provenance, summary report, report writer
# ---------------------------------------------------------------------------

def make_provenance(mapping_file: Path, taxonomy_file: Path,
                    per_row: pd.DataFrame, id_col: str = "uid") -> dict:
    """Facts every output artifact must carry: exactly which files this
    analysis read. (The mapping file does not record which taxonomy version
    produced it — this records what the ANALYSIS used.)"""
    mapping_file, taxonomy_file = Path(mapping_file), Path(taxonomy_file)
    return {
        "taxonomy_file": taxonomy_file.name,
        "mapping_file": mapping_file.name,
        "mapping_mtime": dt.datetime.fromtimestamp(
            mapping_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
        "n_entities": int(per_row[id_col].nunique()),
        "n_rows": int(len(per_row)),
        "analysis_date": dt.date.today().isoformat(),
    }


def _summary_markdown(pairs_by_level: dict[int, pd.DataFrame],
                      levels: list[dict], provenance: dict,
                      nested_min: float, top_n: int = 10,
                      summary_level_indices: list[int] | None = None,
                      subset_label: str | None = None) -> str:
    """One skimmable page: provenance, per-level counts, then ONE SECTION PER
    LEVEL with its top nested pairs split into same-parent (redundancy
    candidates) and cross-parent (co-practice), and how to read the numbers.

    Per-level sections keep a deep level with thousands of near-duplicate
    pairs (Drawdown Activities, OE Sub-Terms) from drowning out the levels
    where redundancy is more actionable. summary_level_indices restricts
    which levels get a detail section at all (the counts table always shows
    every analyzed level, with a note for the ones excluded).
    """
    lines = ["# " + _titled("Taxonomy overlap / nesting summary", subset_label),
             "",
             "## Provenance",
             "",
             f"- **Taxonomy file:** `{provenance['taxonomy_file']}`",
             f"- **Mapping file:** `{provenance['mapping_file']}` "
             f"(modified {provenance['mapping_mtime']})",
             f"- **Entities:** {provenance['n_entities']:,} "
             f"({provenance['n_rows']:,} mapping rows)",]
    # every metric below is computed WITHIN the subset — prevalence, lift and
    # the structural ceiling all use the subset as the denominator, so the
    # numbers are not comparable to a run over the full entity pool
    if subset_label:
        lines.append(f"- **Entity subset:** {subset_label} — every count, "
                     f"base rate, lift and ceiling below is computed within "
                     f"this subset only")
    lines += [
             f"- **Nested threshold:** containment ≥ {nested_min:.0%}",
             f"- **Analysis date:** {provenance['analysis_date']}",
             "",
             "> The mapping file does not record which taxonomy version "
             "produced it. This report states what the analysis read — "
             "confirm the mapping run postdates the taxonomy file.",
             "",
             "## Levels",
             "",
             "| level | terms in use | terms with ≤3 entities "
             "| terms with no overlap | co-occurring pairs (% of possible) "
             "| nested pairs (% of possible) |",
             "|---|---|---|---|---|---|"]
    if summary_level_indices is None:
        summary_level_indices = list(pairs_by_level)
    def _thin(stats):
        if not stats or not stats.get("n_terms"):
            return "—", ""
        le3 = stats["n_size1"] + stats["n_size2"] + stats["n_size3"]
        frac = le3 / stats["n_terms"]
        if le3 == 0:
            return f"0 (0%)", ""
        return (f"{le3} ({frac:.0%})",
                f"{frac:.0%} of the {stats['n_terms']} terms in use are "
                f"matched to 3 or fewer entities "
                f"(n=1: {stats['n_size1']}, n=2: {stats['n_size2']}, "
                f"n=3: {stats['n_size3']}) — containment for those terms "
                f"rests on very thin evidence.")

    for idx, pairs in pairs_by_level.items():
        name = levels[idx]["name"]
        stats = pairs.attrs.get("term_stats", {})
        n_terms = stats.get("n_terms",
                            len(set(pairs.small_id) | set(pairs.large_id))
                            if not pairs.empty else 0)
        thin_cell, _ = _thin(stats)
        n_iso = stats.get("n_isolated", 0)
        iso_cell = (f"{n_iso} ({n_iso / n_terms:.0%})"
                    if n_terms else "—")
        # pair counts as a share of all possible term pairs at this level
        possible = n_terms * (n_terms - 1) // 2
        def _of_possible(k):
            if not possible:
                return str(k)
            frac = k / possible
            return f"{k} ({frac:.0%})" if frac >= 0.095 else f"{k} ({frac:.1%})"
        n_nested = int(pairs.nested.sum()) if not pairs.empty else 0
        excluded = "" if idx in summary_level_indices else " *(counts only)*"
        lines.append(f"| {name}{excluded} | {n_terms} | {thin_cell} | "
                     f"{iso_cell} | {_of_possible(len(pairs))} | "
                     f"{_of_possible(n_nested)} |")
    skipped = [levels[i]["name"] for i in pairs_by_level
               if i not in summary_level_indices]
    if skipped:
        lines += ["", f"*Detail sections below omit "
                      f"{', '.join(skipped)} (deep levels with expected "
                      f"redundancy) — their full pair tables are in the "
                      f"xlsx and their charts alongside this report.*"]

    def _pair_lines(frame, note):
        out = [note + f" Top {min(top_n, len(frame))} by shared entities.", ""]
        if frame.empty:
            return [note, "", "*(none)*"]
        for r in frame.nlargest(top_n, "both").itertuples():
            out.append(f"- **{r.fwd_txt}** — {r.chance_txt}; reverse: "
                       f"{r.rev_txt}")
        return out

    # one section per level, so a deep level with thousands of nested pairs
    # cannot drown out the shallower, more actionable ones
    for idx in summary_level_indices:
        pairs = pairs_by_level.get(idx)
        if pairs is None or pairs.empty:
            continue
        nested = pairs[pairs.nested]
        name = levels[idx]["name"]
        lines += ["", f"## {name} level "
                      f"({len(nested)} nested of {len(pairs)} pairs)"]
        stats = pairs.attrs.get("term_stats", {})
        _, thin_note = _thin(stats)
        if thin_note:
            lines += ["", f"*{thin_note}*"]
        iso = stats.get("isolated", [])
        if iso:
            MAX_LISTED = 15
            listed = ", ".join(f"{lbl} (n={sz})"
                               for lbl, sz in iso[:MAX_LISTED])
            more = (f", + {len(iso) - MAX_LISTED} more"
                    if len(iso) > MAX_LISTED else "")
            lines += ["", f"*{len(iso)} of {stats['n_terms']} terms "
                          f"({len(iso) / stats['n_terms']:.0%}) share no "
                          f"entity with any other {name} — either genuinely "
                          f"distinctive or too thin to overlap: "
                          f"{listed}{more}.*"]
        if idx == 0:
            # no parent at the top level -> no same/cross split
            lines += ["", ""] + _pair_lines(
                nested,
                "Top-level terms whose entity sets nest. How meaningful this "
                "is depends on whether the prompt allows multi-membership "
                "at the top level.")
            continue
        same = nested[nested.kind.str.startswith("Same")]
        cross = nested[nested.kind.str.startswith("Different")]
        parent_name = levels[idx - 1]["name"]
        lines += ["", f"### Redundancy candidates (same {parent_name})", ""]
        lines += _pair_lines(
            same, "Sibling terms that mostly share entities — candidates "
                  "for merging or sharpening definitions.")
        lines += ["", f"### Co-practice (across {parent_name}s)", ""]
        lines += _pair_lines(
            cross, "Terms in different branches whose entities coincide — "
                   "how organizations actually bundle work. Read against "
                   "the structural cap; lift is the reliable cross-parent "
                   "signal.")

    lines += [
        "",
        "## How to read these numbers",
        "",
        "- **Containment** = share of the smaller term's entities that also "
        "carry the larger term. **Jaccard** treats the pair symmetrically "
        "and under-ranks nested pairs of very different sizes.",
        "- **Lift** = containment relative to the larger term's prevalence; "
        "it is the only metric not suppressed by term-size or parent-gate "
        "effects.",
        "- **Structural cap**: the mapping walks the tree, so a term is only "
        "offered to entities that matched its parent — cross-parent "
        "containment is capped below 100% by construction.",
        "- **Top-level caveat**: overlap at level 0 depends on whether the "
        "mapping prompt allows multi-membership there; near-exclusive "
        "prompts make low top-level overlap an artifact of design, not a "
        "finding.",
    ]
    return "\n".join(lines)


_SUMMARY_CSS = """
body { font-family: -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif;
       max-width: 860px; margin: 2em auto; padding: 0 1em; color: #0b0b0b; }
h1, h2 { border-bottom: 1px solid #d9d8d3; padding-bottom: .3em; }
table { border-collapse: collapse; }
th, td { border: 1px solid #d9d8d3; padding: .35em .7em; text-align: left; }
blockquote { color: #52514e; border-left: 3px solid #d9d8d3;
             margin-left: 0; padding-left: 1em; }
code { background: #f4f3f0; padding: .1em .3em; border-radius: 3px; }
"""


def _save_chart(chart: alt.LayerChart, stem: Path, png: bool) -> list[Path]:
    """Write chart as interactive html (+ png). renderer='svg', not the
    default canvas: the click-to-highlight legend silently doesn't register
    clicks under the canvas renderer in Chrome. Tooltips work in both."""
    themed = chart.configure_view(stroke=None).configure_axis(
        gridColor="#eeede9", domainColor=GRID, tickColor=GRID,
        labelColor=MUTED)
    paths = [stem.with_suffix(".html")]
    themed.save(str(paths[0]), embed_options={"renderer": "svg"})
    if png:
        try:
            import vl_convert as vlc   # lazy: undeclared on older installs
            paths.append(stem.with_suffix(".png"))
            paths[-1].write_bytes(vlc.vegalite_to_png(themed.to_json(),
                                                      scale=2))
        except ImportError:
            print(f"  vl_convert not installed — wrote HTML only for "
                  f"{stem.name}; reinstall vdl-tools deps for PNGs")
    return paths


def run_overlap_analysis(mapping_file: Path, taxonomy_file: Path,
                         levels: list[dict], report_dir: Path, *,
                         xlsx_path: Path | None = None,
                         file_prefix: str = "", id_col: str = "uid",
                         nested_min: float = DEFAULT_NESTED_MIN,
                         per_row: pd.DataFrame | None = None,
                         taxonomy_tables: dict[int, pd.DataFrame] | None = None,
                         level_indices: list[int] | None = None,
                         color_level: dict[int, int] | None = None,
                         summary_level_indices: list[int] | None = None,
                         subset_label: str | None = None,
                         group_colors: dict[str, str] | None = None,
                         xlsx_columns: list[str] | None = None,
                         xlsx_rename: dict[str, str] | None = None,
                         max_dumbbell_rows: int = DEFAULT_MAX_DUMBBELL_ROWS,
                         max_scatter_pairs: int = DEFAULT_MAX_SCATTER_PAIRS,
                         png: bool = False) -> dict[int, pd.DataFrame]:
    """The whole analysis in one call — what a project driver runs.

    Reads the per-row mapping xlsx (or takes ``per_row``), loads definitions
    from the taxonomy workbook (or takes pre-loaded ``taxonomy_tables`` for
    taxonomies needing a custom loader), computes overlap for every level
    (or ``level_indices``), writes charts + xlsx + summary with provenance,
    prints a console summary, and returns the pair tables.

    A minimal driver is therefore just paths + one call:

        ato.run_overlap_analysis(MAPPING_FILE, TAXONOMY_FILE, MY_LEVELS,
                                 REPORT_DIR, xlsx_path=PAIRS_XLSX,
                                 file_prefix="mytax_")

    To analyze one entity cohort at a time, call it once per cohort with a
    pre-filtered ``per_row``, a distinct ``file_prefix``, and a
    ``subset_label`` that names the cohort in every title. Everything —
    prevalence, lift, the structural ceiling — is then computed WITHIN that
    cohort, so its numbers stand alone and are not comparable to a full-pool
    run.
    """
    levels = normalize_levels(levels)
    if subset_label:   # so a driver looping over cohorts reads cleanly
        print(f"\n=== {subset_label} ===")
    if per_row is None:
        print(f"reading {Path(mapping_file).name} ...")
        per_row = pd.read_excel(mapping_file)
    if taxonomy_tables is None:
        taxonomy_tables = load_definitions(taxonomy_file, levels)
    print(f"definitions from {Path(taxonomy_file).name}")

    pairs = compute_overlap(per_row, levels, id_col=id_col,
                            nested_min=nested_min,
                            taxonomy_tables=taxonomy_tables,
                            level_indices=level_indices,
                            color_level=color_level)
    provenance = make_provenance(mapping_file, taxonomy_file, per_row, id_col)
    written = write_overlap_report(
        pairs, levels, report_dir, provenance, xlsx_path=xlsx_path,
        file_prefix=file_prefix, nested_min=nested_min,
        group_colors=group_colors, xlsx_columns=xlsx_columns,
        xlsx_rename=xlsx_rename,
        summary_level_indices=summary_level_indices,
        subset_label=subset_label,
        max_dumbbell_rows=max_dumbbell_rows,
        max_scatter_pairs=max_scatter_pairs, png=png)
    for w in written:
        print(f"  wrote {w}")

    for idx, p in pairs.items():
        name = levels[idx]["name"]
        print(f"\n{name}: {len(p)} co-occurring pairs, "
              f"{int(p.nested.sum()) if not p.empty else 0} nested "
              f"(containment >= {nested_min:.0%})")
        if p.empty:
            continue
        show = p[p.nested].nlargest(8, "both")[
            ["small", "large", "n_small", "n_large", "both",
             "containment", "jaccard", "lift_over_chance"]]
        print(show.to_string(index=False, float_format="%.2f"))
    return pairs


def write_overlap_report(pairs_by_level: dict[int, pd.DataFrame],
                         levels: list[dict], report_dir: Path,
                         provenance: dict, *,
                         xlsx_path: Path | None = None,
                         file_prefix: str = "",
                         nested_min: float = DEFAULT_NESTED_MIN,
                         group_names: dict[int, str] | None = None,
                         group_is_self: dict[int, bool] | None = None,
                         group_colors: dict[str, str] | None = None,
                         max_dumbbell_rows: int = DEFAULT_MAX_DUMBBELL_ROWS,
                         max_scatter_pairs: int = DEFAULT_MAX_SCATTER_PAIRS,
                         xlsx_columns: list[str] | None = None,
                         xlsx_rename: dict[str, str] | None = None,
                         summary_level_indices: list[int] | None = None,
                         subset_label: str | None = None,
                         png: bool = False) -> list[Path]:
    """Write, per analyzed level, the scatter + dumbbell charts (html only
    by default; png=True adds .png copies), one xlsx of pair tables
    (provenance sheet first), and one summary report (md + html). Returns
    the written paths.

    group_names: {level_idx: display name of the coloring group level}
    (defaults to the immediate parent level's name; the level's own name
    where group_is_self). group_colors: fixed group->hex override; default
    is the automatic conflict-aware assignment per level.
    xlsx_columns / xlsx_rename let a driver keep a legacy sheet schema.
    summary_level_indices: which levels get detail sections in the summary
    report (default all analyzed) — use it to keep a deep level with
    expected wholesale redundancy (e.g. 1,000+ Activities) from dominating;
    charts and xlsx sheets are unaffected.
    subset_label: name of the entity cohort this run covers (e.g. "For-profit
    entities only"); it leads every chart title and the summary heading, and
    is recorded on the xlsx provenance sheet.
    """
    levels = normalize_levels(levels)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for idx, pairs in pairs_by_level.items():
        if pairs.empty:
            print(f"  [{levels[idx]['name']}] no co-occurring pairs — skipped")
            continue
        level_name = levels[idx]["name"]
        # the coloring-group level rides on the frame from compute_overlap,
        # so group naming needs no separate (and desyncable) configuration
        g = pairs.attrs.get("color_level", max(idx - 1, 0))
        is_self = (group_is_self or {}).get(idx, g == idx)
        gname = (group_names or {}).get(idx, levels[g]["name"])
        colors = group_colors or assign_group_colors(pairs)

        stem = report_dir / f"{file_prefix}{level_name.lower()}_nesting"
        scatter = nesting_scatter(pairs, level_name, gname, colors, provenance,
                                  nested_min=nested_min, group_is_self=is_self,
                                  max_pairs=max_scatter_pairs,
                                  subset_label=subset_label)
        written += _save_chart(scatter, Path(str(stem) + "_scatter"), png)
        dumbbell = nesting_dumbbell(pairs, level_name, gname, colors,
                                    provenance, nested_min=nested_min,
                                    max_rows=max_dumbbell_rows,
                                    subset_label=subset_label)
        if dumbbell is not None:
            written += _save_chart(dumbbell, Path(str(stem) + "_dumbbell"), png)

    if xlsx_path is not None:
        xlsx_path = Path(xlsx_path)
        xlsx_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(xlsx_path) as xl:
            prov_df = pd.DataFrame(list(provenance.items()) +
                                   [("nested_min", nested_min)] +
                                   ([("entity_subset", subset_label)]
                                    if subset_label else []),
                                   columns=["key", "value"])
            prov_df.to_excel(xl, sheet_name="provenance", index=False)
            for idx, pairs in pairs_by_level.items():
                out = pairs.rename(columns=xlsx_rename or {})
                if xlsx_columns:
                    out = out[[c for c in xlsx_columns if c in out.columns]]
                sheet = f"{levels[idx]['name'].lower()}_pairs"[:31]
                out.to_excel(xl, sheet_name=sheet, index=False)
        written.append(xlsx_path)

    md = _summary_markdown(pairs_by_level, levels, provenance, nested_min,
                           summary_level_indices=summary_level_indices,
                           subset_label=subset_label)
    md_path = report_dir / f"{file_prefix}taxonomy_overlap_summary.md"
    md_path.write_text(md)
    written.append(md_path)
    try:
        import markdown as _md   # declared vdl-tools dep
        html = (f"<meta charset='utf-8'><style>{_SUMMARY_CSS}</style>\n"
                + _md.markdown(md, extensions=["tables"]))
        html_path = md_path.with_suffix(".html")
        html_path.write_text(html)
        written.append(html_path)
    except ImportError:
        print("  markdown package not installed — summary written as .md only")
    return written
