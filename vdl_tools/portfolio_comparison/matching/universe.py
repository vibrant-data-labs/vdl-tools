"""Tier 1: match customer rows into the pinned baseline run.

Matches against the *full* enriched file (the superset), so orgs the
landscape filtering removed are still findable; ``in_universe`` is then set
by membership in the nodes-filter id set. A confident match to a filtered-out
org skips Tier 2 and is surfaced in review as ``excluded_by_landscape_filter``
— it either confirms the org is out of scope or exposes a filter error.

Local data only — this tier costs nothing to run.
"""

from difflib import SequenceMatcher

import pandas as pd

from vdl_tools.portfolio_comparison.intake.normalize import (
    name_tokens,
    normalize_domain,
    normalize_name,
)
from vdl_tools.portfolio_comparison.matching import thresholds
from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


def _similarity(name_a: str, name_b: str) -> float:
    """Blend of character-level ratio and token overlap on normalized names."""
    if not name_a or not name_b:
        return 0.0
    char = SequenceMatcher(None, name_a, name_b).ratio()
    tokens_a, tokens_b = set(name_a.split()), set(name_b.split())
    jaccard = len(tokens_a & tokens_b) / len(tokens_a | tokens_b)
    return max(char, jaccard)


class UniverseIndex:
    """Search index over the baseline enriched file."""

    def __init__(
        self,
        enriched: pd.DataFrame,
        universe_ids: set[str],
        id_col: str = "uuid",
        name_col: str = "Organization",
        url_col: str = "url_homepage",
    ):
        self.universe_ids = universe_ids
        records = []
        for _, row in enriched.iterrows():
            records.append({
                "id": str(row[id_col]),
                "name": row.get(name_col) or "",
                "url": row.get(url_col) or "",
                "norm_name": normalize_name(row.get(name_col)),
                "domain": normalize_domain(row.get(url_col)),
            })
        self.records = records

        self.by_domain: dict[str, list[dict]] = {}
        self.by_name: dict[str, list[dict]] = {}
        self.by_token: dict[str, set[int]] = {}
        for i, rec in enumerate(records):
            if rec["domain"]:
                self.by_domain.setdefault(rec["domain"], []).append(rec)
            if rec["norm_name"]:
                self.by_name.setdefault(rec["norm_name"], []).append(rec)
            for token in rec["norm_name"].split():
                self.by_token.setdefault(token, set()).add(i)

    def in_universe(self, matched_id: str) -> bool:
        return matched_id in self.universe_ids

    def _to_candidate(self, rec: dict, score: float, method: str) -> Candidate:
        return Candidate(
            matched_id=rec["id"],
            matched_name=rec["name"],
            matched_url=rec["url"],
            score=score,
            method=method,
            evidence={"domain": rec["domain"], "in_universe": self.in_universe(rec["id"])},
        )

    def match(self, name: str, url: str) -> list[Candidate]:
        domain = normalize_domain(url)
        if domain and domain in self.by_domain:
            recs = self.by_domain[domain]
            return [
                self._to_candidate(r, thresholds.DOMAIN_EXACT_CONFIDENCE, "url_exact")
                for r in recs
            ]

        norm = normalize_name(name)
        if norm and norm in self.by_name:
            return [self._to_candidate(r, 1.0, "name_exact") for r in self.by_name[norm]]

        pool_idx: set[int] = set()
        for token in name_tokens(name):
            pool_idx |= self.by_token.get(token, set())
        scored = []
        for i in pool_idx:
            rec = self.records[i]
            score = _similarity(norm, rec["norm_name"])
            if score >= thresholds.NAME_SIM_CANDIDATE_FLOOR:
                scored.append(self._to_candidate(rec, round(score, 3), "name_fuzzy"))
        scored.sort(key=lambda c: c.score, reverse=True)
        return scored[: thresholds.MAX_CANDIDATES]


def _resolve(
    candidates: list[Candidate], index: UniverseIndex, customer_domain: str = ""
) -> dict:
    """Decide a row's outcome from its Tier-1 candidates.

    Returns partial ID-mapping fields. ``status`` NA means "no Tier-1 signal —
    continue to Tier 2".
    """
    if not candidates:
        return {"status": pd.NA}

    top = candidates[0]
    sole = len(candidates) == 1
    # A name match whose domain contradicts the customer-supplied URL is
    # never confident enough to auto-accept — same name, different site is
    # the classic wrong-entity trap.
    domain_conflict = (
        top.method != "url_exact"
        and bool(customer_domain)
        and bool(top.evidence.get("domain"))
        and customer_domain != top.evidence["domain"]
    )
    auto = not domain_conflict and (
        (top.method == "url_exact" and sole and thresholds.DOMAIN_EXACT_AUTO)
        or (top.method in ("name_exact", "name_fuzzy") and sole
            and top.score >= thresholds.NAME_SIM_AUTO)
    )
    matched = {
        "matched_id": top.matched_id,
        "matched_name": top.matched_name,
        "matched_url": top.matched_url,
        "match_method": top.method,
        "confidence": top.score,
        "in_universe": index.in_universe(top.matched_id),
    }
    if auto and not matched["in_universe"]:
        # Identity is confident but the landscape filtering removed this org:
        # scope, not identity, is the question — a human decides.
        return {
            **matched,
            "status": "needs_review",
            "out_of_universe_reason": "excluded_by_landscape_filter",
            "decided_by": pd.NA,
        }
    if auto:
        return {**matched, "status": "auto_matched", "decided_by": "auto"}
    return {**matched, "status": "needs_review", "decided_by": pd.NA}


def run_tier1(
    rows: pd.DataFrame, index: UniverseIndex
) -> tuple[pd.DataFrame, dict[str, list[Candidate]]]:
    """Run Tier-1 matching for for-profit rows.

    ``rows`` needs: customer_row_id, customer_name, customer_url, entity_type,
    disposition. Returns an ID-mapping-shaped frame plus per-row candidate
    lists for the review queue. Rows with status NA proceed to Tier 2.
    """
    results = []
    candidates_by_row: dict[str, list[Candidate]] = {}
    for _, row in rows.iterrows():
        base = {col: row.get(col, pd.NA) for col in ID_MAPPING_COLUMNS}
        if row.get("entity_type") == "nonprofit":
            results.append(base)  # GT lane handles these
            continue
        cands = index.match(row.get("customer_name"), row.get("customer_url"))
        if cands:
            candidates_by_row[row["customer_row_id"]] = cands
        customer_domain = normalize_domain(row.get("customer_url"))
        results.append({**base, **_resolve(cands, index, customer_domain)})

    return pd.DataFrame(results, columns=ID_MAPPING_COLUMNS), candidates_by_row
