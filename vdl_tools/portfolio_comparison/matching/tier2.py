"""Tier 2: source-API search for rows Tier 1 couldn't identify.

Runs BEFORE any human review or web research (pilot ruling: the source API
has higher recall than the baseline, so it is the first research step, not
the last resort). Two jobs:

1. Unresolved rows (status NA): search the API; a sole domain-signal hit
   auto-accepts as ``in_universe=False``; anything else goes to review with
   the API candidates attached.
2. Rows already in review: attach API candidates as pre-research evidence,
   so reviewers (human or agent) see the source's answer before web search.
"""

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate, SourceClient


def _dedupe(existing: list[Candidate], new: list[Candidate]) -> list[Candidate]:
    seen = {c.matched_id for c in existing}
    return existing + [c for c in new if c.matched_id not in seen]


def run_tier2(
    id_mapping: pd.DataFrame,
    client: SourceClient,
    candidates_by_row: dict[str, list[Candidate]],
) -> tuple[pd.DataFrame, dict[str, list[Candidate]], int]:
    """Mutates ``id_mapping`` for unresolved for-profit rows; enriches the
    review queue's candidate lists. Returns (id_mapping, candidates, n_searched).
    """
    targets = id_mapping[
        (id_mapping["entity_type"] == "for_profit")
        & (id_mapping["status"].isna() | (id_mapping["status"] == "needs_review"))
    ]
    n_searched = 0
    for idx, row in targets.iterrows():
        cands = client.search(row["customer_name"], row["customer_url"])
        n_searched += 1
        if not cands:
            continue
        row_id = row["customer_row_id"]
        candidates_by_row[row_id] = _dedupe(candidates_by_row.get(row_id, []), cands)

        if pd.isna(row["status"]):
            top = cands[0]
            sole_domain = len(cands) == 1 and top.evidence.get("signal") == "domain"
            if not sole_domain and len(cands) == 1 and top.score >= 0.95:
                # Near-exact name, different domain: check whether the two
                # domains redirect to the same site (abalobi.info →
                # abalobi.org). Mechanical evidence — no human needed.
                from vdl_tools.portfolio_comparison.intake.normalize import (
                    domains_converge,
                    normalize_domain,
                )

                customer_domain = normalize_domain(row["customer_url"])
                if domains_converge(customer_domain, top.evidence.get("domain")):
                    top.evidence["redirect_confirmed"] = True
                    sole_domain = True
            if sole_domain:
                # The customer's own domain resolves to exactly one org in
                # the source — identity confirmed, outside the universe.
                id_mapping.loc[idx, [
                    "matched_id", "matched_name", "matched_url",
                    "match_method", "confidence", "in_universe",
                    "status", "decided_by",
                ]] = [
                    top.matched_id, top.matched_name, top.matched_url,
                    "api_search", top.score, False,
                    "auto_matched", "auto",
                ]
            else:
                # Name-signal or multi-hit: never auto — reviewers get the
                # API evidence.
                id_mapping.loc[idx, "status"] = "needs_review"
    logger.info("Tier 2 (%s): searched %d rows", client.source, n_searched)
    return id_mapping, candidates_by_row, n_searched
