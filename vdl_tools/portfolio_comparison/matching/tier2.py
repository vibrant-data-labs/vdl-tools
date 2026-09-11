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
    from vdl_tools.portfolio_comparison.intake.normalize import identity_domain
    from vdl_tools.portfolio_comparison.matching.source_adapter import (
        pick_converging_candidate,
        pick_funded_duplicate,
    )

    def _accept(idx, cand, in_universe):
        id_mapping.loc[idx, [
            "matched_id", "matched_name", "matched_url",
            "match_method", "confidence", "in_universe",
            "status", "decided_by",
        ]] = [
            cand.matched_id, cand.matched_name, cand.matched_url,
            cand.method, cand.score, in_universe,
            "auto_matched", "auto",
        ]

    targets = id_mapping[
        (id_mapping["entity_type"] == "for_profit")
        & (id_mapping["status"].isna() | (id_mapping["status"] == "needs_review"))
    ]
    n_searched = 0
    for idx, row in targets.iterrows():
        cands = client.search(row["customer_name"], row["customer_url"])
        n_searched += 1
        row_id = row["customer_row_id"]
        merged = _dedupe(candidates_by_row.get(row_id, []), cands)
        if not merged:
            continue
        candidates_by_row[row_id] = merged
        customer_domain = identity_domain(row["customer_url"])

        if pd.isna(row["status"]):
            top = cands[0] if cands else None
            confident = (
                top is not None
                and len(cands) == 1
                and top.evidence.get("signal") == "domain"
            )
            if not confident and cands:
                # Duplicate profiles on the customer's own domain: the one
                # reporting financials is the maintained record.
                funded = pick_funded_duplicate(cands)
                if funded is not None:
                    top, confident = funded, True
            if not confident:
                # Near-exact-name candidates (however many — name searches
                # return every same-named company): if exactly one's domain
                # redirects to the same site as the customer's
                # (aquila.earth → aquila.space), that's the org. Mechanical
                # evidence — no human needed.
                winner = pick_converging_candidate(customer_domain, merged)
                if winner is not None:
                    winner.evidence["redirect_confirmed"] = True
                    top, confident = winner, True
            if confident:
                # Identity confirmed; API matches are outside the universe
                # unless the candidate says otherwise (Tier-1 origin).
                _accept(idx, top, bool(top.evidence.get("in_universe")))
            elif cands:
                # Name-signal or multi-hit: never auto — reviewers get the
                # API evidence.
                id_mapping.loc[idx, "status"] = "needs_review"
        else:
            # Row already queued for review (e.g. by the baseline matcher):
            # redirect convergence across ALL its candidates — old and new —
            # is still mechanical proof, and a redirect-confirmed match must
            # never wait on a human.
            winner = pick_converging_candidate(customer_domain, merged)
            if winner is not None:
                winner.evidence["redirect_confirmed"] = True
                _accept(idx, winner, bool(winner.evidence.get("in_universe")))
    logger.info("Tier 2 (%s): searched %d rows", client.source, n_searched)
    return id_mapping, candidates_by_row, n_searched


def _supplement_ids(
    id_mapping: pd.DataFrame, client, out_col: str, primary_id_pattern: str
) -> pd.DataFrame:
    """Completeness pass: matched for-profits whose primary id came from a
    DIFFERENT source get one search against ``client``; a sole
    domain-confirmed hit fills ``out_col`` (duplicate same-domain profiles:
    the funded one). The primary match is never touched; name-only hits are
    never accepted."""
    from vdl_tools.portfolio_comparison.intake.normalize import identity_domain
    from vdl_tools.portfolio_comparison.matching.source_adapter import (
        pick_funded_duplicate,
    )

    targets = id_mapping[
        (id_mapping["entity_type"] == "for_profit")
        & id_mapping["matched_id"].notna()
        & ~id_mapping["matched_id"].astype(str).str.fullmatch(primary_id_pattern)
        & (id_mapping[out_col].isna() | id_mapping[out_col].astype(str).eq(""))
    ]
    n_filled = n_searched = 0
    for idx, row in targets.iterrows():
        domain = identity_domain(row["matched_url"]) or identity_domain(row["customer_url"])
        if not domain:
            continue
        name = row["matched_name"] if pd.notna(row["matched_name"]) else row["customer_name"]
        try:
            hits = client.search(name, domain)
        except Exception as exc:
            logger.warning("%s supplement search failed for %r: %s", client.source, name, exc)
            continue
        n_searched += 1
        confirmed = [c for c in hits if c.evidence.get("signal") == "domain"]
        pick = confirmed[0] if len(confirmed) == 1 else pick_funded_duplicate(confirmed)
        if pick is not None:
            id_mapping.loc[idx, out_col] = pick.matched_id
            n_filled += 1
    logger.info(
        "%s supplement: searched %d rows, domain-confirmed %d %s",
        client.source, n_searched, n_filled, out_col,
    )
    return id_mapping


def supplement_nzi_ids(id_mapping: pd.DataFrame, nzi_client) -> pd.DataFrame:
    """Fill nzi_id for rows whose primary match isn't NZI (numeric ids)."""
    return _supplement_ids(id_mapping, nzi_client, "nzi_id", r"\d+")


def supplement_cb_ids(id_mapping: pd.DataFrame, cb_client) -> pd.DataFrame:
    """Fill cb_id for rows whose primary match isn't CB (36-char uuids)."""
    return _supplement_ids(id_mapping, cb_client, "cb_id", r"[0-9a-fA-F-]{36}")
