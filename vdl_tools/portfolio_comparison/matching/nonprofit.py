"""Nonprofit lane: EIN-direct via Giving Tuesday, identity search pending port.

EIN present → ``GtDatamartClient.get_nonprofits_by_ein``. No EIN → blocked on
the ``search_identity`` port from the datamart frontend's ``searchOrgs`` SQL
(spec §4.5); ProPublica/IRS BMF stays the last-resort fallback after that.
"""

import pandas as pd

from vdl_tools.portfolio_comparison.intake.normalize import normalize_ein
from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate


def match_by_ein(rows: pd.DataFrame, gt_client=None) -> dict[str, Candidate]:
    """Resolve rows that carry an EIN. Returns {customer_row_id: Candidate}.

    An EIN the customer supplied is authoritative — the GT lookup confirms it
    exists in the datamart and retrieves canonical identity fields.
    """
    if gt_client is None:
        from vdl_tools.portfolio_comparison.gt_client import make_gt_client

        gt_client = make_gt_client()

    with_ein = {
        row["customer_row_id"]: normalize_ein(row.get("customer_ein"))
        for _, row in rows.iterrows()
        if normalize_ein(row.get("customer_ein"))
    }
    if not with_ein:
        return {}

    hits = gt_client.get_nonprofits_by_ein(sorted(set(with_ein.values())))
    by_ein = {h.ein: h for h in hits}

    matches = {}
    for row_id, ein in with_ein.items():
        hit = by_ein.get(ein)
        if hit is None:
            continue  # EIN not in datamart → stays unmatched, goes to review
        matches[row_id] = Candidate(
            matched_id=ein,
            matched_name=getattr(hit, "name", "") or "",
            matched_url=getattr(hit, "website", "") or "",
            score=1.0,
            method="customer_provided",
            evidence={"gt_datamart": True},
        )
    return matches


# IdentityHit.signal → candidate score. url_exact is effectively an exact
# match; name is an ILIKE substring hit that still needs human judgment.
_SIGNAL_SCORES = {"ein": 0.99, "url_exact": 0.97, "url_prefix": 0.85, "name": 0.75}


def match_identity(
    rows: pd.DataFrame,
    gt_client=None,
    universe_ids: set[str] | None = None,
    limit: int = 5,
) -> dict[str, list[Candidate]]:
    """EIN-less nonprofit lane: ``GtDatamartClient.search_identity``
    (ported from the datamart frontend's searchOrgs — EIN > URL domain >
    name/DBA tiers, no narrative FTS). Returns {customer_row_id: candidates}.

    Fiscal-sponsor rows arrive here too — their EIN was blanked at intake,
    so only name/url signals are passed, which is exactly right: the
    project's own identity, not the sponsor's.
    """
    from vdl_tools.portfolio_comparison.intake.normalize import normalize_domain

    if gt_client is None:
        from vdl_tools.portfolio_comparison.gt_client import make_gt_client

        gt_client = make_gt_client()
    universe_ids = universe_ids or set()

    from vdl_tools.shared_tools.tools.logger import logger

    results: dict[str, list[Candidate]] = {}
    for _, row in rows.iterrows():
        name = (row.get("customer_name") or "").strip() or None
        domain = normalize_domain(row.get("customer_url")) or None
        if domain and "." not in domain:
            domain = None  # customer junk like "Out of Business" — not a domain
        if not name and not domain:
            continue
        try:
            hits = gt_client.search_identity(name=name, url=domain, limit=limit)
        except ValueError as exc:
            # One row's junk signals must never kill the lane.
            logger.warning("search_identity failed for %r: %s", name, exc)
            try:
                hits = gt_client.search_identity(name=name, limit=limit) if name else []
            except ValueError:
                hits = []
        if not hits and name:
            # ILIKE is a substring match; dotted acronyms ("M.A.R.S.H.
            # Project") never hit their canonical form. Retry depunctuated.
            variant = " ".join(name.replace(".", "").replace(",", "").split())
            if variant.lower() != name.lower():
                try:
                    hits = gt_client.search_identity(name=variant, limit=limit)
                except ValueError:
                    hits = []
        cands = []
        for h in hits:
            ein = normalize_ein(h.ein)
            cands.append(Candidate(
                matched_id=ein,
                matched_name=h.name or "",
                matched_url="",
                score=_SIGNAL_SCORES.get(h.signal, 0.7),
                method="api_search",
                evidence={
                    "signal": h.signal,
                    "gt_org_type": h.org_type,
                    "location": ", ".join(x for x in (h.city, h.state) if x),
                    "latest_taxyear": h.latest_taxyear,
                    "in_universe": ein in universe_ids,
                },
            ))
        if cands:
            results[row["customer_row_id"]] = cands
    return results


def apply_identity_matches(
    id_mapping: pd.DataFrame, matches: dict[str, list[Candidate]]
) -> pd.DataFrame:
    """Resolve unresolved nonprofit rows from identity-search candidates.

    Mirrors Tier-2 semantics: a sole url_exact hit auto-accepts (the org's
    own domain resolved in the datamart); anything else goes to review.
    Rows already decided or in review keep their status — candidates attach
    as evidence only.
    """
    for row_id, cands in matches.items():
        mask = (id_mapping["customer_row_id"] == row_id) & id_mapping["status"].isna()
        if not mask.any():
            continue
        top = cands[0]
        if len(cands) == 1 and top.evidence.get("signal") == "url_exact":
            idx = id_mapping.index[mask][0]
            id_mapping.loc[idx, [
                "matched_id", "matched_name", "matched_url",
                "match_method", "confidence", "in_universe",
                "status", "decided_by",
            ]] = [
                top.matched_id, top.matched_name, top.matched_url,
                "api_search", top.score, bool(top.evidence.get("in_universe")),
                "auto_matched", "auto",
            ]
        else:
            id_mapping.loc[mask, "status"] = "needs_review"
    return id_mapping
