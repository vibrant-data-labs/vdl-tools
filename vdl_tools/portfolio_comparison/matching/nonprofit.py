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
        from givingtuesday_datamart.client.client import GtDatamartClient

        gt_client = GtDatamartClient()

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


def search_identity(name: str, url: str):
    raise NotImplementedError(
        "Blocked on porting the datamart frontend's searchOrgs SQL into "
        "GtDatamartClient.search_identity (EIN > URL domain > name/DBA > FTS). "
        "See spec §4.5 and the flagged task in givingtuesday-datamart."
    )
