"""Match-candidate contract and the per-source search interface (Tier 2).

One engagement uses exactly one for-profit source (inherited from the baseline
run), so adapters are interchangeable behind ``SourceClient``. Tier-2
implementations are next-sprint work; the interface is fixed now so Tier 1,
the queue, and the review apps can build against it.
"""

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class Candidate:
    matched_id: str
    matched_name: str
    matched_url: str
    score: float
    method: str  # url_exact | name_exact | name_fuzzy | api_search | web_research
    evidence: dict = field(default_factory=dict)


class SourceClient(Protocol):
    source: str

    def search(self, name: str, url: str) -> list[Candidate]:
        ...


class CrunchbaseClient:
    """Tier-2 adapter over vdl_tools.scrape_enrich.crunchbase.

    Domain search first (high precision: `domain_eq` on website_url), then
    name search (`contains` on identifier). Results are cached to a local
    JSON file so match reruns don't re-bill the API.
    """

    source = "crunchbase"
    SEARCH_FIELDS = [
        "identifier", "website_url", "short_description", "operating_status",
    ]

    def __init__(self, cache_path=None, limit: int = 10):
        self.limit = limit
        self.cache_path = cache_path
        self._cache = {}
        if cache_path is not None:
            import json
            from pathlib import Path

            self.cache_path = Path(cache_path)
            if self.cache_path.exists():
                self._cache = json.loads(self.cache_path.read_text())

    def _save_cache(self):
        if self.cache_path is not None:
            import json

            self.cache_path.write_text(json.dumps(self._cache))

    def _query(self, filters) -> list[dict]:
        import vdl_tools.scrape_enrich.crunchbase.api as api  # noqa: F401
        import vdl_tools.scrape_enrich.crunchbase.companies_api as companies_api

        df = companies_api.query(
            fields=self.SEARCH_FIELDS, filters=filters, limit=self.limit
        )
        return df.to_dict(orient="records") if len(df) else []

    def _to_candidates(self, hits: list[dict], name: str, signal: str) -> list[Candidate]:
        from vdl_tools.portfolio_comparison.intake.normalize import (
            identity_domain,
            normalize_name,
        )
        from vdl_tools.portfolio_comparison.matching.universe import _similarity

        candidates = []
        for hit in hits:
            identifier = hit.get("identifier") or {}
            hit_name = identifier.get("value") or ""
            score = (
                0.97 if signal == "domain"
                else round(_similarity(normalize_name(name), normalize_name(hit_name)), 3)
            )
            candidates.append(Candidate(
                matched_id=str(hit["uuid"]),
                matched_name=hit_name,
                matched_url=hit.get("website_url") or "",
                score=score,
                method="api_search",
                evidence={
                    "signal": signal,
                    "domain": identity_domain(hit.get("website_url")),
                    "description": hit.get("short_description") or "",
                    "operating_status": hit.get("operating_status") or "",
                    "cb_permalink": identifier.get("permalink") or "",
                    "in_universe": False,
                },
            ))
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates

    def search(self, name: str, url: str) -> list[Candidate]:
        import vdl_tools.scrape_enrich.crunchbase.api as api

        from vdl_tools.portfolio_comparison.intake.normalize import identity_domain

        # Platform URLs (linkedin.com/...) carry no searchable domain — a
        # domain_eq on linkedin.com returns every org whose listed website
        # is its LinkedIn page. Fall through to name search instead.
        domain = identity_domain(url)
        cache_key = f"{domain}|{(name or '').strip().lower()}"
        if cache_key in self._cache:
            return [Candidate(**c) for c in self._cache[cache_key]]

        hits, signal = [], None
        if domain:
            hits = self._query([api.domain_eq("website_url", [domain])])
            # CB's domain_eq matches the registrable domain, so shared-hosting
            # sites (e-z-pack.myshopify.com) return every store on the host.
            # Only exact-host hits are identity evidence.
            hits = [h for h in hits if identity_domain(h.get("website_url")) == domain]
            signal = "domain"
        if not hits and name and name.strip():
            hits = self._query([api.contains("identifier", [name.strip()])])
            signal = "name"
        candidates = self._to_candidates(hits, name, signal) if hits else []

        from dataclasses import asdict

        self._cache[cache_key] = [asdict(c) for c in candidates]
        self._save_cache()
        return candidates


class NZIClient:
    """Tier-2 adapter over search_netzero_api.

    The NZI wrapper supports name search only (no website filter), so
    candidates are found by name and confirmed by comparing the returned
    website domain — see spec §4.4.
    """

    source = "nzi"

    def __init__(self, cache_path=None, limit: int = 10):
        self.cache_path = cache_path
        self.limit = limit

    def search(self, name: str, url: str) -> list[Candidate]:
        raise NotImplementedError(
            "Tier-2 NZI search is follow-on work; wrap "
            "vdl_tools/scrape_enrich/netzero_insights/search_netzero_api.py here "
            "(name search only — confirm by returned website domain, spec §4.4)"
        )


def get_source_client(source: str, **kwargs) -> SourceClient:
    clients = {"crunchbase": CrunchbaseClient, "nzi": NZIClient}
    return clients[source](**kwargs)


def pick_converging_candidate(
    customer_domain: str, candidates: list[Candidate], min_score: float = 0.95
) -> Candidate | None:
    """Among near-exact-name candidates, find the ONE whose domain redirects
    to the same site as the customer's (aquila.earth → aquila.space).

    Many same-named companies is the normal case for name searches; redirect
    convergence singles out the right one mechanically. Returns None unless
    exactly one candidate converges — two converging candidates means
    something strange, and strange goes to review.
    """
    from vdl_tools.portfolio_comparison.intake.normalize import domains_converge

    if not customer_domain:
        return None
    winners = [
        c for c in candidates
        if c.score >= min_score
        and c.evidence.get("domain")
        and domains_converge(customer_domain, c.evidence["domain"])
    ]
    return winners[0] if len(winners) == 1 else None
