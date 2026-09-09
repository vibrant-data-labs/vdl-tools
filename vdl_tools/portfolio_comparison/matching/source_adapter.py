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
        "funding_total", "num_funding_rounds",
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

    def _autocomplete(self, name: str) -> list[dict]:
        """CB's autocomplete — the forgiving search the manual UI uses
        (typo-tolerant, partial names). Returns search-result-shaped hits
        with websites filled by one batched details query."""
        import requests

        from vdl_tools.shared_tools.tools.config_utils import get_configuration

        key = get_configuration()["crunchbase"]["api_key"]
        res = requests.get(
            "https://api.crunchbase.com/api/v4/autocompletes",
            params={"query": name, "collection_ids": "organizations",
                    "limit": min(self.limit, 5), "user_key": key},
            timeout=30,
        )
        if not res.ok:
            return []
        uuids = [e["identifier"]["uuid"] for e in res.json().get("entities", [])]
        if not uuids:
            return []
        import vdl_tools.scrape_enrich.crunchbase.api as api

        return self._query([api.includes("uuid", uuids)])

    def _to_candidates(
        self, hits: list[dict], name: str, signal: str, requested_domain: str = ""
    ) -> list[Candidate]:
        from vdl_tools.portfolio_comparison.intake.normalize import (
            identity_domain,
            normalize_name,
        )
        from vdl_tools.portfolio_comparison.matching.universe import _similarity

        def _financials(hit):
            total = hit.get("funding_total")
            usd = total.get("value_usd") if isinstance(total, dict) else None
            rounds = hit.get("num_funding_rounds")
            has_rounds = isinstance(rounds, (int, float)) and rounds == rounds and rounds > 0
            return bool(usd) or has_rounds, int(usd) if usd else None

        candidates = []
        for hit in hits:
            identifier = hit.get("identifier") or {}
            hit_name = identifier.get("value") or ""
            hit_domain = identity_domain(hit.get("website_url"))
            has_financials, funding_usd = _financials(hit)
            # Per-hit promotion: a name/autocomplete hit whose website
            # exact-hosts the customer's domain is domain-grade evidence.
            hit_signal = signal
            if hit_signal != "domain" and requested_domain and hit_domain == requested_domain:
                hit_signal = "domain"
            score = (
                0.97 if hit_signal == "domain"
                else round(_similarity(normalize_name(name), normalize_name(hit_name)), 3)
            )
            candidates.append(Candidate(
                matched_id=str(hit["uuid"]),
                matched_name=hit_name,
                matched_url=hit.get("website_url") or "",
                score=score,
                method="api_search",
                evidence={
                    "signal": hit_signal,
                    "domain": hit_domain,
                    "description": hit.get("short_description") or "",
                    "operating_status": hit.get("operating_status") or "",
                    "cb_permalink": identifier.get("permalink") or "",
                    "has_financials": has_financials,
                    "funding_usd": funding_usd,
                    "in_universe": False,
                },
            ))
        # Funded-first at equal score: duplicate CB profiles on one domain,
        # the one reporting financial data is the maintained profile.
        candidates.sort(
            key=lambda c: (-c.score, not c.evidence.get("has_financials"))
        )
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

        from vdl_tools.portfolio_comparison.intake.normalize import name_variants

        hits, signal = [], None
        if domain:
            hits = self._query([api.domain_eq("website_url", [domain])])
            # CB's domain_eq matches the registrable domain, so shared-hosting
            # sites (e-z-pack.myshopify.com) return every store on the host.
            # Only exact-host hits are identity evidence.
            hits = [h for h in hits if identity_domain(h.get("website_url")) == domain]
            signal = "domain"
        if not hits:
            # contains is strict; autocomplete is the forgiving search the
            # manual UI uses. Try both, walking the name-variant ladder.
            for variant in name_variants(name):
                hits = self._query([api.contains("identifier", [variant])])
                if not hits:
                    hits = self._autocomplete(variant)
                if hits:
                    break
            signal = "name"
        candidates = self._to_candidates(hits, name, signal, requested_domain=domain) if hits else []

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

    def _search_api(self, name: str) -> list[dict]:
        from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import (
            search_companies,
        )

        res = search_companies(name=name, limit=self.limit, checkpoint_dir=None)
        return (res or {}).get("results", [])

    def search(self, name: str, url: str) -> list[Candidate]:
        from vdl_tools.portfolio_comparison.intake.normalize import (
            identity_domain,
            normalize_name,
        )
        from vdl_tools.portfolio_comparison.matching.universe import _similarity

        name = (name or "").strip()
        if not name:
            return []
        domain = identity_domain(url)
        cache_key = f"{domain}|{name.lower()}"
        if cache_key in self._cache:
            return [Candidate(**c) for c in self._cache[cache_key]]

        from vdl_tools.portfolio_comparison.intake.normalize import name_variants

        raw_hits = []
        for variant in name_variants(name):
            raw_hits = self._search_api(variant)
            if raw_hits:
                break

        candidates = []
        for hit in raw_hits:
            hit_domain = identity_domain(hit.get("website"))
            confirmed = bool(domain) and hit_domain == domain
            score = (
                0.97 if confirmed
                else round(_similarity(normalize_name(name), normalize_name(hit.get("name"))), 3)
            )
            candidates.append(Candidate(
                matched_id=str(hit["clientID"]),
                matched_name=hit.get("name") or "",
                matched_url=hit.get("website") or "",
                score=score,
                method="api_search",
                evidence={
                    "signal": "domain" if confirmed else "name",
                    "domain": hit_domain,
                    "description": hit.get("pitchLine") or "",
                    "nzi": True,
                    "in_universe": False,
                },
            ))
        candidates.sort(key=lambda c: c.score, reverse=True)
        # A domain-confirmed hit makes the same-name noise irrelevant.
        confirmed_only = [c for c in candidates if c.evidence["signal"] == "domain"]
        if confirmed_only:
            candidates = confirmed_only

        from dataclasses import asdict

        self._cache[cache_key] = [asdict(c) for c in candidates]
        self._save_cache()
        return candidates


class ChainedSourceClient:
    """Query sources in preference order without letting preference shadow
    evidence strength.

    Text-objective engagements prefer NZI descriptions, so NZI is consulted
    first — but a preferred source's *name-signal* hits must never hide a
    later source's *domain-confirmed* hit. The first source producing a
    domain-signal hit wins outright; otherwise all sources' name-signal
    hits merge, preferred source first. Failing clients (missing
    credentials, API errors) are logged and skipped, never fatal.
    """

    def __init__(self, clients: list):
        self.clients = clients
        self.source = "+".join(c.source for c in clients)

    def search(self, name: str, url: str) -> list[Candidate]:
        from vdl_tools.shared_tools.tools.logger import logger

        merged: list[Candidate] = []
        for client in self.clients:
            try:
                hits = client.search(name, url)
            except Exception as exc:
                logger.warning("%s search failed, trying next source: %s", client.source, exc)
                continue
            if any(c.evidence.get("signal") == "domain" for c in hits):
                return hits
            merged.extend(hits)
        return merged


def get_source_client(source: str, **kwargs) -> SourceClient:
    clients = {"crunchbase": CrunchbaseClient, "nzi": NZIClient}
    return clients[source](**kwargs)


def pick_funded_duplicate(candidates: list[Candidate]) -> Candidate | None:
    """Duplicate source profiles on ONE domain (all domain-signal hits): the
    profile reporting financial data is the maintained one — choose it
    (Zein's ruling, 2026-08-07). Returns None unless exactly one candidate
    has financials; both-funded or both-unfunded stays with a human."""
    if len(candidates) < 2:
        return None
    if any(c.evidence.get("signal") != "domain" for c in candidates):
        return None
    if len({c.evidence.get("domain") for c in candidates}) != 1:
        return None
    funded = [c for c in candidates if c.evidence.get("has_financials")]
    if len(funded) != 1:
        return None
    funded[0].evidence["funded_duplicate_pick"] = True
    return funded[0]


def pick_converging_candidate(
    customer_domain: str, candidates: list[Candidate], min_score: float = 0.95
) -> Candidate | None:
    """Among near-exact-name candidates, find the ONE whose domain redirects
    to the same site as the customer's (aquila.earth → aquila.space).

    Many same-named companies is the normal case for name searches; redirect
    convergence singles out the right one mechanically. The ambiguity guard
    counts distinct DOMAINS, not candidates: a chained search returns the same
    org once per source (NZI + CB both listing aquila.earth), and that is
    corroboration. Two different converging domains means something strange,
    and strange goes to review. Ties break by list order, which is chain
    preference (NZI first for text engagements).
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
    if not winners:
        return None
    domains = {c.evidence["domain"] for c in winners}
    return winners[0] if len(domains) == 1 else None
