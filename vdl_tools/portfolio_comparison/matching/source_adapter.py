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
    """Tier-2 adapter over vdl_tools.scrape_enrich.crunchbase (name + domain search)."""

    source = "crunchbase"

    def search(self, name: str, url: str) -> list[Candidate]:
        raise NotImplementedError(
            "Tier-2 Crunchbase search is next-sprint work; wrap "
            "vdl_tools/scrape_enrich/crunchbase/organizations_api*.py here"
        )


class NZIClient:
    """Tier-2 adapter over search_netzero_api.

    The NZI wrapper supports name search only (no website filter), so
    candidates are found by name and confirmed by comparing the returned
    website domain — see spec §4.4.
    """

    source = "nzi"

    def search(self, name: str, url: str) -> list[Candidate]:
        raise NotImplementedError(
            "Tier-2 NZI search is next-sprint work; wrap "
            "vdl_tools/scrape_enrich/netzero_insights/search_netzero_api.py here"
        )


def get_source_client(source: str) -> SourceClient:
    clients = {"crunchbase": CrunchbaseClient, "nzi": NZIClient}
    return clients[source]()
