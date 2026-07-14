"""Query the research-funding datamart into pandas DataFrames.

The government-research analogue of ``query_prepare_givingtuesday``: filters
are pushed server-side through ``ResearchFundingClient`` (full-text search,
source, fiscal-year overlap, funding minimum, has-abstract) and the results
land as a project-level DataFrame — one row per deduplicated project, with
its award rollup. ``get_awards_for_projects`` explodes a project set to
award-level rows when per-award detail (agency splits, per-award amounts,
CFDA tags) is needed.

Coverage caveat (see research-funding-datamart docs/sources.md): US sources
are keyword-filtered at ingest while CORDIS/ANR are full-corpus — result
sets are NOT comparable coverage guarantees across blocs.
"""

from dataclasses import asdict

import pandas as pd

from research_funding_datamart.client import ResearchFundingClient
from vdl_tools.shared_tools.parquet_cache import write_dataframe
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger

# search_projects page size used when draining all hits for a term.
_SEARCH_PAGE = 500


def _make_default_client() -> ResearchFundingClient:
    """Build a ResearchFundingClient from vdl-tools' postgres config.

    The client itself does not depend on vdl-tools (it reads
    ``RF_DATAMART_PG_*`` env vars or explicit components, never config.ini);
    this adapter lives on the vdl-tools side and maps ``get_configuration()``
    to the client's component args. Database is pinned to
    ``research_funding`` regardless of what the local config has set.
    """
    pg = get_configuration()["postgres"]
    return ResearchFundingClient(
        host=pg["host"],
        port=int(pg["port"]),
        user=pg["user"],
        password=pg["password"],
        database="research_funding",
    )


def _load_keywords(search_terms_list, search_terms_path, allow_empty=False):
    if search_terms_list:
        return list(search_terms_list)
    if search_terms_path:
        return pd.read_csv(search_terms_path)["term"].tolist()
    if allow_empty:
        return []
    raise ValueError("Must provide either search_terms_list or search_terms_path")


def _search_all_pages(client, term, *, sources, fiscal_years, min_amount_usd, has_abstract):
    """Drain every search hit for one term (limit/offset pagination)."""
    hits, offset = [], 0
    while True:
        page = client.search_projects(
            term,
            sources=sources,
            fiscal_years=fiscal_years,
            min_amount_usd=min_amount_usd,
            has_abstract=has_abstract,
            limit=_SEARCH_PAGE,
            offset=offset,
        )
        hits.extend(page)
        if len(page) < _SEARCH_PAGE:
            return hits
        offset += _SEARCH_PAGE


def _agency_match(project_agencies, wanted_lower: set[str]) -> bool:
    return any((a or "").lower() in wanted_lower for a in project_agencies)


def query_research_funding_data(
    search_terms_list=None,
    search_terms_path=None,
    sources=None,
    fiscal_years=None,
    agencies=None,
    min_amount_usd=None,
    has_abstract=None,
    processed_output_path=None,
    client=None,
):
    """Return a project-level DataFrame of research-funding projects.

    With search terms, runs Postgres FTS per term (all pages drained) and
    dedups by ``project_hash``, keeping each project's best rank. Without
    terms, streams the full filtered corpus via ``iter_projects``.

    Arguments:

    * ``search_terms_list`` / ``search_terms_path`` — optional keyword
      filter; ``search_terms_path`` is a CSV with a single ``term`` column
      (the GT convention). Omit both to pull everything matching the other
      filters.
    * ``sources`` — logical source names (``nsf``, ``nih``,
      ``usaspending_contracts``, ``usaspending_grants``, ``cordis``,
      ``ukri``, ``anr``). A project matches if ANY of its awards came from
      a requested source.
    * ``fiscal_years`` — sequence of fiscal years. Server-side this is an
      overlap test: a project matches if its [min_fiscal_year,
      max_fiscal_year] span touches the requested range.
    * ``agencies`` — awarding agency names, case-insensitive exact match
      against any of the project's ``awarding_agencies``. Applied
      client-side (the datamart client has no agency filter); with very
      large unfiltered pulls, prefer adding ``sources``/``fiscal_years``
      to bound the transfer. Use ``funding_by('agency')`` on the client to
      discover the exact agency strings present.
    * ``min_amount_usd`` — keep projects whose total USD (award start-year
      FX conversion; NULL amounts excluded from totals) is at least this.
    * ``processed_output_path`` — optional URI to write the result parquet
      to (S3 supported). Returns the DataFrame either way.
    * ``client`` — supply a pre-built ``ResearchFundingClient`` to override
      the default (built from vdl-tools postgres config).

    Output: one row per project — ``project_hash``, ``title``,
    ``abstract``, ``has_abstract``, ``total_amount_usd``, ``n_awards``,
    ``sources`` (list), ``awarding_agencies`` (list), ``min_start_date``,
    ``max_end_date``, ``min_fiscal_year``, ``max_fiscal_year``, and
    ``rank`` (FTS score; NaN on non-search pulls). ``project_hash`` is
    stable across re-ingests — safe to key downstream enrichment caches on.
    """
    client = client or _make_default_client()
    keywords = _load_keywords(search_terms_list, search_terms_path, allow_empty=True)

    if keywords:
        by_hash = {}
        for term in keywords:
            for hit in _search_all_pages(
                client, term,
                sources=sources, fiscal_years=fiscal_years,
                min_amount_usd=min_amount_usd, has_abstract=has_abstract,
            ):
                prev = by_hash.get(hit.project_hash)
                if prev is None or hit.rank > prev.rank:
                    by_hash[hit.project_hash] = hit
        projects = list(by_hash.values())
        logger.info("FTS over %d term(s): %d unique projects", len(keywords), len(projects))
    else:
        projects = list(
            client.iter_projects(
                sources=sources, fiscal_years=fiscal_years,
                min_amount_usd=min_amount_usd, has_abstract=has_abstract,
            )
        )
        logger.info("Full filtered pull: %d projects", len(projects))

    if agencies:
        wanted = {a.lower() for a in agencies}
        before = len(projects)
        projects = [p for p in projects if _agency_match(p.awarding_agencies, wanted)]
        logger.info("Agency filter %s: %d -> %d projects", sorted(wanted), before, len(projects))

    if not projects:
        return pd.DataFrame()

    df = pd.DataFrame.from_records([asdict(p) for p in projects])
    if "rank" not in df.columns:
        df["rank"] = float("nan")
    df = df.sort_values(
        ["rank", "total_amount_usd"], ascending=[False, False], na_position="last"
    ).reset_index(drop=True)

    if processed_output_path:
        write_dataframe(df, processed_output_path)
    return df


def get_awards_for_projects(project_hashes, client=None):
    """Return an award-level DataFrame for the given project hashes.

    One row per award with per-award amounts (native + USD), dates, PI/org,
    agency/sub-agency, CFDA tag, and lineage (``last_seen_run_id``,
    ``amended_at``). Fetches one query per project hash — fine for the
    filtered result sets ``query_research_funding_data`` produces, not for
    whole-corpus sweeps (use ``client.iter_projects`` + the datamart's SQL
    surface for those).
    """
    client = client or _make_default_client()
    rows = []
    for i, h in enumerate(project_hashes, 1):
        rows.extend(asdict(a) for a in client.get_awards(h))
        if i % 500 == 0:
            logger.info("Fetched awards for %d/%d projects", i, len(project_hashes))
    logger.info("Awards: %d rows across %d projects", len(rows), len(project_hashes))
    return pd.DataFrame.from_records(rows) if rows else pd.DataFrame()


if __name__ == "__main__":
    df = query_research_funding_data(
        search_terms_list=["direct air capture", "carbon removal"],
        fiscal_years=[2026],
        min_amount_usd=100_000,
    )
    print(df[["title", "total_amount_usd", "sources", "awarding_agencies"]].head(10))
