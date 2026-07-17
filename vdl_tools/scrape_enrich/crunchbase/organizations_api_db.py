from pathlib import Path
from typing import TypedDict
from more_itertools import chunked
import pandas as pd
import hashlib
import json
import math
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

import vdl_tools.scrape_enrich.crunchbase.companies_api as companies
import vdl_tools.scrape_enrich.crunchbase.acquisitions_api as acquisitions
import vdl_tools.scrape_enrich.crunchbase.ipos_api as ipos
import vdl_tools.scrape_enrich.crunchbase.funding_rounds_api as funding_rounds
import vdl_tools.scrape_enrich.crunchbase.people_api as people
import vdl_tools.scrape_enrich.crunchbase.api as api
from vdl_tools.shared_tools.tools.logger import logger

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.database_cache.database_models import (
    CbOrganization, CbFundingRound, CbPerson, CbQueryCache,
)


__query_step = 200


class QueryCompaniesExtendedResult(TypedDict, total=False):
    organizations: pd.DataFrame
    funding_rounds: pd.DataFrame | None
    people_investors: pd.DataFrame | None
    org_investors: pd.DataFrame | None
    founders: pd.DataFrame | None


CB_TABLE_NAMES: tuple[str, ...] = (
    "organizations",
    "funding_rounds",
    "founders",
    "org_investors",
    "people_investors",
)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def __rows_to_df(rows) -> pd.DataFrame | None:
    """Convert SQLAlchemy model instances to a DataFrame."""
    if not rows:
        return None
    return pd.DataFrame(
        [{c.name: getattr(r, c.name) for c in r.__class__.__table__.columns} for r in rows]
    )


def __coerce_to_model_schema(df: pd.DataFrame | None, model) -> pd.DataFrame | None:
    """Normalize column dtypes to match the SQLAlchemy model.

    DB-sourced frames (via __rows_to_df) get native Python types from
    SQLAlchemy — DateTime columns come back as datetime objects. API-sourced
    frames get raw JSON types — the same DateTime columns are ISO strings.
    Concatenating the two yields an object column with mixed types that
    PyArrow rejects at parquet write time. Coerce DateTime columns on each
    side to pandas datetime64[ns, UTC] so the concat is type-safe.
    """
    if df is None or df.empty:
        return df
    for col in model.__table__.columns:
        if col.name not in df.columns:
            continue
        if isinstance(col.type, sa.DateTime):
            df[col.name] = pd.to_datetime(df[col.name], errors="coerce", utc=True)
    return df


def __dedup_df(df):
    if df is None or df.empty:
        return df
    return df.loc[df.astype(str).drop_duplicates().index]


def __canonical_filter_repr(f) -> str:
    if isinstance(f, dict):
        return json.dumps(f, sort_keys=True, separators=(",", ":"))
    return str(f)


def __hash_query(search_condition, items_list, extra_filters):
    query_data = {
        "extra_filters": [__canonical_filter_repr(x) for x in extra_filters],
        "items_list": list(items_list),
        "search_condition": search_condition,
    }
    payload = json.dumps(query_data, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode()).hexdigest()


def __upsert_dataframe(
    session,
    model,
    df,
    chunk_size=1000,
):
    if df is None or df.empty:
        return

    table = model.__table__
    table_col_names = {col.name for col in table.columns}
    pk_cols = [col.name for col in table.primary_key.columns]
    non_pk_cols = [
        c.name for c in table.columns
        if c.name not in set(pk_cols) and c.name != "updated_at"
    ]

    # date_format="iso" so datetime64 columns (e.g. CbPerson.created_at)
    # serialize as ISO strings Postgres can cast to timestamp, not epoch ints.
    safe_records = json.loads(df.to_json(orient="records", date_format="iso"))
    safe_records = [
        {k: v for k, v in rec.items() if k in table_col_names and k != "updated_at"}
        for rec in safe_records
    ]

    logger.info("Upserting %s records into %s", len(safe_records), model.__tablename__)
    num_chunks = math.ceil(len(safe_records) / chunk_size)
    for i, chunk in enumerate(chunked(safe_records, chunk_size)):
        logger.info("Upserting chunk %s of %s", i + 1, num_chunks)
        stmt = insert(table).values(chunk)
        set_ = {col: stmt.excluded[col] for col in non_pk_cols}
        if "updated_at" in table_col_names:
            set_["updated_at"] = sa.func.now()
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_cols,
            set_=set_,
        )
        session.execute(stmt)
        session.commit()


def __fetch_cached(session, model, ids, api_query_fn, *, use_cache, save_to_cache, force=False):
    """Load records from DB cache, fetch missing via API, upsert new results.

    When ``force`` is True, skip the DB read and re-fetch every id from the
    API (results are still upserted when ``save_to_cache`` is True).
    """
    if not ids:
        return None

    cached_df = None
    if use_cache and not force:
        rows = (
            session.query(model)
            .filter(model.uuid.in_(ids))
            .all()
        )
        cached_df = __coerce_to_model_schema(__rows_to_df(rows), model)

    missing = ids
    if cached_df is not None and not cached_df.empty:
        found = set(cached_df["uuid"].tolist())
        missing = [i for i in ids if i not in found]

    if missing:
        logger.info("Fetching %s missing %s from API...", len(missing), model.__tablename__)
        new_df = api_query_fn(missing)
        if new_df is not None and not new_df.empty:
            new_df = __coerce_to_model_schema(new_df, model)
            cached_df = (
                pd.concat([cached_df, new_df], ignore_index=True)
                if cached_df is not None
                else new_df
            )
            if save_to_cache:
                __upsert_dataframe(session, model, new_df)

    return cached_df


# ---------------------------------------------------------------------------
# Iterative Crunchbase queries (unchanged from organizations_api_extended)
# ---------------------------------------------------------------------------

def __iterative_query(
    query_method,
    fields,
    search_field,
    query_list,
    extra_filters = [],
    operator = 'contains',
    limit = None,
):
    if operator == 'includes':
        method = api.includes
    elif operator == 'eq' or operator == 'equals':
        method = api.eq
    elif operator == 'domain_eq':
        method = api.domain_eq
    elif operator == 'domain_includes':
        method = api.domain_includes
    else:
        method = api.contains

    results = pd.DataFrame()
    for i in range(0, len(query_list), __query_step):
        logger.info('Filtering by items from %s to %s out of %s', i + 1, i + __query_step, len(query_list))
        batch = query_method(
            fields = fields,
            filters = [
                method(search_field, query_list[i:i+__query_step]),
                *extra_filters
            ],
            limit=limit,
        )
        results = pd.concat([results, batch], ignore_index=True).drop_duplicates(subset=["uuid"])
        if limit and len(results) >= limit:
            break
    logger.info(f"Found {len(results)} results")
    return results


def companies_query(search_terms_list, extra_filters, limit=None):
    logger.info('Querying companies by description...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "description",
        search_terms_list,
        extra_filters=extra_filters,
        limit=limit,
    )


def companies_query_names(name_list, extra_filters):
    logger.info('Querying companies by names...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "identifier",
        name_list,
        extra_filters=extra_filters
    )


def companies_id_query(ids):
    logger.info('Querying companies by id...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "identifier",
        ids,
        operator="includes"
    )


def companies_industry_query(industry_ids, extra_filters=[], limit=None):
    logger.info('Querying companies by industry...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "categories",
        industry_ids,
        extra_filters=extra_filters,
        operator='includes',
        limit=limit,
    )


def companies_query_urls(urls, extra_filters=[]):
    logger.info('Querying companies by url...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "website_url",
        urls,
        operator="domain_includes",
        extra_filters=extra_filters,
    )


def companies_ipo_query(organization_uuids, extra_filters=[]):
    logger.info('Querying IPO events by company ID...')
    return __iterative_query(
        ipos.query,
        ipos.all_fields,
        "organization_identifier",
        organization_uuids,
        operator="includes",
        extra_filters=extra_filters
    )


def companies_aquiree_query(organization_uuids, extra_filters=[]):
    logger.info('Querying Acquisitions by acquiree ID...')
    return __iterative_query(
        acquisitions.query,
        acquisitions.all_fields,
        "acquiree_identifier",
        organization_uuids,
        operator="includes",
        extra_filters=extra_filters
    )


def funding_rounds_query(companies_uuids):
    logger.info('Querying funding rounds by organization identifier...')
    return __iterative_query(
        funding_rounds.query,
        funding_rounds.all_fields,
        "funded_organization_identifier",
        companies_uuids,
        operator="includes"
    )


def funding_rounds_query_by_investor_id(investor_uuids, extra_filters=[]):
    logger.info('Querying funding rounds by investor identifier...')
    return __iterative_query(
        funding_rounds.query,
        funding_rounds.all_fields,
        "investor_identifiers",
        investor_uuids,
        operator="includes",
        extra_filters=extra_filters
    )


def people_query(people_ids):
    logger.info('Querying people by identifier...')
    return __iterative_query(
        people.query,
        people.all_fields,
        "identifier",
        people_ids,
        operator="includes"
    )


# ---------------------------------------------------------------------------
# Aggregation helpers (unchanged from organizations_api_extended)
# ---------------------------------------------------------------------------

def __get_aggregated_data(organizations: pd.DataFrame, list_column: str):
    temp_data = organizations[[list_column, "permalink"]]
    temp_data = temp_data.rename(columns = {"permalink": "org_permalink"})
    temp_data = temp_data[temp_data[list_column].notnull()]
    temp_data = temp_data.explode(list_column)
    temp_data = temp_data.reset_index()
    temp_data = temp_data[temp_data[list_column].notnull()]
    temp_data["type"] = temp_data.apply(lambda x: x[list_column]['entity_def_id'] if 'permalink' in x[list_column] else None, axis=1)
    temp_data = temp_data[temp_data['type'].notnull()]
    temp_data['permalink'] = temp_data.apply(lambda x: x[list_column]['permalink'], axis=1)
    temp_data['uuid'] = temp_data.apply(lambda x: x[list_column].get('uuid'), axis=1)
    return temp_data


def __investors_query(session, investor_temp_data: pd.DataFrame, entity_type, *, use_cache, save_to_cache, force=False):
    df = investor_temp_data[investor_temp_data["type"] == entity_type]
    if entity_type == "person":
        ids = df['uuid'].dropna().to_list()
        if len(ids) == 0:
            return None
        return __fetch_cached(session, CbPerson, ids, people_query, use_cache=use_cache, save_to_cache=save_to_cache, force=force)

    companies_ids = list(set(df['uuid'].dropna().to_list()))
    if len(companies_ids) == 0:
        return None
    return __fetch_cached(session, CbOrganization, companies_ids, companies_id_query, use_cache=use_cache, save_to_cache=save_to_cache, force=force)


def __combine_lists(x):
    res_list = x[~x.isnull()].to_list()
    res_list = [item for sublist in res_list for item in sublist]
    res_list = list(
        {
            item['permalink']: item
            for item in res_list
        }.values()
    )
    return res_list


def __aggregate_investors(funding_rounds_df: pd.DataFrame):
    temp_data = funding_rounds_df.groupby('_funded_org_id').aggregate({
        'investor_identifiers': __combine_lists
    })
    temp_data = temp_data.reset_index()
    temp_data['org_permalink'] = temp_data['_funded_org_id']
    temp_data = temp_data.explode('investor_identifiers')
    temp_data = temp_data.reset_index()
    temp_data = temp_data[temp_data['investor_identifiers'].notnull()]
    temp_data['type'] = temp_data['investor_identifiers'].apply(lambda x: x['entity_def_id'])
    temp_data['permalink'] = temp_data['investor_identifiers'].apply(lambda x: x['permalink'])
    temp_data['uuid'] = temp_data['investor_identifiers'].apply(lambda x: x.get('uuid'))
    return temp_data


# ---------------------------------------------------------------------------
# Main entry point — like organizations_api_extended.query_companies_extended
# but with Postgres DB caching instead of JSON files.
# ---------------------------------------------------------------------------

def query_companies_extended(
    items_list,
    search_condition='search_terms',
    extra_filters=[],
    force_query=False,
    force_refresh_records=False,
    use_cache=True,
    save_to_cache=True,
    save_to_uri=None,
) -> QueryCompaniesExtendedResult | None:
    """Query Crunchbase organizations and related entities with Postgres DB caching.

    Searches for companies via the Crunchbase API based on the given search
    condition, then resolves their funding rounds, investors (people and
    organizations), and founders. Results are cached in a Postgres database
    so that subsequent calls can skip the API for already-fetched records.

    Parameters
    ----------
    items_list : list
        Values to search for. Interpretation depends on ``search_condition``:
        search terms, company names, URLs, industry IDs, or Crunchbase UUIDs.
    search_condition : {'search_terms', 'name', 'url', 'industry', 'id'}, default 'search_terms'
        Determines how ``items_list`` is interpreted and which Crunchbase API
        endpoint is queried.
    extra_filters : list, optional
        Additional Crunchbase API filter predicates (e.g.,
        ``[api.eq("status", "operating")]``). Ignored when
        ``search_condition='id'``.
    force_query : bool, default False
        When True, bypass the query-level cache and always hit the API for
        organizations (per-record caching via ``use_cache`` still applies).
    force_refresh_records : bool, default False
        When True, bypass the per-record DB cache for investors, founders,
        and (for ``search_condition='id'``) organizations — every record is
        re-fetched from the API so previously cached rows pick up updated
        data. Results are still upserted when ``save_to_cache`` is True.
        Combine with ``force_query=True`` for a full re-pull. Note this
        re-fetches every investor/founder uuid in the result set (batched
        200 per API request), so use for periodic refreshes, not every run.
    use_cache : bool, default True
        When True, attempt to load organizations, funding rounds, investors,
        and founders from the Postgres cache before falling back to the API.
    save_to_cache : bool, default True
        When True, upsert newly fetched records into the Postgres cache for
        future reuse. Independent of ``use_cache`` and ``force_query``.

    save_to_uri : str, optional
        When provided, write the 5 result tables as Parquet under this URI,
        one file per table (``{save_to_uri}/{table_name}.parquet``). Accepts
        a local directory path, ``file://`` URI, or ``s3://`` URI (the full
        URI including bucket, e.g. ``s3://shared-data-clone/cb_raw/fisheries``).
        Any files currently at that location will be overwritten. If not
        provided, results are returned in memory only.

    Notes
    -----
    ``use_cache`` controls whether **any** DB reads happen.
    ``force_query`` is narrower: it only skips the org query-cache /
    id-preload and the funding-rounds DB preload, but still allows DB reads
    for investors and founders when ``use_cache=True``.
    ``force_refresh_records`` covers that remaining piece: it re-fetches
    investor/founder (and id-queried org) records from the API even when
    they already exist in the DB.

    Typical combinations:

    ================  ==============  =========================================
    ``use_cache``     ``force_query`` Effect
    ================  ==============  =========================================
    True              False           Normal: reuse query cache + DB where
                                      possible, API only for gaps.
    True              True            Refresh org list + funding rounds from
                                      API; still use DB for investors and
                                      founders.
    False             ``*``           Skip all DB reads; fetch everything from
                                      the API.
    ================  ==============  =========================================

    Returns
    -------
    QueryCompaniesExtendedResult or None
        A TypedDict with the following keys, or ``None`` if no organizations
        matched the query:

        - **organizations** : pd.DataFrame -- matched Crunchbase organizations.
        - **funding_rounds** : pd.DataFrame or None -- funding rounds for
          organizations that have them.
        - **people_investors** : pd.DataFrame or None -- person-type investors
          linked via funding rounds, merged with their organization permalink.
        - **org_investors** : pd.DataFrame or None -- organization-type
          investors, merged with their investee organization permalink.
        - **founders** : pd.DataFrame or None -- founders of the matched
          organizations, merged with their organization permalink.
    """
    extra_filters = extra_filters or []
    query_hash = __hash_query(search_condition, items_list, extra_filters)

    with get_session() as session:
        # --- Organizations ---
        organizations: pd.DataFrame = None

        if use_cache and not force_query:
            if search_condition == 'id':
                organizations = __fetch_cached(
                    session, CbOrganization, items_list, companies_id_query,
                    use_cache=True, save_to_cache=save_to_cache,
                    force=force_refresh_records,
                )
            else:
                cache_entry = session.query(CbQueryCache).filter_by(query_hash=query_hash).first()
                if cache_entry and cache_entry.resulting_uuids:
                    uuids = list(cache_entry.resulting_uuids)
                    organizations = __rows_to_df(
                        session.query(CbOrganization).filter(CbOrganization.uuid.in_(uuids)).all()
                    )
                    if organizations is not None:
                        logger.info("Loaded %s organizations from query cache.", len(organizations))

        if organizations is None or organizations.empty:
            if search_condition == 'name':
                organizations = companies_query_names(items_list, extra_filters)
            elif search_condition == 'url':
                organizations = companies_query_urls(items_list, extra_filters)
            elif search_condition == 'industry':
                organizations = companies_industry_query(items_list, extra_filters)
            elif search_condition == 'id':
                organizations = companies_id_query(items_list)
            elif search_condition == 'search_terms':
                organizations = companies_query(items_list, extra_filters)
            else:
                raise ValueError(f'Invalid search condition: {search_condition}')

            organizations = __dedup_df(organizations)

            if organizations is not None and not organizations.empty and save_to_cache:
                __upsert_dataframe(session, CbOrganization, organizations)
                if search_condition != "id":
                    uuids = organizations["uuid"].tolist() if "uuid" in organizations.columns else []
                    stmt = insert(CbQueryCache).values([{
                        "query_hash": query_hash,
                        "search_condition": search_condition,
                        "items_list": list(items_list),
                        "extra_filters": [__canonical_filter_repr(f) for f in extra_filters],
                        "resulting_uuids": uuids,
                    }]).on_conflict_do_update(
                        index_elements=["query_hash"],
                        set_={"resulting_uuids": uuids, "updated_at": sa.func.now()},
                    )
                    session.execute(stmt)
                    session.commit()

        if organizations is None or organizations.empty:
            return None

        # --- Funding rounds ---
        companies_funding_rounds: pd.DataFrame = None

        orgs_with_funding = organizations[organizations['num_funding_rounds'] > 0]
        org_ids = orgs_with_funding[orgs_with_funding['permalink'].notnull()]['permalink'].to_list()

        if len(org_ids) > 0:
            logger.info("Found %s organizations with funding", len(org_ids))

            if use_cache and not force_query:
                permalink_expr = CbFundingRound.funded_organization_identifier["permalink"].astext
                rows = session.query(CbFundingRound).filter(permalink_expr.in_(org_ids)).all()
                companies_funding_rounds = __rows_to_df(rows)
                if companies_funding_rounds is not None and not companies_funding_rounds.empty:
                    logger.info("Loaded %s funding rounds from database.", len(companies_funding_rounds))

            if companies_funding_rounds is None or companies_funding_rounds.empty:
                companies_funding_rounds = funding_rounds_query(org_ids)
                companies_funding_rounds = __dedup_df(companies_funding_rounds)
                if companies_funding_rounds is not None and not companies_funding_rounds.empty and save_to_cache:
                    __upsert_dataframe(session, CbFundingRound, companies_funding_rounds)

        # --- Investors ---
        people_investors = None
        org_investors = None

        if companies_funding_rounds is None or companies_funding_rounds.empty:
            logger.info("No funding rounds found")
        else:
            companies_funding_rounds['_funded_org_id'] = companies_funding_rounds['funded_organization_identifier'].apply(lambda x: x['permalink'] if isinstance(x, dict) else None)
            investor_temp_data = __aggregate_investors(companies_funding_rounds)

            logger.info("Found %s organizations with known investors", investor_temp_data.shape[0])
            people_investors = __investors_query(session, investor_temp_data, "person", use_cache=use_cache, save_to_cache=save_to_cache, force=force_refresh_records)
            if people_investors is None:
                logger.info('Investors of type `person` are not found, skipping...')
            else:
                temp_investors = investor_temp_data[investor_temp_data['type'] == 'person'][['uuid', 'org_permalink']]
                people_investors = pd.merge(temp_investors, people_investors, how="left", on="uuid")
                people_investors = __dedup_df(people_investors)

            org_investors = __investors_query(session, investor_temp_data, "organization", use_cache=use_cache, save_to_cache=save_to_cache, force=force_refresh_records)
            if org_investors is None:
                logger.info('Investors of type `organizations` are not found, skipping...')
            else:
                temp_investors = investor_temp_data[investor_temp_data['type'] == 'organization'][['uuid', 'org_permalink']]
                org_investors = pd.merge(temp_investors, org_investors, how="left", on="uuid")
                org_investors = __dedup_df(org_investors)

        # --- Founders ---
        founders = None
        founders_temp_data = __get_aggregated_data(organizations, "founder_identifiers")
        logger.info("Found %s organizations with known founders", founders_temp_data.shape[0])
        founder_ids = founders_temp_data['uuid'].dropna().to_list()

        if len(founder_ids) == 0:
            logger.info('No founders found, skipping...')
        else:
            founders = __fetch_cached(session, CbPerson, founder_ids, people_query, use_cache=use_cache, save_to_cache=save_to_cache, force=force_refresh_records)
            if founders is not None and not founders.empty:
                founders = pd.merge(founders_temp_data[['uuid', 'org_permalink']], founders, how="left", on="uuid")
                founders = __dedup_df(founders)

        results = {
            "organizations": organizations,
            "funding_rounds": companies_funding_rounds,
            "people_investors": people_investors,
            "org_investors": org_investors,
            "founders": founders,
        }
        if save_to_uri:
            search_results_to_parquet(
                results,
                save_to_uri,
                search_metadata={
                    "search_condition": search_condition,
                    "items_list": list(items_list),
                    "extra_filters": [str(f) for f in (extra_filters or [])],
                },
            )

        return results


def search_results_to_parquet(
    search_results: QueryCompaniesExtendedResult,
    dir_uri: str,
    search_metadata: dict | None = None,
) -> dict[str, str]:
    """Persist the 5 CB result DataFrames as Parquet under ``dir_uri``.

    Parameters
    ----------
    search_results
        The dict returned from :func:`query_companies_extended`.
    dir_uri
        URI of the directory-like container to write into. Accepts a local
        directory path, ``file://`` URI, or full ``s3://`` URI (include bucket,
        e.g. ``s3://shared-data-clone/cb_raw/fisheries``). Files are written
        as ``{dir_uri}/{table_name}.parquet`` and overwrite any existing
        file at those paths.
    search_metadata
        Query context (search terms, filters, etc.) embedded in each Parquet
        file's footer for lineage.

    Returns
    -------
    dict[str, str]
        ``{table_name: written_uri}``.
    """
    # Local import to avoid pulling pyarrow/fsspec at module import time.
    from vdl_tools.shared_tools.parquet_cache import write_dataframes

    lineage = {
        "source": "crunchbase.organizations_api_db.query_companies_extended",
        **(search_metadata or {}),
    }
    return write_dataframes(
        {k: v for k, v in search_results.items() if v is not None},
        dir_uri=dir_uri,
        lineage=lineage,
    )


def load_search_results_from_parquet(
    dir_uri: str,
    names: list[str] | None = None,
    *,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> dict[str, pd.DataFrame]:
    """Load the 5 CB result DataFrames previously written by
    :func:`search_results_to_parquet`.

    For ``s3://`` sources, reads go through a local filecache
    (``~/.cache/vdl-tools/parquet``) so each user downloads each file once,
    with an ETag HEAD check on every read to avoid silent stale reads when
    someone else pushes a new version.

    Parameters
    ----------
    dir_uri
        URI of the directory-like container to read from (local path,
        ``file://`` URI, or full ``s3://`` URI including bucket).
    names
        Subset of tables to load. Defaults to all 5.
    use_cache
        If False, bypass the local cache and read straight from S3 every
        time. Mostly useful for debugging or confirming remote contents.
    cache_dir
        Override the default cache directory (``~/.cache/vdl-tools/parquet``).
    check_remote
        If True (default), HEAD-check the remote ETag on every open. Set to
        False for offline / airplane use — you'll serve whatever is in the
        local cache without validating against the remote.
    """
    from vdl_tools.shared_tools.parquet_cache import read_dataframes

    names = names or list(CB_TABLE_NAMES)
    return read_dataframes(
        dir_uri=dir_uri,
        names=names,
        use_cache=use_cache,
        cache_dir=cache_dir,
        check_remote=check_remote,
    )
