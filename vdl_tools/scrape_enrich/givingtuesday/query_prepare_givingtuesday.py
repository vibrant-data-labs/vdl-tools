"""Build the Giving Tuesday CB-shaped DataFrame for ed-tracker / similar pipelines.

Server-side eligibility filters via ``GtDatamartClient.search_nonprofits``
push keyword match, avg-contributions threshold, avg-grants threshold, and
min-num-grants into a single Postgres query. Bulk per-EIN basic fields and
grant summaries are then fetched in two further round trips and pivoted
into the wide ``{prefix}_{YYYY}`` form the downstream Crunchbase-shaped
consumers expect.
"""

from dataclasses import asdict

import pandas as pd

from givingtuesday_datamart.client import GtDatamartClient
from vdl_tools.shared_tools.parquet_cache import write_dataframe
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger


def _make_default_client() -> GtDatamartClient:
    """Build a GtDatamartClient from vdl-tools' postgres config.

    The client itself does not depend on vdl-tools (so it is shippable as a
    standalone package); this adapter lives on the vdl-tools side and maps
    ``get_configuration()`` to the client's component args. Database is
    pinned to ``gt_datamart`` regardless of what the local config has set.
    """
    pg = get_configuration()["postgres"]
    return GtDatamartClient(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        database="gt_datamart",
    )


def _load_keywords(search_terms_list, search_terms_path, allow_empty=False):
    if search_terms_list:
        return search_terms_list
    if search_terms_path:
        return pd.read_csv(search_terms_path)["term"].tolist()
    if allow_empty:
        return []
    raise ValueError("Must provide either search_terms_list or search_terms_path")


def _records_to_df(rows):
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records([asdict(r) for r in rows])


def _pivot_year_col(long_df, ein_col, year_col, value_col, prefix):
    """Pivot ``(ein, year, value)`` long → wide ``{prefix}_{YYYY}`` + ``total_{prefix}``.

    Uses ``aggfunc='sum'`` so multiple rows per ``(ein, taxyear)`` collapse
    into one cell rather than dropping all but one. Grant summaries already
    arrive at one row per (ein, year) and pass through unchanged.
    """
    if long_df.empty:
        return pd.DataFrame(columns=[ein_col, f"total_{prefix}"])
    wide = long_df.pivot_table(
        index=ein_col,
        columns=year_col,
        values=value_col,
        aggfunc="sum",
    ).fillna(0)
    wide.columns = [f"{prefix}_{int(y)}" for y in wide.columns]
    wide[f"total_{prefix}"] = wide.sum(axis=1)
    return wide.reset_index()


def reformat_ein(filerein):
    if len(filerein) == 10:
        if "-" not in filerein:
            raise Exception("Expecting 10 length EIN to have a `-`")
        return filerein
    filerein = str(filerein)
    if len(filerein) == 8:
        reformatted_ein = "0" + filerein
    else:
        reformatted_ein = filerein
    return reformatted_ein[:2] + "-" + reformatted_ein[2:]


def _format_address_parts(
    addr_line_1=None,
    addr_line_2:str =None,
    city:str =None,
    state:str =None,
    zip:str =None,
):
    """
    Format the address parts into a single string.
    """
    address = ""
    if addr_line_1:
        address += f"{addr_line_1} "

    if addr_line_2:
        address += f"{addr_line_2} "

    if city:
        address += f"{city} "

    if state:
        address += f"{state} "

    if zip:
        address += f"{zip} "

    return address.strip()



def _assemble_cb_shape(hits, basic_long, grants_long, column_for_funding):
    """Merge the per-EIN identity row + pivoted year columns + synthetic fields."""
    logger.info("Assembling CB-shaped output")

    # Identity frame: one row per EIN, taken from its most recent filing
    # year. The identity columns (Organization, Website_cb_cd, hq_address,
    # total_revenue_current_year, total_cash_contributions,
    # total_cash_contributions_no_gov) come straight off this row.
    base = (
        basic_long.sort_values(["ein", "taxyear"], ascending=[True, False])
        .drop_duplicates(subset=["ein"], keep="first")
        .copy()
    )

    contrib_wide = _pivot_year_col(
        basic_long, "ein", "taxyear", column_for_funding, column_for_funding
    )

    if grants_long.empty:
        grants_wide = pd.DataFrame(columns=["ein"])
    else:
        grants_wide = _pivot_year_col(
            grants_long, "ein", "taxyear", "total_grant_amount", "grant"
        ).rename(columns={"total_grant": "total_grants_amount"})
        max_yr = (
            grants_long.groupby("ein")["taxyear"]
            .max()
            .rename("last_grant_year")
            .reset_index()
        )
        grants_wide = grants_wide.merge(max_yr, on="ein", how="left")
        # Union funder sets across years (each year already arrives deduped).
        funders = (
            grants_long.groupby("ein")
            .agg({
                "granter_eins": lambda s: sorted({e for arr in s for e in (arr or [])}),
                "granter_names": lambda s: sorted({n for arr in s for n in (arr or [])}),
            })
            .reset_index()
            .rename(columns={"granter_eins": "Funders", "granter_names": "Funder_Names"})
        )
        grants_wide = grants_wide.merge(funders, on="ein", how="left")

    df = base.merge(contrib_wide, on="ein", how="left")
    df = df.merge(grants_wide, on="ein", how="left")

    # Description is the FTS hit text (not anything on basic_fields).
    description_by_ein = {h.ein: (h.unique_text or "") for h in hits}
    df["Description"] = df["ein"].map(description_by_ein)

    df["hq_address"] = df.apply(
        lambda x: _format_address_parts(x['addr_line_1'], x['addr_line_2'], x['city'], x['state'], x['zip']),
        axis=1,
    )
    df["Last_Funding_Type"] = "Grant"
    df["Funding Stage"] = "Philanthropy"
    df["Philanthropy_vs_Venture"] = "Philanthropy"
    df["Data Source"] = "Giving Tuesday"
    df["Org Type"] = "Non Profit"

    df.rename(columns={"name": "Organization", "website": "Website_cb_cd"}, inplace=True)
    df["Website_cb_cd"] = df["Website_cb_cd"].apply(
        lambda x: x.lower() if isinstance(x, str) else None
    )

    df["ein"] = df["ein"].apply(reformat_ein)
    df["id"] = df["ein"].apply(lambda x: f"givingtuesday_{x}")

    df.drop(
        columns=[
            "name_secondary",
            "addr_line_1",
            "addr_line_2",
            "city",
            "state",
            "zip",
            "taxyear",
        ],
        inplace=True,
    )

    # Mirror each ``{column_for_funding}_YYYY`` to a ``Funding_YYYY``
    # column — the CB-shaped consumer reads off the ``Funding_*`` family.
    # ``Total_Funding_$`` is the row sum across those columns;
    # ``Last_Funding_Year`` is the max year present (same scalar broadcast
    # to every row).
    funding_year_cols = [
        c
        for c in df.columns
        if c.startswith(f"{column_for_funding}_") and c[-4:].isnumeric()
    ]
    funding_yyyy_cols = []
    for col in funding_year_cols:
        year = int(col[-4:])
        new_col = f"Funding_{year}"
        df[new_col] = df[col]
        funding_yyyy_cols.append(new_col)
    if funding_yyyy_cols:
        df["Total_Funding_$"] = df[funding_yyyy_cols].sum(axis=1)
        df["Last_Funding_Year"] = max(
            int(c.rsplit("_", 1)[-1]) for c in funding_yyyy_cols
        )

    return df


def query_process_givingtuesday_data(
    search_terms_list=None,
    search_terms_path=None,
    search_mode="exact",
    proceessed_output_path=None,
    filter_yr=2017,
    min_avg_contributions=300_000,
    min_avg_grants=0,
    min_num_grants=1,
    column_for_funding="total_cash_contributions",
    client=None,
    return_full_text=True,
    force_include_eins=None,
    remove_granter_eins=True,
):
    """Return the Crunchbase-shaped DataFrame of grantee orgs matching ``search_terms``.

    Pipeline:

    1. Postgres FTS over ``nonprofit_text`` for the supplied keywords,
       intersected server-side with the eligibility filters below.
    2. Bulk fetch of ``basic_fields`` and aggregated grant summaries for
       the eligible EIN set.
    3. Pivot per-year facts to wide columns and assemble the CB-shaped
       output frame.

    Arguments:

    * ``search_terms_list`` / ``search_terms_path`` — supply exactly one.
      ``search_terms_path`` is a CSV with a single ``term`` column.
    * ``search_mode`` — FTS matching strategy:
        - ``"exact"`` (default): no stemming, multi-word inputs match as
          a literal phrase. ``"needs based"`` only matches the contiguous
          phrase. Best for precise term lookups.
        - ``"stemmed"``: snowball stems and stopword-strips, tokens
          AND-ed anywhere in the doc. ``"tutoring"`` matches ``tutor``,
          ``tutored``, ``tutors``. Best for relevance-ranked discovery.
    * ``filter_yr`` — lower bound (inclusive) on taxyear for both the
      eligibility aggregations *and* the per-year facts pulled into the
      output. Years before this are ignored.
    * ``min_avg_contributions`` — keep only EINs whose mean yearly
      ``totacashcont`` since ``filter_yr`` is at least this. Computed
      as sum per (ein, taxyear) first, then mean across years; rows
      with NULL/blank ``totacashcont`` are skipped (don't count as $0).
    * ``min_avg_grants`` — same per-year-sum-then-mean shape over
      grants received from ``unioned_grants``. NULL grant amounts
      coalesce to $0 within their year so the EIN isn't silently dropped.
    * ``min_num_grants`` — total grant-row count across the
      ``filter_yr`` window must be at least this many. ``1`` filters
      to "received at least one grant".
    * Pass ``None`` to disable any of the three eligibility thresholds.
    * ``column_for_funding`` — which ``basic_fields`` column gets
      mirrored into the ``Funding_YYYY`` family on the output.
      Defaults to ``total_cash_contributions``.
    * ``return_full_text`` — when ``True``, populates the ``Description``
      column from the org's deduped 990 text; ``False`` leaves it blank.
    * ``proceessed_output_path`` — optional URI to write the result
      parquet to (S3 supported). Returns the DataFrame either way.
    * ``client`` — supply a pre-built ``GtDatamartClient`` to override
      the default (built from vdl-tools postgres config).
    * ``force_include_eins`` — optional list of EINs to inject into the
      result regardless of whether they match the keyword search or pass
      the eligibility thresholds. Looked up via
      ``client.get_nonprofits_by_ein`` and unioned with the FTS hits
      (search-first dedup, so an EIN matched by both keeps its FTS rank).
      When supplied, ``search_terms_list`` / ``search_terms_path`` become
      optional — passing only ``force_include_eins`` runs in forced-only
      mode. Forced EINs that have no ``basic_fields`` row in the DB are
      logged at WARNING and dropped (there's no identity data to assemble).
    * ``remove_granter_eins`` — when ``True``, remove the EINs that are
      granters from the result.

    Output columns (one row per eligible EIN):

    * Identity: ``ein`` (``NN-NNNNNNN``), ``id`` (``givingtuesday_<ein>``),
      ``Organization``, ``Website_cb_cd``, ``Description``, ``hq_address``.
    * Latest-filing scalars: ``total_revenue_current_year``,
      ``total_cash_contributions``, ``total_cash_contributions_no_gov``.
    * Contributions per year: ``total_cash_contributions_YYYY`` (one
      column per filing year present), ``total_total_cash_contributions``.
    * Grants per year: ``grant_YYYY``, ``total_grants_amount``,
      ``last_grant_year``, ``Funders``, ``Funder_Names``.
    * CB-shaped funding aliases: ``Funding_YYYY`` (mirror of the
      ``column_for_funding`` per-year columns), ``Total_Funding_$``,
      ``Last_Funding_Year``.
    * Static labels: ``Last_Funding_Type='Grant'``,
      ``Funding Stage='Philanthropy'``, ``Philanthropy_vs_Venture='Philanthropy'``,
      ``Data Source='Giving Tuesday'``, ``Org Type='Non Profit'``.
    """
    client = client or _make_default_client()
    keywords = _load_keywords(
        search_terms_list,
        search_terms_path,
        allow_empty=bool(force_include_eins),
    )

    hits_search = (
        client.search_nonprofits(
            keywords,
            search_mode=search_mode,
            return_text=return_full_text,
            min_avg_contributions=min_avg_contributions,
            min_avg_grants=min_avg_grants,
            min_num_grants=min_num_grants,
            min_taxyear=filter_yr,
        )
        if keywords
        else []
    )
    hits_forced = (
        client.get_nonprofits_by_ein(
            force_include_eins, return_text=return_full_text
        )
        if force_include_eins
        else []
    )

    # Dedup search-first so an EIN matched by FTS keeps its rank/text;
    # forced-only EINs fall through unchanged.
    seen = set()
    hits = []
    for h in hits_search + hits_forced:
        if h.ein in seen:
            continue
        seen.add(h.ein)
        hits.append(h)

    eins = [h.ein for h in hits]
    logger.info(
        "Eligible EIN set: %d (search=%d, forced=%d)",
        len(eins),
        len(hits_search),
        len(hits_forced),
    )
    if not eins:
        return pd.DataFrame()

    basic_long = _records_to_df(client.get_basic_fields(eins, min_taxyear=filter_yr))
    grant_summaries = client.get_grant_summaries(eins, role="grantee", min_taxyear=filter_yr)
    all_granter_eins = set(granter_ein for grant_summary in grant_summaries for granter_ein in (grant_summary.granter_eins or []))

    if remove_granter_eins:
        # Remove from hits the EINs thare in all_granter_eins
        clean_hit_eins = seen.difference(all_granter_eins)
    else:
        clean_hit_eins = seen
    basic_long = basic_long[basic_long["ein"].isin(clean_hit_eins)]
    clean_grants = [g for g in grant_summaries if g.ein in clean_hit_eins]
    grants_long = _records_to_df(clean_grants)

    if force_include_eins:
        found = set(basic_long["ein"]) if not basic_long.empty else set()
        missing = [e for e in force_include_eins if e not in found]
        if missing:
            logger.warning(
                "force_include_eins: %d EIN(s) had no basic_fields row and were dropped: %s",
                len(missing),
                missing,
            )

    df = _assemble_cb_shape(hits, basic_long, grants_long, column_for_funding)

    if proceessed_output_path:
        write_dataframe(df, proceessed_output_path)
    return df


if __name__ == "__main__":
    query_process_givingtuesday_data(
        search_terms_list=["teachers", "education"],
        proceessed_output_path="s3://ed-tracker-data/givingtuesday/giving_tuesday_data_cleaned.json",
        filter_yr=2023,
        column_for_funding="total_cash_contributions",
    )
