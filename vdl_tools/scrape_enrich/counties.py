"""Resolve geocoded addresses to US county FIPS codes.

Google gives a county NAME ('Santa Clara County'); most things you would then
want to do with it -- join census data, join county boundaries, count
organizations per county -- need the 5-digit FIPS code. Turning one into the
other is not a lookup, because county names are not unique:

- the legal suffix varies by state (County, Parish, Borough, Census Area,
  City and Borough, Municipality, Municipio) and is absent from most reference
  data;
- six (state, bare name) pairs exist as BOTH a county and an independent city
  -- Virginia's Richmond / Franklin / Fairfax / Roanoke, plus St. Louis MO and
  Baltimore MD -- so dropping the suffix makes them ambiguous;
- Google omits ``administrative_area_level_2`` entirely where the city IS the
  county equivalent: Washington DC, Baltimore city, St. Louis city, Virginia's
  independent cities, the NYC boroughs. That is ~5% of US addresses and it is
  concentrated in dense metros, so it cannot be waved off as an edge case.

This module owns those rules once, so every product that maps organizations to
counties gets the same answers instead of reimplementing the edge cases. It
sits next to the geocoding because it consumes exactly what that produces.

Resolution runs in two passes:

1. ``administrative_area_level_2`` against the FIPS crosswalk, after stripping
   the legal suffix. Google returns the FULL name here, so 'Baltimore County'
   and 'Baltimore city' stay distinguishable.
2. ``locality`` where Google omitted level_2 -- the county-equivalent case
   above. On this path an ambiguous name resolves to the INDEPENDENT CITY,
   because a locality is a city.

Nothing is guessed: an address that resolves to neither gets None and is
counted in ``report_county_coverage``.

KNOWN GAP -- Connecticut. Connecticut replaced its counties with nine Planning
Regions as county equivalents in 2022 and Google now returns those ('Western
Connecticut Planning Region'). The reference crosswalk still carries the eight
legacy counties, so Connecticut addresses resolve to None rather than being
mapped onto a legacy county they only partly overlap. Fixing it needs a
crosswalk carrying the planning regions.

The crosswalk (``reference/county_fips.csv``: fips, name, state) currently
carries bare names, matching the us-atlas county topology the climate dashboard
renders, so that display names built from it line up with the map's polygons.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from vdl_tools.shared_tools.tools.logger import logger


CROSSWALK_PATH = Path(__file__).parent / "reference" / "county_fips.csv"

# Legal suffixes Google appends that the crosswalk's bare names omit.
COUNTY_SUFFIXES = (
    " County",
    " Parish",
    " Borough",
    " Census Area",
    " City and Borough",
    " Municipality",
    " Municipio",
    " city",
    " City",
)

# Locality spellings that differ from the county-equivalent's crosswalk name.
# Keyed (state, locality) so a name is never rewritten outside its state.
LOCALITY_ALIASES = {
    ("District of Columbia", "Washington"): "District of Columbia",
    ("New York", "Brooklyn"): "Kings",
    ("New York", "Staten Island"): "Richmond",
    ("New York", "Manhattan"): "New York",
}

# (state, bare name) pairs that exist as both a county and an independent city,
# mapped to the CITY's FIPS. The locality pass resolves these to the city; the
# level_2 pass never needs them because Google spells the suffix out.
INDEPENDENT_CITY_FIPS = {
    ("Maryland", "Baltimore"): "24510",
    ("Missouri", "St. Louis"): "29510",
    ("Virginia", "Fairfax"): "51600",
    ("Virginia", "Franklin"): "51620",
    ("Virginia", "Richmond"): "51760",
    ("Virginia", "Roanoke"): "51770",
}

# The one county coextensive with its state — callers rendering a display name
# usually want just "District of Columbia" for it.
DC_FIPS = "11001"


def _strip_suffix(name: str) -> str:
    """Drop the legal suffix from a county name, if present.

    Parameters
    ----------
    name : str
        e.g. 'Santa Clara County', 'Tangipahoa Parish', 'Baltimore city'.

    Returns
    -------
    str
        The bare name ('Santa Clara', 'Tangipahoa', 'Baltimore').
    """
    for suffix in COUNTY_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)].strip()
    return name.strip()


def load_county_reference(path: Path = CROSSWALK_PATH) -> pd.DataFrame:
    """Load the raw county reference table.

    Parameters
    ----------
    path : pathlib.Path
        The crosswalk CSV.

    Returns
    -------
    pandas.DataFrame
        Columns ``fips``, ``name`` (bare, no legal suffix), ``state``.
    """
    return pd.read_csv(path, dtype=str)


def load_crosswalk(path: Path = CROSSWALK_PATH) -> dict:
    """Load the (state, bare name) -> FIPS lookup.

    Ambiguous pairs (county vs independent city) are omitted from the direct
    map and keyed with a third element instead, so a lookup can never silently
    pick the wrong one of the pair.

    Parameters
    ----------
    path : pathlib.Path
        The crosswalk CSV.

    Returns
    -------
    dict
        ``{(state, bare_name): fips}`` for unambiguous pairs, plus
        ``{(state, bare_name, is_city): fips}`` for ambiguous ones.
    """
    df = load_county_reference(path)
    counts = df.groupby(["state", "name"]).size()
    ambiguous = set(counts[counts > 1].index)

    lookup = {}
    for state, name, fips in zip(df["state"], df["name"], df["fips"]):
        key = (state, name)
        if key in ambiguous:
            is_city = INDEPENDENT_CITY_FIPS.get(key) == fips
            lookup[(state, name, is_city)] = fips
        else:
            lookup[key] = fips
    logger.info(
        "County crosswalk: %d counties, %d ambiguous (state, name) pairs",
        len(df), len(ambiguous),
    )
    return lookup


def _component(raw, wanted: str):
    """Pull one address component out of a stored Google response.

    Parameters
    ----------
    raw : dict or str
        ``geocode.response_full``.
    wanted : str
        Component type, e.g. 'administrative_area_level_2'.

    Returns
    -------
    str or None
        The component's long_name.
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    for component in (raw or {}).get("address_components", []):
        if wanted in component.get("types", []):
            return component.get("long_name")
    return None


def fetch_geocode_components(
    session, addresses: list[str], address_column: str = "Location"
) -> pd.DataFrame:
    """Read county / state / locality components for the given addresses.

    Reads the ``geocode`` cache directly because resolution needs the raw
    ``locality`` for the county-equivalent fallback, which the enrichment
    pipeline's composite ``City`` column does not preserve.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        Open session on the vdl database.
    addresses : list of str
        Distinct address strings to look up.
    address_column : str
        Name to give the address column in the result, so callers can merge on
        whatever they call it.

    Returns
    -------
    pandas.DataFrame
        Columns ``<address_column>``, ``county_raw``, ``geo_state``,
        ``locality``, ``geo_country``.
    """
    rows = session.execute(
        text("SELECT address, response_full FROM geocode WHERE address = ANY(:addrs)"),
        {"addrs": addresses},
    ).fetchall()
    logger.info(
        "Geocode cache: %d of %d distinct addresses found",
        len(rows), len(addresses),
    )
    columns = [address_column, "county_raw", "geo_state", "locality", "geo_country"]
    return pd.DataFrame([
        {
            address_column: address,
            "county_raw": _component(raw, "administrative_area_level_2"),
            "geo_state": _component(raw, "administrative_area_level_1"),
            "locality": _component(raw, "locality"),
            "geo_country": _component(raw, "country"),
        }
        for address, raw in rows
    ], columns=columns)


def resolve_county_fips(components: pd.DataFrame) -> pd.DataFrame:
    """Resolve geocode components to county FIPS codes.

    Parameters
    ----------
    components : pandas.DataFrame
        Output of ``fetch_geocode_components``.

    Returns
    -------
    pandas.DataFrame
        The input plus ``county_fips`` (5-digit str or None) and
        ``county_source`` ('admin_area_2', 'locality', or None) recording which
        pass resolved it.
    """
    lookup = load_crosswalk()

    out = components.copy()
    fips_col, source_col = [], []
    for county_raw, state, locality, country in zip(
        out["county_raw"], out["geo_state"], out["locality"], out["geo_country"]
    ):
        if country != "United States" or not isinstance(state, str):
            fips_col.append(None)
            source_col.append(None)
            continue

        fips = None
        source = None
        if isinstance(county_raw, str) and county_raw.strip():
            bare = _strip_suffix(county_raw)
            is_city = county_raw.rstrip().endswith(("city", "City"))
            fips = lookup.get((state, bare)) or lookup.get((state, bare, is_city))
            source = "admin_area_2" if fips else None
        if fips is None and isinstance(locality, str) and locality.strip():
            # County-equivalent fallback: the locality IS the county here, so
            # an ambiguous name resolves to the independent city.
            name = LOCALITY_ALIASES.get((state, locality.strip()), locality.strip())
            bare = _strip_suffix(name)
            fips = (
                INDEPENDENT_CITY_FIPS.get((state, bare))
                or lookup.get((state, bare))
                or lookup.get((state, bare, True))
            )
            source = "locality" if fips else None

        fips_col.append(fips)
        source_col.append(source)

    out["county_fips"] = fips_col
    out["county_source"] = source_col
    return out


def report_county_coverage(resolved: pd.DataFrame) -> dict:
    """Log how many rows resolved to a county, and why others didn't.

    Parameters
    ----------
    resolved : pandas.DataFrame
        Output of ``resolve_county_fips``, optionally merged back onto a
        per-entity frame (one row per organization, say).

    Returns
    -------
    dict
        Coverage counts, including the unresolved breakdown by state.
    """
    us = resolved[resolved["geo_country"] == "United States"]
    got_county = us["county_fips"].notna()
    stats = {
        "rows_total": len(resolved),
        "rows_us": len(us),
        "rows_with_county": int(got_county.sum()),
        "distinct_counties": int(us["county_fips"].nunique()),
        "via_admin_area_2": int((us["county_source"] == "admin_area_2").sum()),
        "via_locality": int((us["county_source"] == "locality").sum()),
        "rows_us_without_county": int((~got_county).sum()),
    }
    logger.info(
        "County coverage: %(rows_with_county)d of %(rows_us)d US rows "
        "(%(via_admin_area_2)d via admin_area_2, %(via_locality)d via "
        "locality) across %(distinct_counties)d counties; "
        "%(rows_us_without_county)d US rows unresolved",
        stats,
    )
    if stats["rows_us_without_county"]:
        by_state = us.loc[~got_county, "geo_state"].value_counts().head(5).to_dict()
        logger.info("Unresolved US rows by state (top 5): %s", by_state)
        stats["unresolved_by_state"] = by_state
    return stats
