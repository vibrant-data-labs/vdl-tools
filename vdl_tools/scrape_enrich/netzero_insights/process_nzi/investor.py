import re
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool
from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake


# ---------------------------------------------------------------------------
# Mapped investor types (Excel-driven)
# ---------------------------------------------------------------------------
# The Excel maps each raw NZI `investor_type` (33 values in NZI's universe) to
# a canonical `mapped_investor_type`. We surface a curated subset of those
# mapped names as per-investor boolean columns. Each entry in the subset
# becomes one ``is_<snake_mapped>_investor_calced_nzi`` column on the
# processed-investor DataFrame.
#
# The cascade after this layer is auto-discovery — see
# `funding_round.add_investor_type_flag` and
# `split_early_late_funding_rounds.divided_funding_rows_and_flatten`. Add a
# new mapped type below and the per-round and per-bucket columns appear with
# no further code changes.

INVESTOR_TYPE_MAPPING_XLSX = (
    Path(__file__).resolve().parent.parent
    / "investor_type_mappings_definitions.xlsx"
)

# Curated subset of mapped types we emit columns for. The Excel itself
# defines ~21 distinct mapped values; surfacing all of them would inflate
# the schema with columns the current analyses don't use. Add a name here
# (verbatim from the Excel `mapped_investor_type` column) to surface it.
SUBSET_MAPPED_TYPES = [
    "Government",
    "Foundation / Non-Profit",
    "Bank",
    "Non-Bank Lender",
    "Infrastructure",
    "Real Estate",
    "Corporate",
    "Private Equity",
    "Venture Capital",
    "Angels",                 # added 2026-06 (xlsx bin renamed from High-Net-Worth Individual(s))
    "Accelerator / Incubator",  # added 2026-06
]


def _snake_case(name: str) -> str:
    """Lowercase + collapse any run of non-alphanumeric chars into a single
    underscore. Used to normalise mapped-type names into column-friendly
    stems (e.g. 'Foundation / Non-Profit' -> 'foundation_non_profit',
    'Non-Bank Lender' -> 'non_bank_lender').
    """
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def load_investor_type_mappings(
    path: Path = INVESTOR_TYPE_MAPPING_XLSX,
    subset: list[str] = SUBSET_MAPPED_TYPES,
) -> dict[str, set[str]]:
    """Read the Excel mapping file and return ``{mapped_type: set(source_types)}``
    restricted to ``subset``.

    The Excel has two key columns: ``investor_type`` (raw NZI string, the
    value that appears in `primaryType` or `secondaryTypes`) and
    ``mapped_investor_type`` (the canonical bin). We group the raw types
    by their mapped target so each entry in the returned dict is the set
    of raw-type strings that map to that bin.

    A KeyError-style guardrail: if any name in ``subset`` is missing from
    the Excel we raise loudly, since silent absence would yield all-False
    columns downstream.
    """
    df = pd.read_excel(path, sheet_name="Investor Type Definitions")
    df = df[df["mapped_investor_type"].isin(subset)]
    grouped = (
        df.groupby("mapped_investor_type")["investor_type"]
        .apply(set)
        .to_dict()
    )
    missing = [name for name in subset if name not in grouped]
    if missing:
        raise KeyError(
            f"Mapped investor type(s) {missing!r} are listed in "
            f"SUBSET_MAPPED_TYPES but absent from {path.name}. "
            f"Check the spelling in `mapped_investor_type`."
        )
    return grouped


ORIGINAL_INVESTOR_DETAILS_COLUMNS = [
    # "id",
    # "lp",
    "name",
    "city",
    "website",
    "isLP",
    # "note",
    # "size",
    # "phone",
    # # "domain",
    # "sizeID",
    # "country",
    # "logoURL",
    "acquirer",
    "continent",
    "strategic",
    "investorID",
    # "coInvestors",
    # "countryCode",
    "description",
    # "foundedDate",
    "investments",
    "linkedInURL",
    "primaryType",
    # "lastDealDate",
    # "lastDealType",
    "numberOfDeals",
    # "primaryTypeID",
    "buyoutInvestor",
    "equityInvestor",
    "growthInvestor",
    "secondaryTypes",
    "commercialBuyer",
    # "lastRoundAmount",
    "ventureInvestor",
    "acquisitionCount",
    "growthDealsCount",
    # "secondaryTypeIDs",
    "commercialPartner",
    "financialInvestor",
    "ventureDealsCount",
    "commercialBuyCount",
    # "lastRoundAmountUSD",
    "buyoutInvestmentCount",
    "equityInvestmentCount",
    # "numberOfDealsFiltered",
    "infrastructureInvestor",
    "infrastructureDealsCount",
    "commercialAgreementsCount",
    "commercialPartnershipCount",
    "infrastructureProjectsCount",
    # "email",
    # "twitterURL",
    # "facebookURL"
]


def filter_format_columns(
  investor_df,
  keep_suffix="_nzi",
):
    investor_df = investor_df.copy()
    keep_columns = [col for col in ORIGINAL_INVESTOR_DETAILS_COLUMNS]
    for col in investor_df.columns:
        if col.endswith(keep_suffix):
            keep_columns.append(col)

    rename_dict = {
        col: f"{camel_to_snake(col)}{keep_suffix}" for col in ORIGINAL_INVESTOR_DETAILS_COLUMNS
    }

    investor_df = investor_df[keep_columns]
    investor_df = investor_df.rename(columns=rename_dict)
    return investor_df


def add_investor_type_flag(
    investor_df: pd.DataFrame,
    mapped_type: str,
    source_types: set[str],
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:
    """Emit one `is_<snake_mapped>_investor_calced<suffix>` per investor.

    An investor matches the mapped target iff its `primaryType` is any of
    the raw `source_types`, OR any of its `secondaryTypes` is in
    `source_types`. The collapse-multiple-raw-types-into-one-target
    happens entirely here via ``source_types`` (e.g. for mapped_type=
    "Bank", source_types = {"Bank", "Commercial Banks", "Investment Bank"}).
    """
    col = f'is_{_snake_case(mapped_type)}_investor_calced{keep_suffix}'
    investor_df[col] = investor_df.apply(
        lambda x: (
            (coerced_bool(x['primaryType']) and x['primaryType'] in source_types)
            or (
                coerced_bool(x['secondaryTypes'])
                and bool(set(x['secondaryTypes']) & source_types)
            )
        ),
        axis=1,
    )
    return investor_df


def add_investor_boolean_flags(
    investor_df: pd.DataFrame,
    column_name: str,
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:
    """Coerce an NZI investor-record boolean field into a per-investor flag.

    Unlike `add_investor_type_flag`, this path doesn't go through the
    Excel mapping — it just lifts a pre-existing boolean column (e.g.
    `strategic`, `growthInvestor`) into the `is_*_investor_calced_nzi`
    naming scheme so the downstream cascade picks it up alongside the
    mapped types.
    """
    investor_df = investor_df.copy()
    # Strip a trailing "investor" from the flag name so we don't double it up
    # (e.g. "growthInvestor" → stem "growth" → is_growth_investor_calced_nzi,
    # not is_growthinvestor_investor_calced_nzi). "strategic" is unaffected.
    stem = column_name.lower()
    if stem.endswith("investor"):
        stem = stem[:-len("investor")].rstrip("_")
    investor_df[f'is_{stem}_investor_calced{keep_suffix}'] = investor_df[column_name].astype(bool)
    return investor_df


# Separate path from the Excel-driven mapping above: these are NZI investor-
# record boolean fields (not primary/secondary type values), so they aren't
# in the mapping spreadsheet. We surface them as `is_<flag>_investor_calced_nzi`
# columns alongside the mapped types.
INVESTOR_BOOLEAN_FLAGS_TO_ADD = [
    'strategic',
    'growthInvestor',
]


def process_nzi_investors(
    investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    investor_type_mapping_path: Path = INVESTOR_TYPE_MAPPING_XLSX,
) -> pd.DataFrame:
    """Add per-investor `is_*_investor_calced_nzi` columns and tidy schema.

    Two parallel paths produce these columns:

    1. **Mapped-type path** (driven by the Excel at
       ``investor_type_mapping_path``). For each entry in
       `SUBSET_MAPPED_TYPES`, looks up the raw NZI source types that map
       to it and emits a single boolean column per investor (True if any
       of those raw types appears on the investor's `primaryType` or
       `secondaryTypes`).

    2. **Boolean-flag path** (`INVESTOR_BOOLEAN_FLAGS_TO_ADD`). Coerces
       pre-existing NZI investor-record boolean fields (`strategic`,
       `growthInvestor`) into the same `is_*_investor_calced_nzi`
       naming scheme.

    Downstream stages (`funding_round.add_investor_type_flag` and
    `split_early_late_funding_rounds.divided_funding_rows_and_flatten`)
    discover both kinds of column by pattern-matching on
    ``is_*_investor_calced{keep_suffix}`` — they don't care which path
    produced them.
    """
    investor_df = investor_df.copy()
    mappings = load_investor_type_mappings(investor_type_mapping_path)
    for mapped_type, source_set in mappings.items():
        investor_df = add_investor_type_flag(
            investor_df,
            mapped_type=mapped_type,
            source_types=source_set,
            keep_suffix=keep_suffix,
        )
    for boolean_flag in INVESTOR_BOOLEAN_FLAGS_TO_ADD:
        investor_df = add_investor_boolean_flags(
            investor_df, column_name=boolean_flag, keep_suffix=keep_suffix,
        )
    investor_df = filter_format_columns(investor_df, keep_suffix=keep_suffix)
    return investor_df