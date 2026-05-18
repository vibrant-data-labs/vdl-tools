import pandas as pd

from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool
from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake


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
    investor_type: str = 'Government',
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:
    investor_type_lower = investor_type.lower()
    investor_df[f'is_{investor_type_lower}_investor_calced{keep_suffix}'] = investor_df.apply(
        lambda x: (coerced_bool(x['primaryType']) and x['primaryType'] == investor_type) or
            (coerced_bool(x['secondaryTypes']) and investor_type in x['secondaryTypes']),
        axis=1,
    )
    return investor_df


def add_investor_boolean_flags(
    investor_df: pd.DataFrame,
    column_name: str,
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:
    investor_df = investor_df.copy()
    investor_df[f'is_{column_name.lower()}_investor_calced{keep_suffix}'] = investor_df[column_name].astype(bool)
    return investor_df


INVESTOR_TYPES_TO_ADD = [
    'Government',
    'Private Equity',
    'Venture Capital',
    'Bank',
    'Commercial Banks',  # to remove later (combined with Bank)
    'Investment Bank',  # to remove later (combined with Bank)
    "Lender/Debt Provider",  # update later to "Non-Bank Lender / Debt Provider"
    'Non-Profit Organisation',  # to remove later (combined with Foundation)
    'Foundation',
    'Corporation',
    'Infrastructure',
    'Real Estate',
]

INVESTOR_BOOLEAN_FLAGS_TO_ADD = [
    'strategic',
    'growthInvestor',
]


def process_nzi_investors(
    investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    investor_types_to_add: list[str] = INVESTOR_TYPES_TO_ADD,
) -> pd.DataFrame:
    investor_df = investor_df.copy()
    for investor_type in investor_types_to_add:
        investor_df = add_investor_type_flag(investor_df, investor_type=investor_type, keep_suffix=keep_suffix)
    for boolean_flag in INVESTOR_BOOLEAN_FLAGS_TO_ADD:
        investor_df = add_investor_boolean_flags(investor_df, column_name=boolean_flag, keep_suffix=keep_suffix)
    investor_df = filter_format_columns(investor_df, keep_suffix=keep_suffix)
    return investor_df