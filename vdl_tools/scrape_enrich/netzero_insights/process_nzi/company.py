from collections import defaultdict

import pandas as pd

from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake
from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool

BANNED_LINKEDIN_URLS = [
    'https://www.linkedin.com/company/pitchbook'
]


ORIGINAL_COMPANY_DETAILS_COLUMNS = [
    "logo",
    "name",
    "active",
    "website",
    #   "id",
      "trl",
    "city",
    #   "note",
    "size",
    #   "tags",
    #   "email",
    #   "phone",
    "stage",
    #   "trlID",
    "admin4",
    "domain",
    #   "sizeID",
    "address",
    "country",
    #   "stageID",

    "acquired",
    #   "admin4ID",
    "champion",
    "emerging",
    #   "georowID",
    "continent",
    #   "countryID",
    "directURL",
    "pitchLine",
    "description",
    #   "actorTypes",
    "newEntrant",
    #   "reviewDate",
    #   "roundCount",
    "twitterURL",
    #   "countryCode",
    "facebookURL",
    "foundedDate",
    #   "fundRaising",
    "linkedinURL",
    #   "previousTrl",
    #   "revenueEuro",
    "revenueYear",
    "eutopiaScore",
    #   "fundingRange",
    "fundingTypes",
    #   "lastReviewer",
    #   "piFrameworks",
    "fundingAmount",
    #   "fundingString",
    "lastRoundDate",
    "lastRoundType",
    "revenuesRange",
    #   "fundingRangeID",
    "numberOfGrants",
    #   "acquisitionDate",
    #   "commercialBuyer",
    #   "dealsReviewDate",
    "fundingRangeUSD",
    #   "lastEquityRound",
    #   "lastRoundAmount",
    #   "revenuesRangeID",
    #   "trlOneYearPrior",
    "fundingAmountUSD",
    #   "fundingStringUSD",
    #   "tbFinancialStage",
    #   "trlLastFiveYears",
    #   "commercialPartner",
    #   "dealsLastReviewer",
    #   "fundingRangeIDUSD",
    #   "trlFiveYearsPrior",
    #   "trlLastThreeYears",
    #   "commercialBuyCount",
    #   "lastRoundAmountUSD",
    #   "trlAcquisitionDate",
    #   "trlThreeYearsPrior",
    #   "employeesGrowthJSON",
    #   "lastEquityRoundType",
    #   "numberOfEquityRounds",
    #   "sustainabilityMetric",
    "currentEmployeesCount",
    #   "lastRoundAmountString",
    #   "totalEquityFundingEUR",
    #   "totalEquityFundingUSD",
    #   "sustainabilityMetricID",
    #   "linkedInRevenuesRangeID",
    #   "commercialAgreementCount",
    #   "lastRoundAmountStringUSD",
    #   "trlFiveYearsHorizonPrior",
    #   "sustainabilityMetricLabel",
    #   "trlThreeYearsHorizonPrior",
    #   "commercialPartnershipCount",
    #   "financialStageOneYearPrior",
    #   "totalNonDilutiveFundingEUR",
    "totalNonDilutiveFundingUSD",
    #   "financialStageLastFiveYears",
    #   "financialStageLastThreeYears",
    #   "financialStageThreeYearsPrior",
    #   "financialStageThreeYearsHorizonPrior",
    #   "numberOfDebtRounds",
    #   "financialStageFiveYearsPrior",
    #   "financialStageFiveYearsHorizonPrior",
    #   "alternativeNames",
    #   "lastCommercialDeal",
    #   "qoQEmployeesGrowth",
    #   "yoYEmployeesGrowth",
    #   "qoQCorrespondingQuarter",
    #   "yoYCorrespondingQuarter",
    #   "lastSeenDate",
    #   "legalNames",
    #   "lastInfrastructureProject",
    #   "infrastructureProjectsCount",
    "clientID",
]

DELETE_TAG_COLUMNS = [
    "d3_review_cycle_tag_nzi",
    "d3_review_status_tag_nzi",
    "custom_tag_nzi",
    "d3_submission_year_tag_nzi",
    "d3_reason_for_decline/withdraw_(tag)_tag_nzi",
    "precision_fermentation_outcome_tag_nzi",
    "production_platform_tag_nzi",
    "feedstock_tag_nzi",
    "sector_tag_nzi",
    "climate_kic_tag_nzi",
    "deployment_role_tag_nzi",
]


def parse_company_tags(
    tags_dicts_list,
    suffix='_tag_nzi',
    flatten_tags=True,
):
    parsed_tags = defaultdict(list)
    for tag_dict in tags_dicts_list:
        tag_type = tag_dict['tagType']['tagType']
        label = tag_dict['label']
        formatted_tag_type = tag_type.lower().replace(" ", "_")
        if suffix:
            if suffix.startswith("_"):
                formatted_tag_type = f"{formatted_tag_type}{suffix}"
            else:
                formatted_tag_type = f"{formatted_tag_type}_{suffix}"
        if formatted_tag_type not in DELETE_TAG_COLUMNS:
            parsed_tags[formatted_tag_type].append(label)
    if flatten_tags:
        all_tags = []
        for tag_type, tags in parsed_tags.items():
            all_tags.extend(tags)
        return all_tags
    return parsed_tags


def add_parsed_tags(companies_df: pd.DataFrame, tag_col='tags', tag_suffix='_tag_nzi') -> pd.DataFrame:
    company_tags_list = [
        {
            'clientID': client_id,
            **parse_company_tags(tags, suffix=tag_suffix, flatten_tags=False),
            'flat_tags_nzi': parse_company_tags(tags, suffix=tag_suffix, flatten_tags=True),
        }
        for client_id, tags in zip(companies_df['clientID'], companies_df[tag_col])
    ]
    tags_df = pd.DataFrame(company_tags_list)
    companies_df = companies_df.merge(tags_df, on='clientID', how='left', suffixes=('', '_parsed'))
    for col in companies_df.columns:
        if col.endswith(tag_suffix):
            companies_df[col] = companies_df[col].apply(lambda x: x if coerced_bool(x) else [])
    return companies_df


def filter_format_columns(
  companies_df,
  keep_suffix="_nzi",
):
    companies_df = companies_df.copy()
    keep_columns = [col for col in ORIGINAL_COMPANY_DETAILS_COLUMNS]
    for col in companies_df.columns:
        if col.endswith(keep_suffix):
            keep_columns.append(col)

    rename_dict = {
        col: f"{camel_to_snake(col)}{keep_suffix}" for col in ORIGINAL_COMPANY_DETAILS_COLUMNS
    }

    companies_df = companies_df[keep_columns]
    companies_df = companies_df.rename(columns=rename_dict)
    return companies_df


def add_investor_type_flag(
    companies_df: pd.DataFrame,
    processed_funding_round_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    investor_type: str = 'Government',
) -> pd.DataFrame:

    has_investor_type_flag_col_name = f'has_{investor_type.lower()}_investor_calced{keep_suffix}'
    has_investor_type_flag_df = processed_funding_round_df.groupby('client_id_nzi').agg({has_investor_type_flag_col_name: 'sum'}).reset_index()
    has_investor_type_flag_df[has_investor_type_flag_col_name] = has_investor_type_flag_df[has_investor_type_flag_col_name].astype(bool)

    companies_df = companies_df.merge(
        has_investor_type_flag_df,
        left_on='clientID',
        right_on='client_id_nzi',
        how='left'
    )
    companies_df.drop(columns=['client_id_nzi'], inplace=True)
    companies_df[has_investor_type_flag_col_name] = companies_df[has_investor_type_flag_col_name].fillna(False)

    return companies_df

def process_nzi_companies_details(
    companies_df: pd.DataFrame,
    processed_funding_round_df: pd.DataFrame,
    tag_col: str = 'tags',
    tag_suffix: str = '_tag_nzi',
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:
    companies_df = companies_df.copy()

    companies_df = add_parsed_tags(
        companies_df,
        tag_col=tag_col,
        tag_suffix=tag_suffix
    )
    companies_df = add_investor_type_flag(
        companies_df,
        processed_funding_round_df,
        keep_suffix=keep_suffix,
        investor_type='Government'
    )
    companies_df['trl_parsed_nzi'] = companies_df['trl'].apply(lambda x: x.get('label') if coerced_bool(x) else None)
    companies_df = filter_format_columns(companies_df, keep_suffix=keep_suffix)
    companies_df['linkedin_url_nzi_cleaned'] = companies_df['linkedin_url_nzi'].apply(
        lambda x: x if x not in BANNED_LINKEDIN_URLS else None
    )
    return companies_df