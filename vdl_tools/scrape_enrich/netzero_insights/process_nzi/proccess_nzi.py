import pandas as pd
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.company_processing import process_nzi_companies_details
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.investor_processing import process_nzi_investors
# from vdl_tools.scrape_enrich.netzero_insights.process_nzi.funding_round_processing import process_nzi_companies_funding_rounds


def process_nzi_companies_funding_rounds(
    round_df: pd.DataFrame,
    investor_df: pd.DataFrame,
) -> pd.DataFrame:

    nzi_data = None

    return nzi_data


def process_nzi_companies_commercial_deals(nzi_data: pd.DataFrame) -> pd.DataFrame:
    return nzi_data