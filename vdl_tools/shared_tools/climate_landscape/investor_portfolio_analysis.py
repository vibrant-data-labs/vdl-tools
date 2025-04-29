from itertools import combinations

import pandas as pd
import numpy as np

from vdl_tools.scrape_enrich.crunchbase.organizations_api_extended import (
    companies_id_query,
    people_query,
    funding_rounds_query_by_investor_id
)
from vdl_tools.shared_tools.cb_funding_calculations import ROUND_TO_STAGE
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.project_config import get_paths
from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool

PATHS = get_paths()


from joblib import Memory
location = './crunchbase_investors_cachedir'
memory = Memory(location, verbose=0)


AGGREGATION_DICT = {
    "count_total_rounds{metric_suffix}": ('funding_round_id', 'count'),
    "count_total_companies{metric_suffix}": ('company_id', 'nunique'),
    "median_money_raised{metric_suffix}": ('money_raised', 'median'),
}

class InvestorAnalysis:
    def __init__(
        self,
        company_df: pd.DataFrame,
        columns_to_count: list = None,
        investor_column_name: str = "Investors Data",
        original_alias: str = "climate",
        filter_out_other_investors: bool = True,
        aggregation_dict: dict = AGGREGATION_DICT,
    ):
        self.company_df = company_df
        self.investor_column_name = investor_column_name
        self.investor_funding_round_company_df = None
        self.raw_investor_funding_rounds = None
        self.columns_to_count = columns_to_count or []
        self.original_alias = original_alias
        self.investor_funding_round_company_merged_df = None
        self.filter_out_other_investors = filter_out_other_investors
        self.aggregation_dict = aggregation_dict
        self.people_investor_metadata = None
        self.company_investor_metadata = None
        self.investor_metadata = None


    def get_company_investors_metadata(self, investor_ids, use_cache=True):
        """
        Retrieves investor data from the Crunchbase API for the given investor IDs.

        Args:
            investor_ids (list): A list of investor IDs.

        Returns:
            pd.DataFrame: A DataFrame containing investor data.
        """
        if use_cache:
            investors_cached_query = memory.cache(companies_id_query)
        else:
            investors_cached_query = companies_id_query
        investor_metadata = investors_cached_query(investor_ids)
        investor_metadata.rename(
            columns={
                "uuid": "investor_id",
                "name": "investor_name",
            },
            inplace=True
        )
        investor_metadata.set_index("investor_id", inplace=True)
        investor_metadata['is_government_investor'] = (
            investor_metadata['investor_type']
            .apply(lambda x: True if coerced_bool(x) and 'government_office' in x else False)
        )

        self.company_investor_metadata = investor_metadata
        return self.company_investor_metadata

    def get_people_investors_metadata(self, investor_ids, use_cache=True):
        if use_cache:
            investors_cached_query = memory.cache(people_query)
        else:
            investors_cached_query = people_query

        investor_metadata = investors_cached_query(investor_ids)
        investor_metadata.rename(
            columns={
                "uuid": "investor_id",
                "name": "investor_name",
            },
            inplace=True
        )
        investor_metadata.set_index("investor_id", inplace=True)
        investor_metadata['is_government_investor'] = False
        self.people_investor_metadata = investor_metadata
        return self.people_investor_metadata

    def get_investors_metadata(self, investor_ids, use_cache=True):
        company_investor_metadata = self.get_company_investors_metadata(investor_ids, use_cache=use_cache)
        company_investor_metadata['investor_entity_type'] = 'organization'

        leftover_investor_ids = list(set(investor_ids).difference(set(company_investor_metadata.index)))

        people_investor_metadata = self.get_people_investors_metadata(leftover_investor_ids, use_cache=use_cache)
        people_investor_metadata['investor_entity_type'] = 'person'
        investor_metadata = pd.concat([
            company_investor_metadata[['investor_name', 'investor_entity_type', 'is_government_investor']],
            people_investor_metadata[['investor_name', 'investor_entity_type', 'is_government_investor']],
        ])
        return investor_metadata


    def filter_to_original_investors(self):
        # Get all the investors and the number of companies they invested in from
        # the original data (by the funding rounds)
        original_data_investment_counts = (
            self.investor_funding_round_company_df
            .groupby('investor_id')
            ['in_original_data']
            .sum()
        )
        # It could be that we've filtered out some companies from the original data later
        # so we should filter out investors that have no original investments after that filtering
        no_data_investors = (
            original_data_investment_counts
            [original_data_investment_counts < 1]
            .index
            .tolist()
        )
        self.investor_funding_round_company_df = (
            self.investor_funding_round_company_df[
                ~self.investor_funding_round_company_df['investor_id'].isin(no_data_investors)
            ]
        )
        return self.investor_funding_round_company_df


    def get_investors_porfolios(
        self,
    ):
        self.investor_funding_round_company_df = self._get_investors_porfolios(
            filter_out_other_investors=self.filter_out_other_investors
        )
        self.investor_funding_round_company_df = self.filter_to_original_investors()
        return self.investor_funding_round_company_df

    def _query_crunchbase_for_investor_portfolios(self, investor_ids, force_query=False):
        """
        Queries the Crunchbase API for funding rounds associated with the given investor IDs.

        Args:
            investor_ids (list): A list of investor IDs to query.

        Returns:
            pd.DataFrame: A DataFrame containing funding round data for the specified investors.
        """
        if self.raw_investor_funding_rounds is None or force_query:
            cached_compute = memory.cache(funding_rounds_query_by_investor_id)
            self.raw_investor_funding_rounds = cached_compute(sorted(investor_ids))
        return self.raw_investor_funding_rounds

    def _get_investors_porfolios(
        self,
        filter_out_other_investors: bool = True,
    ):
        """
        Retrieves the portfolios of investors from a given DataFrame of companies.

        Args:
            company_df (pd.DataFrame): A DataFrame containing company data, including investor information.
            filter_out_other_investors (bool, optional): If True, filters out investors not present in the original DataFrame. Defaults to True.

        Returns:
            pd.DataFrame: A DataFrame containing the portfolio companies for each investor, with columns for investor and company details.

        Raises:
            Exception: If any original companies with investors are missing from the resulting portfolio DataFrame.
        """
        # Filter to specific category
        investors_data_series = self.company_df[self.investor_column_name].tolist()

        # Get all unique investor ids
        investor_ids = list({
            investor_data['uuid'] for investors_list in investors_data_series
            for investor_data in investors_list
        })

        all_investor_funding_rounds = self._query_crunchbase_for_investor_portfolios(investor_ids)

        investor_funding_round_company_rows = []
        for _, company_funding_round in all_investor_funding_rounds.iterrows():

            company_id = company_funding_round['funded_organization_identifier']['uuid']
            company_name = company_funding_round['funded_organization_identifier']['value']
            company_permalink = company_funding_round['funded_organization_identifier'].get('permalink')

            if coerced_bool(company_funding_round['money_raised']):
                money_raised = company_funding_round['money_raised'].get('value_usd')
            else:
                money_raised = None
            investment_type = company_funding_round['investment_type']
            investment_stage = ROUND_TO_STAGE.get(investment_type, 'unknown')
            date_announced = company_funding_round['announced_on']
            for investor_data in company_funding_round['investor_identifiers']:
                if filter_out_other_investors and investor_data['uuid'] not in investor_ids:
                    continue
                if coerced_bool(company_funding_round['money_raised']):
                    money_raised = company_funding_round['money_raised'].get('value_usd')
                else:
                    money_raised = None
                investor_funding_round_company_rows.append({
                    'funding_round_id': company_funding_round['uuid'],
                    'funding_round_permalink': company_funding_round.get('permalink'),
                    'money_raised': money_raised,
                    'investor_id': investor_data['uuid'],
                    'investor_name': investor_data['value'],
                    'investor_permalink': investor_data.get('permalink'),
                    'investor_entity_type': investor_data.get('entity_def_id'),
                    'company_id': company_id,
                    'company_name': company_name,
                    'company_permalink': company_permalink,
                    'investment_type': investment_type,
                    'investment_stage': investment_stage,
                    "date_announced": date_announced,
                    "money_raised": money_raised,
                })

        self.investor_funding_round_company_df = pd.DataFrame(investor_funding_round_company_rows)
        self.investor_funding_round_company_df['date_announced_dt'] = pd.to_datetime(self.investor_funding_round_company_df['date_announced'])
        self.investor_funding_round_company_df['quarter_announced'] = (
            self.investor_funding_round_company_df['date_announced_dt']
            .dt
            .to_period('Q')
            .astype(str)
        )

        # Make sure all the original companies with investors came through in the total portfolios
        missing_original_companies = (
            set(self.company_df[self.company_df[self.investor_column_name].apply(lambda x: len(x) > 0)]['uuid'])
            .difference(set(self.investor_funding_round_company_df['company_id']))
        )
        if len(missing_original_companies) > 0:
            logger.warning(
                "Missing original companies: %s from the investor portfolios",
                missing_original_companies
            )

        self.investor_funding_round_company_df['in_original_data'] = (
            self.investor_funding_round_company_df['company_id']
            .isin(self.company_df['uuid'])
        )

        # Why are we seeing duplicates?
        self.investor_funding_round_company_df.drop_duplicates(
            subset=self.investor_funding_round_company_df.columns,
            inplace=True,
        )
        return self.investor_funding_round_company_df

    def add_original_data(self, columns_to_count=None):
        columns_to_count = self.columns_to_count if columns_to_count is None else columns_to_count
        if self.investor_funding_round_company_df is None:
            raise ValueError("Investor funding round company DataFrame is empty. Please run get_investors_porfolios() first.")
        
        self.investor_funding_round_company_merged_df = self.investor_funding_round_company_df.merge(
            self.company_df[['uuid', *columns_to_count]],
            left_on='company_id',
            right_on='uuid',
            how='left',
            suffixes=('', '_original')
        ).copy()
        return self.investor_funding_round_company_merged_df

    def _fill_in_aggregation_dict(self, metric_suffix=""):
        metric_suffix = f"_{metric_suffix}" if metric_suffix else ""
        agg_dict = {
            k.format(metric_suffix=metric_suffix): v
            for k, v in self.aggregation_dict.items()
        }
        return agg_dict

    def _aggregate_group(self, group, metric_suffix=""):
        agg_dict = self._fill_in_aggregation_dict(metric_suffix)
        return group.agg(**agg_dict)

    def aggregate_group(self, group, metric_suffix=""):
        return self._aggregate_group(group, metric_suffix=metric_suffix)

    def aggergate_multilevel(
        self,
        df,
        groupby_columns,
        metric_suffix=""
    ):
        grouped = df.groupby(groupby_columns)
        aggregated_group = self.aggregate_group(grouped, metric_suffix=metric_suffix)

        if aggregated_group.index.nlevels == 1:
            return aggregated_group

        unstacked = aggregated_group
        for _ in range(aggregated_group.index.nlevels - 1):
            unstacked = unstacked.unstack()

        # Renames the columns to look like
        # [
        #   (<metric1>, [(column_1_name, column_1_value1), (column_2_name, column_2_value1)])
        #   (<metric1>, [(column_1_name, column_1_value1), (column_2_name, column_2_value2)])
        unstacked.columns = [
            (x[0], tuple(zip(unstacked.columns.names[1:], x[1:])))
            for x  in unstacked.columns
        ]
        return unstacked

    def aggregate_df_by_level(
        self,
        original_only: bool = False,
        levels: tuple[str] = (),
    ):
        levels = [x for x in levels if x != 'investor_id']
        if self.investor_funding_round_company_df is None:
            raise ValueError("Investor funding round company DataFrame is empty. Please run get_investors_porfolios() first.")

        df = self.investor_funding_round_company_merged_df.copy()
        if original_only:
            df = df[df['in_original_data']]

        metric_suffix = self.original_alias if original_only else ""
        all_aggs = [
            self.aggregate_group(
                df.groupby('investor_id'),
                metric_suffix,
            )
        ]

        all_combinations = []
        for i in range(len(levels)):
            all_combinations.extend(combinations(levels, i + 1))
        all_combinations = [
            ["investor_id"] + list(combination)
            for combination in all_combinations
        ]
        for combination in all_combinations:
            all_aggs.append(self.aggergate_multilevel(df, combination, metric_suffix))
        return pd.concat(all_aggs, axis=1)

    def aggregate_investor_portfolios(
        self,
        levels: tuple[str] = (),
        columns_to_count: list = None,
    ):
        if self.investor_funding_round_company_df is None:
            logger.warning("Investor funding round company DataFrame is empty. Running get_investors_porfolios() first.")
            self.get_investors_porfolios()
        
        self.add_original_data(columns_to_count=columns_to_count)

        columns_to_count = self.columns_to_count if columns_to_count is None else columns_to_count
        total_df = self.aggregate_df_by_level(levels=levels)
        original_levels = list(levels) + columns_to_count
        original_only_df = self.aggregate_df_by_level(original_only=True, levels=original_levels)

        return total_df, original_only_df

    def _weighting_calculation(self, denominator):
        return np.log(denominator)

    def weighted_fraction(self, numerator, denominator, weighted=False):
        ratio = numerator / denominator
        if weighted:
            weight = self._weighting_calculation(denominator)
        else:
            weight = 1
        return ratio * weight

    def create_normalized_metrics(
        self,
        levels: tuple[str] = (),
        columns_to_count: list = None,
        weighted: bool = True,
    ):
        total_df, original_only_df = self.aggregate_investor_portfolios(levels, columns_to_count)

        if weighted:
            calculated_metric_suffix = "_perc_weighted"
        else:
            calculated_metric_suffix = "_perc"

        normalized_metrics = {}
        for metric_column in self.aggregation_dict:
            total_metric_name = metric_column.format(metric_suffix='')
            original_metric_name = metric_column.format(metric_suffix=f"_{self.original_alias}")
            perc_name = metric_column.format(metric_suffix=calculated_metric_suffix)

            normalized_metric = self.weighted_fraction(
                original_only_df[original_metric_name],
                total_df[total_metric_name],
                weighted=weighted,
            )
            normalized_metric.name = perc_name
            normalized_metrics[perc_name] = normalized_metric

        # Now calculate the percentage of cuts based on totals of the original only counts
        original_only_metric_names = {
            metric_column.format(metric_suffix=f"_{self.original_alias}")
            for metric_column in self.aggregation_dict.keys()
        }
        original_only_metric_columns = [
            x for x
            in original_only_df.columns.tolist()
            if x not in original_only_metric_names
        ]

        for original_only_metric_column in original_only_metric_columns:
            metric_name = original_only_metric_column[0]
            metric_rename = f"{metric_name}{calculated_metric_suffix}_{self.original_alias}_only"
            # Match the original_only_metric_column but replace the count metric name with the perc version
            full_name = (metric_rename, *original_only_metric_column[1:])
            # Divide the subset by the total for all original only counts
            #i.e % of early stage climate / total climate
            normalized_metric = self.weighted_fraction(
                original_only_df[original_only_metric_column],
                original_only_df[metric_name],
                weighted=weighted,
            )
            normalized_metric.name = full_name
            normalized_metrics[full_name] = normalized_metric
        return normalized_metrics

    def create_summary_analysis(
        self,
        levels: tuple[str] = (),
        columns_to_count: list = None,
        weighted: bool = True,
    ):
        total_df, original_only_df = self.aggregate_investor_portfolios(
            levels=levels,
            columns_to_count=columns_to_count
        )
        normalized_metrics = self.create_normalized_metrics(
            levels=levels,
            columns_to_count=columns_to_count,
            weighted=weighted,
        )
        self.investor_metadata = self.get_investors_metadata(total_df.index.tolist())
        all_metrics_df = pd.concat(
            [
                self.investor_metadata,
                total_df,
                original_only_df,
                *normalized_metrics.values(),
            ],
            axis=1
        )
        return all_metrics_df