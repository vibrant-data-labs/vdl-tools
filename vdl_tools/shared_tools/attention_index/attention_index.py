import json

import pandas as pd

from vdl_tools.shared_tools.project_config import get_paths
from vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping import redistribute_funding_fracs
from vdl_tools.shared_tools.climate_landscape.add_taxonomy_mapping import (
    load_one_earth_taxonomy,
    remove_mapping_name_suffix_from_taxonomy_results,
)

from vdl_tools.shared_tools.tools.numeric_normalizations import (
    zero_max_normalization,
    max_min_normalization,
    geometric_mean_values,
)

from vdl_tools.shared_tools.climate_landscape import funding_mapping_combination_utils as fmcu

from vdl_tools.shared_tools.tools.logger import logger


class AttentionIndexer:
    meta_df_prefix = "meta"
    funding_df_prefix = "funding"
    tax_map_prefix = "tax_map"

    def __init__(
        self,
        # taxonomy_path,
        # taxonomy_mapping_results_path,
        # meta_df_path,
        # funding_round_path,
        # candid_funding_path,
        taxonomy,
        taxonomy_mapping_results,
        taxonomy_mapping_id_col,
        meta_df,
        round_df,
        candid_funding_long,
        combined_funding_df,
        min_year=2018,
        max_year=2025,
        rounds_to_include=['pre_seed', 'seed', 'angel', 'series_a'],
        taxonomy_mapping_name_suffix="one_earth_category",
        distributed_funding_level=3,
        additional_rooting_factor=3.0,
    ):

        self._taxonomy = taxonomy
        self._taxonomy_mapping_results = taxonomy_mapping_results
        self._meta_df = meta_df
        self._round_df = round_df
        self._candid_funding_long = candid_funding_long
        self._combined_funding_df = combined_funding_df
        self._taxonomy_mapping_id_col = taxonomy_mapping_id_col

        self.taxonomy = None
        self.taxonomy_mapping_results = None
        self.meta_df = None
        self.round_df = None
        self.candid_funding_long = None
        self.combined_funding_df = None

        self.min_year = min_year
        self.max_year = max_year
        self.rounds_to_include = rounds_to_include
        self.taxonomy_mapping_name_suffix = taxonomy_mapping_name_suffix
        self.distributed_funding_level = distributed_funding_level
        self.additional_rooting_factor = additional_rooting_factor

        self.distributed_funding_df = None
        self.funding_mapped_to_taxonomy_df = None
        self.filtered_funding_mapped_to_taxonomy_df = None

        self._metric_column_names = None
        self._min_max_column_names = None
        self._scaled_column_names = None
        self._geometric_mean_column_names = None


    def initialize_files(self):

        # Start from the original files
        self.taxonomy = self._taxonomy.copy()
        self.taxonomy_mapping_results = self._taxonomy_mapping_results.copy()
        self.meta_df = self._meta_df.copy()
        self.round_df = self._round_df.copy()
        self.candid_funding_long = self._candid_funding_long.copy()
        self.combined_funding_df = self._combined_funding_df.copy()

        self.distributed_funding_df = self.redistribute_funding_fracs()
        self.rename_cols()

    def rename_cols(self):
        self.meta_df = fmcu.rename_df_cols(self.meta_df, self.meta_df_prefix)
        self.combined_funding_df = fmcu.rename_df_cols(
            self.combined_funding_df,
            self.funding_df_prefix
        )
        self.distributed_funding_df = fmcu.rename_df_cols(
            self.distributed_funding_df,
            self.tax_map_prefix
        )

    def _additional_redistribution_modifications(self, **kwargs):
        """
        Override this method in subclasses to apply additional modifications
        to the distributed funding fractions before returning.
        """
        pass

    def redistribute_funding_fracs(
        self,
        **kwargs,
    ):
        logger.info("Redistributing funding fractions")

        # This function is what adds the No_Level categories to the funding mapped to taxonomy df
        self.distributed_funding_df = redistribute_funding_fracs(
            df=self.taxonomy_mapping_results,
            taxonomy=self.taxonomy,
            id_attr=self._taxonomy_mapping_id_col,
            keepcols=[],
            max_level=self.distributed_funding_level,
        )
        if self._taxonomy_mapping_id_col != 'uid':
            self.distributed_funding_df['uid'] = self.distributed_funding_df[self._taxonomy_mapping_id_col]
        self.distributed_funding_df = self.distributed_funding_df[self.distributed_funding_df['FundingFrac'] > 0]

        self._additional_redistribution_modifications(**kwargs)
        return self.distributed_funding_df

    def _additional_mapping_modifications(self, **kwargs):
        """
        Override this method in subclasses to apply additional modifications
        to the funding mapped to taxonomy df before returning.
        """
        pass

    def map_taxonomy_to_funding_rounds(self, **kwargs):

        # Filter out the rounds that are not in the distributed funding df
        self.combined_funding_df = self.combined_funding_df[
            self.combined_funding_df[
                f'{self.funding_df_prefix}_uid']
                .isin(self.distributed_funding_df[f'{self.tax_map_prefix}_uid'])
        ]

        self.funding_mapped_to_taxonomy_df = self.combined_funding_df.merge(
            self.distributed_funding_df,
            left_on=f'{self.funding_df_prefix}_uid',
            right_on=f'{self.tax_map_prefix}_uid',
            suffixes=('_funding', '_taxonomy')
        )

        # Calculate the distributed funding
        self.funding_mapped_to_taxonomy_df['distributed_funding'] = (
            self.funding_mapped_to_taxonomy_df['tax_map_fundingfrac'] *
            self.funding_mapped_to_taxonomy_df['funding_funding']
        )

        # Check if the sum of the funding amounts in the combined funding df matches the sum of
        # the distributed funding amounts in the funding mapped to taxonomy df
        expected_total = self.combined_funding_df['funding_funding'].sum().round(0)
        actual_total = self.funding_mapped_to_taxonomy_df['distributed_funding'].sum().round(0)

        if abs(expected_total - actual_total) / expected_total > 0.01:
            raise ValueError(
                """
                The sum of the funding amounts in the combined funding df does not match
                the sum of the distributed funding amounts in the
                funding mapped to taxonomy df
                """
            )
        self._additional_mapping_modifications(**kwargs)
        return self.funding_mapped_to_taxonomy_df

    def _additional_filtering_mapped_rounds_modifications(self, **kwargs):
        """
        Override this method in subclasses to apply additional modifications
        to the filtered funding mapped to taxonomy df before returning.
        """
        pass

    def filter_mapped_rounds(self, **kwargs):
        logger.info("Filtering funding by year %s to %s", self.min_year, self.max_year)

        self.filtered_funding_mapped_to_taxonomy_df = self.funding_mapped_to_taxonomy_df[
            self.funding_mapped_to_taxonomy_df['funding_year'].between(self.min_year, self.max_year)
        ]

        if self.rounds_to_include:
            logger.info('Filtering round by rounds to include')
            self.filtered_funding_mapped_to_taxonomy_df = self.filtered_funding_mapped_to_taxonomy_df[
                self.filtered_funding_mapped_to_taxonomy_df['funding_investment_type'].isin(self.rounds_to_include)
            ]

        self._additional_filtering_mapped_rounds_modifications(**kwargs)
        return self.filtered_funding_mapped_to_taxonomy_df

    def aggregate_funding_to_orgs(
        self,
        level,
        **kwargs,
    ):
        """
        After filtering the rounds, we aggregate the funding to the orgs

        Every Organization / taxonomy level combination will have a row in the aggregation df
        with the distributed funding amounts for that organization / taxonomy level combination
        """

        taxonomy_level_columns = self._make_taxonomy_level_columns(level)

        self.org_level_aggregation_df = (
            self.filtered_funding_mapped_to_taxonomy_df.groupby(['tax_map_uid'] + taxonomy_level_columns)
            .agg({
                 # Sum up all the distributed_funding for all the lower levels 
                "distributed_funding": "sum",
                # Because we are at the round level this will have been multiplied for each round, so take the mean of it
                "tax_map_fundingfrac": "mean",
            })
        ).reset_index()

        self.org_level_aggregation_df.rename(columns={
            "distributed_funding": "distributed_funding",
            "tax_map_fundingfrac": "fractional_orgs",
        }, inplace=True)

        self.org_level_aggregation_df["whole_orgs"] = 1 ## Add a column for when we double count

        return self.org_level_aggregation_df

    def aggregate_to_level(self, level):
        """
        Aggregate the funding to the max level

        The returns the org level dataframe grouped by the max taxonomy level and calculates
        metrics for each level.

        At the end one row per category at the max level with the metrics for that level.

        It then calculates the min and max for each metric and adds them to the dataframe.
        """
        self._metric_column_names = [
            "distributed_funding",
            "fractional_orgs",
            # "whole_orgs"
        ]

        taxonomy_level_columns = self._make_taxonomy_level_columns(level)

        self.level_tax_aggregated = (
            self.org_level_aggregation_df
            .groupby(taxonomy_level_columns)
            [self._metric_column_names]
            .sum()
            .reset_index()
        )

        self.level_tax_aggregated['funding_per_company'] = (
            self.level_tax_aggregated['distributed_funding'] /
            self.level_tax_aggregated['fractional_orgs']
        )
        self._metric_column_names.append('funding_per_company')

        self.min_max_metrics = self.level_tax_aggregated.agg({
            metric: ["min", "max"]
            for metric in self._metric_column_names
        })

        self._min_max_column_names = []
        for _col in self._metric_column_names:
            for agg_name in ["min", "max"]:
                self.level_tax_aggregated[f"{_col}_{agg_name}"] = self.min_max_metrics[_col][agg_name]
                self._min_max_column_names.append(f"{_col}_{agg_name}")

        return self.level_tax_aggregated

    def _zero_max_scale_function(self, x, min_value, max_value):
        return max_min_normalization(x, max_value=max_value, min_value=0)

    def scale_metrics(
        self,
        scale_type="min_max",
    ):

        self._scaled_column_names = self._scaled_column_names or set()
        if scale_type == "min_max":
            scale_function = max_min_normalization
        elif scale_type == "zero_max":
            scale_function = self._zero_max_scale_function
        else:
            raise ValueError(f"Invalid scale type: {scale_type}")

        for _col in self._metric_column_names:
            scaled_column_name = f"{scale_type}_scale_{_col}"
            self._scaled_column_names.add(scaled_column_name)

            self.level_tax_aggregated[scaled_column_name] = (
                self.level_tax_aggregated.apply(
                    lambda x: scale_function(x[_col], min_value=x[f"{_col}_min"], max_value=x[f"{_col}_max"]),
                    axis=1
                )
            )
        return self.level_tax_aggregated

    def add_geometric_mean_to_scaled_metrics(
        self,
        scale_type="min_max",
        additional_rooting_factor=None,
    ):

        self._scaled_column_names = self._scaled_column_names or set()
        if scale_type == "min_max":
            scale_function = max_min_normalization
        elif scale_type == "zero_max":
            scale_function = self._zero_max_scale_function
        else:
            raise ValueError(f"Invalid scale type: {scale_type}")


        additional_rooting_factor = additional_rooting_factor or self.additional_rooting_factor
        columns_for_calc = [
            x for x in self._scaled_column_names if x.startswith(scale_type)
        ]

        geo_mean_column_name = f'{scale_type}_geometric_mean'
        self.level_tax_aggregated[geo_mean_column_name] = (
            self.level_tax_aggregated.apply(
                lambda x: geometric_mean_values(x[columns_for_calc]),
                axis=1
            ) ** (1.0 / additional_rooting_factor)
        )
        self._geometric_mean_column_names = self._geometric_mean_column_names or set()
        self._geometric_mean_column_names.add(geo_mean_column_name)

        min_geo_mean = self.level_tax_aggregated[geo_mean_column_name].min()
        max_geo_mean = self.level_tax_aggregated[geo_mean_column_name].max()

        rescaled_geo_mean_column_name = f'{scale_type}_scale_{geo_mean_column_name}'
        self._geometric_mean_column_names.add(rescaled_geo_mean_column_name)
        self.level_tax_aggregated[rescaled_geo_mean_column_name] = self.level_tax_aggregated.apply(
            lambda x: scale_function(
                x[geo_mean_column_name],
                min_value=min_geo_mean,
                max_value=max_geo_mean
            ),
            axis=1
        )

        return self.level_tax_aggregated

    def _make_taxonomy_level_columns(self, level):
        taxonomy_level_columns = []
        for i in range(0, level + 1):
            taxonomy_level_columns.append(f"tax_map_level{i}")
        return taxonomy_level_columns

    def calculate_attention_index_at_level(
        self,
        level,
    ):

        self.aggregate_funding_to_orgs(level)
        self.aggregate_to_level(level)
        self.scale_metrics(scale_type="min_max")
        self.scale_metrics(scale_type="zero_max")
        self.add_geometric_mean_to_scaled_metrics(scale_type="min_max")
        self.add_geometric_mean_to_scaled_metrics(scale_type="zero_max")

        taxonomy_level_columns = self._make_taxonomy_level_columns(level)

        return_cols = taxonomy_level_columns + list(self._geometric_mean_column_names)
        return self.level_tax_aggregated[return_cols]

    def calculate_attention_index(
        self,
        max_level
    ):
        self.initialize_files()

        self.map_taxonomy_to_funding_rounds()
        self.filter_mapped_rounds()

        attention_index_df = None
        join_cols = []
        for level in range(0, max_level + 1):
            logger.info(f"Calculating attention index at level {level}")
            attention_index_level_df = self.calculate_attention_index_at_level(level)

            if attention_index_df is None:
                attention_index_df = attention_index_level_df
            else:
                attention_index_df = attention_index_df.merge(
                    attention_index_level_df,
                    on=join_cols,
                    how='left',
                    suffixes=('', f'_level_{level}'),
                )
            join_cols.append(f'tax_map_level{level}')

        taxonomy_level_columns = self._make_taxonomy_level_columns(max_level)

        column_order = taxonomy_level_columns + [x for x in attention_index_df.columns if x not in taxonomy_level_columns]
        return attention_index_df[column_order]


def load_files(
    taxonomy_path,
    taxonomy_mapping_results_path,
    meta_df_path,
    funding_round_path,
    candid_funding_path,
    add_geo_engineering=False,  # Only works for the re-worked taxonomy for grantham
):
    taxonomy = load_one_earth_taxonomy(
        taxonomy_path,
        add_geo_engineering=add_geo_engineering
    )
    taxonomy_mapping_results = pd.read_json(taxonomy_mapping_results_path)
    taxonomy_mapping_results = remove_mapping_name_suffix_from_taxonomy_results(taxonomy_mapping_results, "one_earth_category")
    meta_df = pd.read_json(meta_df_path)
    round_df = fmcu.load_cb_round_data(funding_round_path)
    candid_funding_raw = pd.read_excel(candid_funding_path)
    candid_funding_long = fmcu.reshape_candid_funding(candid_funding_raw, id_col='id')
    combined_funding_df = fmcu.combine_funding_data(round_df, candid_funding_long)

    return taxonomy, taxonomy_mapping_results, meta_df, round_df, candid_funding_long, combined_funding_df
