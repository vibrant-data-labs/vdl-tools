"""
Example subclasses of AttentionIndexer demonstrating different filtering strategies.
"""

import pandas as pd
from vdl_tools.shared_tools.attention_index.attention_index import AttentionIndexer


class NoLevelFilteringAttentionIndexer(AttentionIndexer):
    """
    Example subclass that replaces No_Level categories with more meaningful names.
    
    This demonstrates level-specific modifications where:
    - At level 1: Replace "No_Level_1_Energy Transition" with "Cross-Cutting Energy Transition"
    - At level 2+: Replace "No_Level_X_..." with the parent category if parent starts with "Cross-Cutting"
    """
    
    def _change_df_at_level(self, level, dataframe):
        """
        Apply level-specific modifications to replace No_Level categories.
        """
        if level >= 1:
            level_col = f"tax_map_level{level}"
            parent_level = level - 1
            parent_level_col = f"tax_map_level{parent_level}"
            
            # Check if the level column exists in the dataframe
            if level_col in dataframe.columns and parent_level_col in dataframe.columns:
                # Create a copy to avoid modifying the original
                modified_df = dataframe.copy()
                
                if parent_level == 0:
                    # If the parent level is 0 and the category value starts with `No_Level`, 
                    # replace it with `Cross-Cutting <parent_level_value>`
                    modified_df[level_col] = modified_df.apply(
                        lambda x: f"Cross-Cutting {x[parent_level_col]}" if
                                str(x[level_col]).startswith("No_Level")
                        else x[level_col],
                        axis=1
                    )
                else:
                    # If the level value is `No_Level` but its parent starts with `Cross-Cutting`, 
                    # take the parent value
                    modified_df[level_col] = modified_df.apply(
                        lambda x: x[parent_level_col] if
                                str(x[level_col]).startswith("No_Level")
                                and str(x[parent_level_col]).startswith("Cross-Cutting")
                            else x[level_col],
                        axis=1
                    )
                
                return modified_df
        
        return dataframe


class NoLevelRemovingAttentionIndexer(AttentionIndexer):
    """
    Example subclass that completely removes rows with No_Level categories at the current level.
    
    This demonstrates level-specific filtering where:
    - At level 1: Remove rows where level1 starts with "No_Level"
    - At level 2: Remove rows where level2 starts with "No_Level"
    - etc.
    """
    
    def _change_df_at_level(self, level, dataframe):
        """
        Remove rows with No_Level categories at the current level.
        """
        level_col = f"tax_map_level{level}"
        
        # Check if the level column exists in the dataframe
        if level_col in dataframe.columns:
            # Filter out rows where the current level starts with "No_Level"
            mask = ~dataframe[level_col].astype(str).str.startswith('No_Level')
            return dataframe[mask]
        
        return dataframe


class EnergyTransitionFilteringAttentionIndexer(AttentionIndexer):
    """
    Example subclass that filters out Energy Transition categories at level 1.
    """
    
    def _change_df_at_level(self, level, dataframe):
        """
        Filter out Energy Transition categories at level 1.
        """
        if level >= 1:
            # Filter out rows where level 1 is "Energy Transition"
            level1_col = "tax_map_level1"
            mask = dataframe[level1_col] != "Energy Transition"
            return dataframe[mask]
        
        return dataframe


class CustomFilteringAttentionIndexer(AttentionIndexer):
    """
    Example subclass that combines multiple filtering strategies.
    """
    
    def _additional_filtering(self):
        """
        Apply global filtering that affects all levels.
        """
        # Example: Remove rows with very low funding amounts
        self.filtered_funding_mapped_to_taxonomy_df = self.filtered_funding_mapped_to_taxonomy_df[
            self.filtered_funding_mapped_to_taxonomy_df['distributed_funding'] > 1000
        ]
    
    def _filter_at_level(self, level, dataframe):
        """
        Apply level-specific filtering.
        """
        # Filter out No_Level categories at the current level
        level_col = f"tax_map_level{level}"
        mask = ~dataframe[level_col].astype(str).str.startswith('No_Level')
        filtered_df = dataframe[mask]
        
        # Additional level-specific logic
        if level >= 2:
            # At level 2 and above, also filter out certain categories
            level2_col = "tax_map_level2"
            mask2 = ~filtered_df[level2_col].isin(['Unwanted Category 1', 'Unwanted Category 2'])
            filtered_df = filtered_df[mask2]
        
        return filtered_df


class SectorSpecificAttentionIndexer(AttentionIndexer):
    """
    Example subclass for sector-specific analysis with custom filtering.
    """
    
    def __init__(self, target_sectors=None, **kwargs):
        """
        Initialize with target sectors to focus on.
        
        Args:
            target_sectors (list): List of sector names to focus on
            **kwargs: Other arguments passed to parent class
        """
        super().__init__(**kwargs)
        self.target_sectors = target_sectors or []
    
    def _filter_at_level(self, level, dataframe):
        """
        Filter to focus on target sectors at level 1.
        """
        if level >= 1 and self.target_sectors:
            # Only include rows where level 1 is in target sectors
            level1_col = "tax_map_level1"
            mask = dataframe[level1_col].isin(self.target_sectors)
            return dataframe[mask]
        
        return dataframe
