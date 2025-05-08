import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger


def rename_clean_for_player(
    ndf,
    df_settings
):
    logger.info('renaming and cleaning columns for player')
    df_settings = df_settings[df_settings.Keep == 1].copy()
    keep_cols = df_settings['Attribute'].tolist()
    rename_cols = df_settings.set_index('Attribute').to_dict()['Display_Name']
    ndf = ndf[keep_cols].copy()
    ndf.rename(columns=rename_cols, inplace=True)
    return ndf

def get_attribute_settings(df_settings):
    # remove any columns that are not needed
    df_settings = df_settings[df_settings['Keep'] == 1].copy()
    # for each settings column create a list of attributes that = 1 and name the list by the column name.
    # This is the format that the player expects for the player attribute settings file
    settings_list = [col for col in df_settings.columns.tolist() if col not in ['Name', 'Display_Name']]
    attrib_settings = {}  # dict to hold the setting name and associated list of attributes
    for setting in settings_list:
        # get list of attributes that = 1 for each setting
        attrib_settings[setting] = list(df_settings[df_settings[setting] == 1].Display_Name)
    # create a dictionary of attribute descriptions just for the attributes that have a tooltip
    df_tooltips = df_settings[df_settings.tooltip.notnull()]
    attr_descriptions = dict(zip(df_tooltips.Display_Name, df_tooltips.tooltip))
    return attrib_settings, attr_descriptions