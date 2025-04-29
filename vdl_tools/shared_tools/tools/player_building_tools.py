import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger


def rename_clean_for_player(
    ndf,
    player_attrib_settings_path
):
    logger.info('renaming and cleaning columns for player')
    # load player attribute settings
    df_attribs = pd.read_excel(player_attrib_settings_path)
    df_attribs = df_attribs[df_attribs.Keep == 1]
    keep_cols = df_attribs['Attribute'].tolist()
    rename_cols = df_attribs.set_index('Attribute').to_dict()['Display_Name']
    ndf = ndf[keep_cols]
    ndf.rename(columns=rename_cols, inplace=True)
    return ndf