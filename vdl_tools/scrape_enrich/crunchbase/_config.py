import pathlib as pl
import configparser

from vdl_tools.shared_tools.tools.config_utils import get_configuration

def get_url(source_url):
    wd = pl.Path.cwd()
    # load config.ini and get use and key
    ### config
    cfg = get_configuration()
    section = "crunchbase"
    if section not in cfg:
        raise KeyError('CB section not found in config.ini')

    return f"{source_url}?user_key={cfg[section]['api_key']}"
    