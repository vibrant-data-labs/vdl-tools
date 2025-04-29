import os

from typing import TypeVar
import pathlib as pl
import configparser
from vdl_tools.shared_tools.tools.logger import logger

T = TypeVar("T")
_warn_set = set()


def get_configuration_value(config: dict, key: str, defaults: T = None) -> T:
    if not config:
        raise ValueError(f'Configuration was not found')

    if '.' in key:
        parent_key, rest = key.split('.', maxsplit=1)
        if parent_key in config:
            return get_configuration_value(config[parent_key], rest, defaults)

    if key not in config:
        if defaults is not None:
            if key not in _warn_set:
                logger.warning(
                    'Configuration value for "%s" is not set. Using default value "%s".',
                    key,
                    defaults,
                )
            _warn_set.add(key)
            return defaults

        raise ValueError(f'Configuration value for {key} is not set.')

    return config[key]


def get_configuration(configpath: pl.Path = None) -> dict:
    if configpath is None:
        if os.getenv('VDL_GLOBAL_CONFIG_PATH', None):
            configpath = pl.Path(os.getenv('VDL_GLOBAL_CONFIG_PATH'))
        else:
            configpath = pl.Path.cwd() / 'config.ini'
    if configpath.exists():
        logger.debug(
            'Config file found at %s. Using it for configuration.', configpath)
    else:
        logger.warning(
            'Config file was not found at %s. Expecting configuration to be passed explicitly.', configpath)
        return {}

    config = configparser.ConfigParser()
    config.read(configpath)
    return config
