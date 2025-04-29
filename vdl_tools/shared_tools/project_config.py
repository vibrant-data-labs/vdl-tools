import configparser
import pathlib as pl
from typing import Dict

substitutions = {}
# {'data': 'climate-tech/data',
#  'cftroot': 'climate-tech/data/cft-us-published'}


def get_project_config():
    config = configparser.ConfigParser()
    config_path = pl.Path.cwd() / "config.ini"
    if not config_path.exists():
        raise ValueError(f"Configuration file {config_path} does not exist")

    config.read(config_path)

    return config["project"]


def get_paths() -> Dict[str, pl.Path]:
    global dataroot
    global cftroot
    paths_config = configparser.ConfigParser()
    paths_file = pl.Path.cwd() / "paths.ini"
    paths_config.read(paths_file)

    path_items = list(paths_config.items('paths'))
    # replace {data} with (relative) data root
    cwd = pl.Path.cwd()
    subst = substitutions.copy()
    res = dict()
    for key, pval in path_items:
        pval = pval.format(**subst)
        subst[key] = pval
        res[key] = cwd / pval
    return res


def set_dataroot(val):
    if val is not None:
        substitutions['data'] = f'climate-tech/{val}'
    (pl.Path.cwd() / substitutions['data']).mkdir(parents=True, exist_ok=True)


def set_cftroot(val):
    if val is not None:
        substitutions['cftroot'] = f'../climate-landscape/data/{val}'


if __name__ == '__main__':
    paths = get_paths()
