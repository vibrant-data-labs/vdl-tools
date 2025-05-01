import configparser
import pathlib as pl
import logging
from typing import TypeVar

T = TypeVar("T")

data_types_map = {
    'org': 'organizations',
    'profile': 'profiles',
    'org-member': 'org-members'
}
