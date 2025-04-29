import pandas as pd


def coerced_bool(value):
    """
    Check if a value is falsey
    """
    # Need to check for a list specifically
    # because pd.isnull([]) checks each element
    # in the array and returns array with each
    if isinstance(value, list):
        return bool(value)
    if pd.isnull(value):
        return False
    return bool(value)
