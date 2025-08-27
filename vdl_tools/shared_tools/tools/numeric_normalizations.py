import numpy as np
import pandas as pd


def normalize_value(
    value,  # value to normalize
    reference_value,  # parent value to normalize by
):
    normalized_value = (value - reference_value) / (value + reference_value)
    return normalized_value


def geometric_mean_values(
    values: list[float] | np.array | pd.Series,
    remove_nan=True, # whether to remove NaN values)
    remove_zero=True # whether to remove zero values
):
    values = np.array(values, dtype=float)
    if remove_nan:
        values = values[~np.isnan(values)]
    if remove_zero:
        values = values[values != 0]
    if (len(values) == 0) or np.all(np.isnan(values)):
        return np.nan  # Return NaN if the input is empty or all NaN
    else:
        # compute geometric mean of values, preserve sign  -> mean^^1/n
        # remove NaN and Zero values
        avg = values.mean()

        # Create the product of the absolute values (so no sign is lost)
        product = np.prod(np.abs(values))
        geo_mean = np.power(product, 1 / len(values))
        return geo_mean


def max_min_normalization(
    value: float,
    min_value: float,
    max_value: float,
):
    """Normalize a value to the range [0, 1]."""
    # Convert to float, catch conversion errors
    if any(pd.isna([value, min_value, max_value])):
        return np.nan
    if any([not x for x in [value, min_value, max_value]]):
        return np.nan

    diff = max_value - min_value
    # Avoid division by zero
    if diff == 0:
        if max_value > 0:
            return 0.5
        else:
            return 0
    value_normalized = (value - min_value) / diff
    return value_normalized
