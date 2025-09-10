import numpy as np
import pandas as pd


def normalize_value(
    value,  # value to normalize
    reference_value,  # parent value to normalize by
):
    normalized_value = (value - reference_value) / (value + reference_value)
    return normalized_value


def geometric_mean_values(
    values,
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
        # Create the product of the absolute values
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
    diff = max_value - min_value
    # Avoid division by zero
    if diff == 0:
        if max_value > 0:
            return 0.5
        else:
            return 0
    value_normalized = (value - min_value) / diff
    return value_normalized


def zero_max_normalization(
    value,
    max_value,
):
    if max_value == 0:
        raise ValueError("Max value is 0")
    """Normalize a value to the range [0, 1]."""
    return value / max_value