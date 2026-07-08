"""Tests for parquet_cache.write_dataframe robustness to deep mixed-type columns.

Covers the sampling blind spot: an object column that is type-consistent in its
first ``_SAMPLE_SIZE`` non-null values but mixed (or JSON) deeper. The sampled
scan misses it, ``pa.Table.from_pandas`` raises, and the full-scan fallback must
coerce and retry.
"""

import logging

import pandas as pd
import pytest

from vdl_tools.shared_tools import parquet_cache as pqc


@pytest.fixture()
def path(tmp_path):
    return str(tmp_path / "frame.parquet")


@pytest.fixture()
def vdl_logs(caplog):
    """Capture records from the vdl_tools logger, which has ``propagate=0`` and
    so never reaches caplog's root handler on its own."""
    pqc.logger.addHandler(caplog.handler)
    caplog.set_level(logging.WARNING, logger=pqc.logger.name)
    try:
        yield caplog
    finally:
        pqc.logger.removeHandler(caplog.handler)


def test_deep_mixed_scalar_column(path, vdl_logs):
    """A stray int past the 100-sample window coerces to string and round-trips."""
    values = ["2015"] * 150 + [2016]  # int at index 150, past _SAMPLE_SIZE=100
    df = pd.DataFrame({"founded": values})

    pqc.write_dataframe(df, path)

    assert "full-scan coercing" in vdl_logs.text  # fallback fired

    out = pqc.read_dataframe(path)
    assert list(out["founded"]) == ["2015"] * 150 + ["2016"]
    assert out["founded"].map(type).eq(str).all()


def test_deep_json_column(path):
    """A dict buried past the 100-sample window is JSON-encoded and decodes back."""
    values = ["x"] * 150 + [{"a": 1}]  # dict at index 150, past _SAMPLE_SIZE
    df = pd.DataFrame({"payload": values})

    pqc.write_dataframe(df, path)

    out = pqc.read_dataframe(path)
    assert out["payload"].iloc[:150].tolist() == ["x"] * 150
    assert out["payload"].iloc[150] == {"a": 1}


def test_fast_path_not_triggered(path, vdl_logs):
    """A clean, type-consistent frame writes without hitting the fallback."""
    df = pd.DataFrame(
        {
            "year": ["2015"] * 200,
            "count": list(range(200)),
            "tags": [["a", "b"]] * 200,  # clean JSON column, handled up front
        }
    )

    pqc.write_dataframe(df, path)

    assert "full-scan coercing" not in vdl_logs.text  # fallback never fired

    out = pqc.read_dataframe(path)
    assert list(out["year"]) == ["2015"] * 200
    assert list(out["count"]) == list(range(200))
    assert out["tags"].iloc[0] == ["a", "b"]
