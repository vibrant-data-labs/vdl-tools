"""Parquet-backed DataFrame I/O with transparent local caching for S3 reads.

Drop-in replacement for ``pd.read_json`` / ``DataFrame.to_json`` that:

- writes typed, ZSTD-compressed Parquet,
- works with local paths, ``file://`` URIs, and ``s3://`` URIs,
- for ``s3://`` reads, caches locally with ETag validation on every open
  (no silent stale reads when someone else pushes a new version),
- serializes dict/list columns as JSON (round-trips via footer metadata),
- coerces mixed-scalar-type object columns to string (with a warning),
- stores caller-supplied ``lineage`` in the footer; retrieve via :func:`get_lineage`.

Cache dir defaults to ``~/.cache/vdl-tools/parquet``; override with
``VDL_PARQUET_CACHE_DIR`` or by passing ``cache_dir=``.
"""

from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.s3_tools import get_s3_client, create_bucket, bucket_exists


DEFAULT_CACHE_DIR = Path(
    os.environ.get("VDL_PARQUET_CACHE_DIR", Path.home() / ".cache" / "vdl-tools" / "parquet")
)

_JSON_COLS_KEY = b"vdl_json_columns"
_LINEAGE_KEY = b"vdl_lineage"
_SAMPLE_SIZE = 100  # non-null values per column to check when scanning


# ---------------------------------------------------------------------------
# URIs & S3 credentials
# ---------------------------------------------------------------------------

def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _s3_creds() -> dict:
    """Read AWS creds from config.ini ``[aws]``. Empty dict → boto3 default chain."""
    try:
        aws = get_configuration()["aws"]
    except Exception:
        return {}
    opts: dict = {}
    if aws.get("access_key_id"):
        opts["key"] = aws["access_key_id"]
    if aws.get("secret_access_key"):
        opts["secret"] = aws["secret_access_key"]
    if aws.get("region"):
        opts["client_kwargs"] = {"region_name": aws["region"]}
    return opts


def _read_target(
    uri: str,
    use_cache: bool,
    cache_dir: Path | None,
    check_remote: bool,
) -> tuple[str, dict]:
    """Return (effective_uri, fsspec_opts) for reading from ``uri``."""
    if not _is_s3(uri):
        return uri, {}
    if not use_cache:
        return uri, {"s3": _s3_creds()}
    cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return f"filecache::{uri}", {
        "filecache": {
            "cache_storage": str(cache_dir),
            "check_files": check_remote,  # ETag HEAD on every open
            "expiry_time": None,          # no TTL — rely on ETag check
            "same_names": False,
        },
        "s3": _s3_creds(),
    }


# ---------------------------------------------------------------------------
# Column scanning & value coercion
# ---------------------------------------------------------------------------

def _scan_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Classify object columns by sampling up to ``_SAMPLE_SIZE`` non-null values.

    Returns ``(json_cols, mixed_cols)``:
    - ``json_cols``: any sampled value is a dict/list/tuple → JSON-encode
    - ``mixed_cols``: >1 scalar type seen (no dict/list) → coerce to string

    Non-object columns are skipped (pandas already has one type for them).
    """
    json_cols, mixed_cols = [], []
    for col in df.columns:
        if df[col].dtype != object:
            continue
        sample = df[col].dropna().head(_SAMPLE_SIZE)
        if sample.empty:
            continue
        types = {type(v) for v in sample}
        if types & {dict, list, tuple}:
            json_cols.append(col)
        elif len(types) > 1:
            mixed_cols.append(col)
    return json_cols, mixed_cols


def _is_null(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def _encode_json(v):
    return None if _is_null(v) else json.dumps(v, default=str, ensure_ascii=False)


def _decode_json(v):
    return None if _is_null(v) else json.loads(v)


def _to_string(v):
    return None if _is_null(v) else str(v)


def _is_s3_bucket_exists(uri: str) -> bool:
    return _is_s3(uri) and bucket_exists(uri.split("/")[2])


# ---------------------------------------------------------------------------
# Public API — single file
# ---------------------------------------------------------------------------

def write_dataframe(
    df: pd.DataFrame,
    uri: str | Path,
    *,
    lineage: dict | None = None,
) -> str:
    """Write ``df`` to ``uri`` as Parquet (ZSTD level 3).

    - Dict/list columns are JSON-encoded; they round-trip via :func:`read_dataframe`.
    - Object columns with mixed scalar types are coerced to string (with a
      warning) so pyarrow has a single type per column.
    - ``lineage`` is stored in the file footer under ``vdl_lineage``; a
      ``created_at`` timestamp is added automatically.

    Parameters
    ----------
    df
        DataFrame to write.
    uri
        Destination. Local path (``str`` or ``Path``), ``file://`` URI, or
        full ``s3://`` URI including bucket.
    lineage
        Optional JSON-serializable dict stored in the file footer under
        ``vdl_lineage``. Recommended keys: ``source``, ``created_by``,
        plus any domain-specific metadata (search terms, filters, row
        counts, etc.). Retrieve via :func:`get_lineage`.

    Returns
    -------
    str
        The URI written (useful for chaining / logging).
    """
    uri = str(uri)
    json_cols, mixed_cols = _scan_columns(df)

    if json_cols or mixed_cols:
        df = df.copy()
    for col in json_cols:
        df[col] = df[col].map(_encode_json)
    if mixed_cols:
        logger.warning("Coercing mixed-type columns to string: %s", mixed_cols)
        for col in mixed_cols:
            df[col] = df[col].map(_to_string)

    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[_JSON_COLS_KEY] = json.dumps(json_cols).encode()
    meta[_LINEAGE_KEY] = json.dumps(
        {"created_at": datetime.now(timezone.utc).isoformat(), **(lineage or {})},
        default=str,
    ).encode()
    table = table.replace_schema_metadata(meta)

    if _is_s3(uri):
        opts = {"s3": _s3_creds()}
        # Check if the bucket exists and create it if it doesn't
        if not _is_s3_bucket_exists(uri):
            create_bucket(get_s3_client(), uri.split("/")[2])
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        opts = {}

    with fsspec.open(uri, "wb", **opts) as f:
        pq.write_table(table, f, compression="zstd", compression_level=3)

    logger.info("Wrote %d rows → %s (%d json cols)", len(df), uri, len(json_cols))
    return uri


def read_dataframe(
    uri: str | Path,
    *,
    columns: list[str] | None = None,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame.

    For ``s3://`` sources with ``use_cache=True`` (default), reads go through
    a local filecache with an ETag HEAD on every open. Local paths read
    directly — cache params are ignored.

    Parameters
    ----------
    uri
        Source. Local path (``str`` or ``Path``), ``file://`` URI, or full
        ``s3://`` URI including bucket.
    columns
        If set, only these columns are read (column pruning). Much faster
        for wide tables on remote storage.
    use_cache
        If False, skip the local cache and read straight from S3 every time.
        Useful for debugging or confirming remote contents.
    cache_dir
        Override the default cache directory (``~/.cache/vdl-tools/parquet``,
        or whatever ``VDL_PARQUET_CACHE_DIR`` is set to).
    check_remote
        If True (default), HEAD-check the remote ETag on every open so a
        concurrent writer's new version invalidates the local cache. Set to
        False for offline / airplane use — the local cache is served without
        validating against the remote.
    """
    uri = str(uri)
    effective_uri, opts = _read_target(uri, use_cache, cache_dir, check_remote)

    with fsspec.open(effective_uri, "rb", **opts) as f:
        table = pq.read_table(f, columns=columns)

    meta = table.schema.metadata or {}
    json_cols = set(json.loads(meta.get(_JSON_COLS_KEY) or b"[]"))

    df = table.to_pandas()
    for col in json_cols & set(df.columns):
        df[col] = df[col].map(_decode_json)
    return df


def get_lineage(
    uri: str | Path,
    *,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> dict:
    """Return the ``vdl_lineage`` dict from a Parquet file's footer.

    Metadata-only read — does not load row groups. Returns an empty dict if
    the file has no ``vdl_lineage`` metadata (e.g. it wasn't written by
    :func:`write_dataframe`).

    Parameters
    ----------
    uri
        Source. Local path, ``file://`` URI, or full ``s3://`` URI.
    use_cache, cache_dir, check_remote
        See :func:`read_dataframe` — same cache semantics apply.
    """
    uri = str(uri)
    effective_uri, opts = _read_target(uri, use_cache, cache_dir, check_remote)
    with fsspec.open(effective_uri, "rb", **opts) as f:
        meta = pq.ParquetFile(f).schema_arrow.metadata or {}
    return json.loads(meta.get(_LINEAGE_KEY) or b"{}")


# ---------------------------------------------------------------------------
# Public API — multi-table
# ---------------------------------------------------------------------------

def write_dataframes(
    tables: dict[str, pd.DataFrame],
    dir_uri: str | Path,
    *,
    lineage: dict | None = None,
) -> dict[str, str]:
    """Write each non-None DataFrame as ``{dir_uri}/{name}.parquet``.

    Each file's footer gets the same ``lineage`` dict plus a ``table_name``
    field so individual files are self-describing.

    Parameters
    ----------
    tables
        Mapping of table name → DataFrame. Entries with a ``None`` value
        are skipped (so callers can pass an optional table slot).
    dir_uri
        URI of the directory-like container. Local path, ``file://`` URI,
        or full ``s3://`` URI including bucket.
    lineage
        Optional dict applied to every written file. ``table_name`` is
        added automatically per file.

    Returns
    -------
    dict[str, str]
        ``{table_name: uri_written}``.
    """
    base = str(dir_uri).rstrip("/")
    return {
        name: write_dataframe(
            df,
            f"{base}/{name}.parquet",
            lineage={**(lineage or {}), "table_name": name},
        )
        for name, df in tables.items()
        if df is not None
    }


def read_dataframes(
    dir_uri: str | Path,
    names: list[str],
    *,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> dict[str, pd.DataFrame]:
    """Read ``{dir_uri}/{name}.parquet`` for each name.

    Parameters
    ----------
    dir_uri
        URI of the directory-like container. Local path, ``file://`` URI,
        or full ``s3://`` URI including bucket.
    names
        Base filenames to load (without the ``.parquet`` extension).
    use_cache, cache_dir, check_remote
        See :func:`read_dataframe`.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{name: df}``. Missing files raise ``FileNotFoundError``.
    """
    base = str(dir_uri).rstrip("/")
    return {
        name: read_dataframe(
            f"{base}/{name}.parquet",
            use_cache=use_cache,
            cache_dir=cache_dir,
            check_remote=check_remote,
        )
        for name in names
    }


# ---------------------------------------------------------------------------
# Cache maintenance
# ---------------------------------------------------------------------------

def prune_cache(cache_dir: Path | None = None, keep_recent_days: int = 30) -> int:
    """Delete cached files not accessed in the last ``keep_recent_days`` days.

    Safe to run anytime — cache is transparently rebuilt on next read.

    Parameters
    ----------
    cache_dir
        Cache directory to prune. Defaults to ``DEFAULT_CACHE_DIR`` (i.e.
        ``~/.cache/vdl-tools/parquet``, or ``VDL_PARQUET_CACHE_DIR`` if set).
    keep_recent_days
        Files with atime newer than this are kept. Defaults to 30 days.

    Returns
    -------
    int
        Number of files removed (0 if ``cache_dir`` does not exist).
    """
    cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
    if not cache_dir.exists():
        return 0
    cutoff = time.time() - keep_recent_days * 86400
    removed = 0
    for p in cache_dir.rglob("*"):
        if p.is_file() and p.stat().st_atime < cutoff:
            try:
                p.unlink()
                removed += 1
            except OSError:
                pass
    logger.info("Pruned %d cached files from %s", removed, cache_dir)
    return removed
