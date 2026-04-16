"""Parquet-backed DataFrame I/O with transparent local caching for remote stores.

Purpose
-------
A drop-in replacement for ``pd.read_json`` / ``DataFrame.to_json`` that:

1. Writes compressed, schema-bearing Parquet instead of JSON.
2. Works transparently with local paths, ``file://`` URIs, and ``s3://`` URIs.
3. For remote reads, uses fsspec's ``filecache`` with ETag validation so that
   (a) each user only downloads a given file once, and (b) if someone pushes
   a new version, the ETag HEAD check on the next read invalidates the local
   cache automatically — no silent stale reads.
4. Handles dict/list columns by JSON-encoding them to strings; the list of
   JSON-encoded columns is recorded in the Parquet footer metadata so reads
   round-trip cleanly back to Python dicts/lists.
5. Stores caller-supplied lineage metadata (search terms, source, timestamp,
   vdl-tools version, etc.) in the footer for later audit.

Usage
-----
Single file::

    from vdl_tools.shared_tools.parquet_cache import write_dataframe, read_dataframe

    # anywhere pd.to_json / pd.read_json were used
    write_dataframe(df, "s3://shared-data-clone/cb_raw/fisheries/organizations.parquet")
    df = read_dataframe("s3://shared-data-clone/cb_raw/fisheries/organizations.parquet")

Multiple files under a shared prefix::

    from vdl_tools.shared_tools.parquet_cache import write_dataframes, read_dataframes

    write_dataframes(
        {"organizations": df_orgs, "funding_rounds": df_fr, ...},
        prefix="s3://shared-data-clone/cb_raw/fisheries",
        lineage={"source": "crunchbase", "search_terms": [...]},
    )
    tables = read_dataframes(
        prefix="s3://shared-data-clone/cb_raw/fisheries",
        names=["organizations", "funding_rounds", "founders"],
    )

Caching behaviour
-----------------
Local paths are passed through as-is — no caching layer.

``s3://`` paths are read through fsspec's ``filecache`` with
``check_files=True``. Every read issues an S3 HEAD against the object and
compares ETags; matching ETag → serve from local cache, differing ETag →
re-download. This costs one ~10ms HEAD per read in exchange for correctness
against concurrent writers.

Cache location is ``~/.cache/vdl-tools/parquet`` by default; override by
setting ``VDL_PARQUET_CACHE_DIR`` or passing ``cache_dir=``.

Dependencies
------------
Requires ``pyarrow`` (already in our stack) and, for S3, ``s3fs``. Install
``s3fs`` if not already present: ``pip install s3fs``.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.config_utils import get_configuration


# ---------------------------------------------------------------------------
# Constants & config
# ---------------------------------------------------------------------------

DEFAULT_CACHE_DIR = Path(
    os.environ.get("VDL_PARQUET_CACHE_DIR", Path.home() / ".cache" / "vdl-tools" / "parquet")
)

# Metadata keys we write into the Parquet footer. pyarrow requires bytes.
_META_JSON_COLS = b"vdl_json_columns"
_META_LINEAGE = b"vdl_lineage"
_META_WRITER_VERSION = b"vdl_writer_version"
_WRITER_VERSION = "1"

# Remote protocols that should be routed through filecache.
_REMOTE_PROTOCOLS = {"s3", "gs", "gcs", "az", "abfs", "http", "https"}


# ---------------------------------------------------------------------------
# URI / path handling
# ---------------------------------------------------------------------------

def _normalize_uri(uri: str | Path) -> str:
    """Return a string URI. Pathlib paths become plain local paths."""
    if isinstance(uri, Path):
        return str(uri)
    return uri


def _protocol(uri: str) -> str:
    """Return fsspec protocol for a URI ('' for local paths)."""
    parsed = urlparse(uri)
    # Windows drive letters (C:\) parse as scheme='c'; treat single-letter as local.
    if len(parsed.scheme) <= 1:
        return ""
    return parsed.scheme


def _is_remote(uri: str) -> bool:
    return _protocol(uri) in _REMOTE_PROTOCOLS


def _s3_storage_options() -> dict[str, Any]:
    """Pull S3 credentials from the standard vdl-tools config.ini.

    Returns an empty dict if config is missing — in that case s3fs falls
    back to the normal boto3 credential chain (env vars, ~/.aws/credentials, etc.).
    """
    try:
        config = get_configuration()
        aws = config.get("aws", {}) if config else {}
        opts: dict[str, Any] = {}
        # s3fs expects 'key' and 'secret' (not 'aws_access_key_id' etc.)
        if aws.get("access_key_id"):
            opts["key"] = aws["access_key_id"]
        if aws.get("secret_access_key"):
            opts["secret"] = aws["secret_access_key"]
        if aws.get("region"):
            opts["client_kwargs"] = {"region_name": aws["region"]}
        return opts
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug("Could not load S3 config (%s); relying on default credential chain.", exc)
        return {}


def _read_storage_options(
    uri: str,
    cache_dir: Path | None,
    check_remote: bool,
    extra: dict[str, Any] | None,
) -> tuple[str, dict[str, Any]]:
    """Build (possibly-wrapped) URI and storage_options for a read.

    For remote URIs, wraps with ``filecache::`` so downloads are cached locally
    and validated against remote ETag on each open.
    """
    if not _is_remote(uri):
        return uri, extra or {}

    cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)

    protocol = _protocol(uri)
    wrapped_uri = f"filecache::{uri}"

    storage_options: dict[str, Any] = {
        "filecache": {
            "cache_storage": str(cache_dir),
            "check_files": check_remote,  # HEAD-check ETag on every open
            "expiry_time": None,          # no TTL; rely on ETag check
            "same_names": False,          # cache by URL hash, not filename
        },
    }

    if protocol == "s3":
        storage_options["s3"] = {**_s3_storage_options(), **((extra or {}).get("s3", {}))}
    # Merge any caller-supplied options for other protocols verbatim.
    for k, v in (extra or {}).items():
        if k not in storage_options:
            storage_options[k] = v

    return wrapped_uri, storage_options


def _write_storage_options(
    uri: str,
    extra: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build storage_options for a write — no cache layer, just creds."""
    if not _is_remote(uri):
        return extra or {}
    opts: dict[str, Any] = {}
    if _protocol(uri) == "s3":
        opts.update(_s3_storage_options())
    opts.update(extra or {})
    return opts


# ---------------------------------------------------------------------------
# JSON column handling (for dict/list columns)
# ---------------------------------------------------------------------------

def _is_json_scalar(v: Any) -> bool:
    """True iff ``v`` is a dict/list/tuple that needs JSON-encoding for Parquet."""
    return isinstance(v, (dict, list, tuple))


def _detect_json_columns(df: pd.DataFrame) -> list[str]:
    """Find columns containing dict/list values (ignoring nulls)."""
    json_cols: list[str] = []
    for col in df.columns:
        s = df[col]
        if s.dtype != object:
            continue
        non_null = s.dropna()
        if non_null.empty:
            continue
        # Sample the first non-null value; if it's a dict/list, treat column as JSON.
        # (Mixed-type object columns are rare in our data; if they occur, caller
        #  should pass ``json_columns=`` explicitly.)
        if _is_json_scalar(non_null.iloc[0]):
            json_cols.append(col)
    return json_cols


def _encode_json_column(series: pd.Series) -> pd.Series:
    def enc(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return json.dumps(v, default=str, ensure_ascii=False)
    return series.map(enc)


def _decode_json_column(series: pd.Series) -> pd.Series:
    def dec(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return json.loads(v)
    return series.map(dec)


# ---------------------------------------------------------------------------
# Public API — single file
# ---------------------------------------------------------------------------

def write_dataframe(
    df: pd.DataFrame,
    uri: str | Path,
    *,
    compression: str = "zstd",
    compression_level: int = 3,
    json_columns: Iterable[str] | None = None,
    lineage: dict[str, Any] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> str:
    """Write ``df`` to ``uri`` as Parquet.

    Parameters
    ----------
    df
        DataFrame to write.
    uri
        Destination. Local path, ``file://`` URI, or ``s3://`` URI.
    compression, compression_level
        Passed to pyarrow. Default ZSTD level 3 — ~2-3x smaller than Snappy
        at comparable decode speed.
    json_columns
        Columns whose values are Python dicts/lists. If ``None``, auto-detected
        from the first non-null value in each object column. Pass an empty
        list to disable auto-detection. These columns are serialized as JSON
        strings in Parquet; ``read_dataframe`` will decode them back.
    lineage
        Arbitrary JSON-serializable dict stored in the Parquet footer under
        ``vdl_lineage``. Recommended keys: ``source``, ``created_at``,
        ``created_by``, ``vdl_tools_version``, plus anything domain-specific
        (search terms, filters, row counts, etc.).
    storage_options
        Passed through to the fsspec filesystem (e.g. custom S3 endpoint).

    Returns
    -------
    str
        The URI that was written (useful for chaining / logging).
    """
    uri_str = _normalize_uri(uri)

    if json_columns is None:
        json_cols = _detect_json_columns(df)
    else:
        json_cols = list(json_columns)

    # Encode JSON columns to strings before handing to pyarrow.
    if json_cols:
        df = df.copy()
        for col in json_cols:
            df[col] = _encode_json_column(df[col])

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Attach metadata to the schema (preserving pandas's own schema metadata).
    existing_meta = dict(table.schema.metadata or {})
    full_lineage = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        **(lineage or {}),
    }
    existing_meta[_META_JSON_COLS] = json.dumps(json_cols).encode()
    existing_meta[_META_LINEAGE] = json.dumps(full_lineage, default=str).encode()
    existing_meta[_META_WRITER_VERSION] = _WRITER_VERSION.encode()
    table = table.replace_schema_metadata(existing_meta)

    write_opts = _write_storage_options(uri_str, storage_options)

    # Ensure parent directory exists for local writes.
    if not _is_remote(uri_str):
        Path(uri_str).parent.mkdir(parents=True, exist_ok=True)

    with fsspec.open(uri_str, "wb", **write_opts) as f:
        pq.write_table(
            table,
            f,
            compression=compression,
            compression_level=compression_level,
        )

    logger.info("Wrote %d rows → %s (%d json cols)", len(df), uri_str, len(json_cols))
    return uri_str


def read_dataframe(
    uri: str | Path,
    *,
    columns: Iterable[str] | None = None,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
    dtype_backend: str = "numpy_nullable",
    storage_options: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Read a Parquet file written by :func:`write_dataframe` back into a DataFrame.

    Parameters
    ----------
    uri
        Source. Local path, ``file://`` URI, or ``s3://`` URI.
    columns
        If set, only these columns are read (column pruning — much faster for
        wide tables on remote storage).
    use_cache
        If False, bypass the local cache and read straight from remote. Rarely
        needed; mostly useful for debugging.
    cache_dir
        Override the default cache directory (``~/.cache/vdl-tools/parquet``).
    check_remote
        If True (default), HEAD-check the remote ETag on every open. Set to
        False for offline / airplane use — you'll serve whatever's in the
        cache without validating.
    dtype_backend
        Passed to pandas. Default ``"numpy_nullable"`` preserves pandas's
        nullable dtypes on round-trip. Use ``"pyarrow"`` for the fully-typed
        Arrow-backed frame.
    storage_options
        Extra fsspec options.
    """
    uri_str = _normalize_uri(uri)

    if not use_cache or not _is_remote(uri_str):
        effective_uri = uri_str
        opts = _write_storage_options(uri_str, storage_options)  # write opts = creds only, no cache
    else:
        effective_uri, opts = _read_storage_options(
            uri_str,
            cache_dir=cache_dir,
            check_remote=check_remote,
            extra=storage_options,
        )

    with fsspec.open(effective_uri, "rb", **opts) as f:
        table = pq.read_table(f, columns=list(columns) if columns else None)

    meta = table.schema.metadata or {}
    json_cols_raw = meta.get(_META_JSON_COLS)
    json_cols = set(json.loads(json_cols_raw)) if json_cols_raw else set()

    df = table.to_pandas(types_mapper=pd.ArrowDtype if dtype_backend == "pyarrow" else None)

    # Decode JSON columns back to Python objects. Skip any pruned columns.
    for col in json_cols:
        if col in df.columns:
            df[col] = _decode_json_column(df[col])

    return df


def get_lineage(
    uri: str | Path,
    *,
    cache_dir: Path | None = None,
    check_remote: bool = True,
    storage_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the ``vdl_lineage`` dict embedded in a Parquet file's footer."""
    uri_str = _normalize_uri(uri)

    if _is_remote(uri_str):
        effective_uri, opts = _read_storage_options(
            uri_str, cache_dir=cache_dir, check_remote=check_remote, extra=storage_options,
        )
    else:
        effective_uri, opts = uri_str, (storage_options or {})

    with fsspec.open(effective_uri, "rb", **opts) as f:
        # metadata-only read — pyarrow reads the footer without loading row groups
        pqfile = pq.ParquetFile(f)
        meta = pqfile.schema_arrow.metadata or {}

    raw = meta.get(_META_LINEAGE)
    return json.loads(raw) if raw else {}


# ---------------------------------------------------------------------------
# Public API — multi-table convenience
# ---------------------------------------------------------------------------

def write_dataframes(
    tables: dict[str, pd.DataFrame],
    prefix: str | Path,
    *,
    compression: str = "zstd",
    compression_level: int = 3,
    lineage: dict[str, Any] | None = None,
    json_columns_per_table: dict[str, Iterable[str]] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Write ``{name: df}`` pairs as ``{prefix}/{name}.parquet``.

    Each file carries the same ``lineage`` plus a ``table_name`` field, so
    you can reconstruct where any one file came from without the prefix.

    Returns
    -------
    dict[str, str]
        ``{table_name: uri_written}``.
    """
    prefix_str = _normalize_uri(prefix).rstrip("/")
    json_columns_per_table = json_columns_per_table or {}

    written: dict[str, str] = {}
    for name, df in tables.items():
        if df is None:
            logger.debug("Skipping %s (None)", name)
            continue
        uri = f"{prefix_str}/{name}.parquet"
        written[name] = write_dataframe(
            df,
            uri,
            compression=compression,
            compression_level=compression_level,
            json_columns=json_columns_per_table.get(name),
            lineage={**(lineage or {}), "table_name": name},
            storage_options=storage_options,
        )
    return written


def read_dataframes(
    prefix: str | Path,
    names: Iterable[str],
    *,
    columns_per_table: dict[str, Iterable[str]] | None = None,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
    dtype_backend: str = "numpy_nullable",
    storage_options: dict[str, Any] | None = None,
) -> dict[str, pd.DataFrame]:
    """Read ``{prefix}/{name}.parquet`` for each name into a dict of DataFrames."""
    prefix_str = _normalize_uri(prefix).rstrip("/")
    columns_per_table = columns_per_table or {}

    out: dict[str, pd.DataFrame] = {}
    for name in names:
        uri = f"{prefix_str}/{name}.parquet"
        out[name] = read_dataframe(
            uri,
            columns=columns_per_table.get(name),
            use_cache=use_cache,
            cache_dir=cache_dir,
            check_remote=check_remote,
            dtype_backend=dtype_backend,
            storage_options=storage_options,
        )
    return out


# ---------------------------------------------------------------------------
# Cache maintenance
# ---------------------------------------------------------------------------

def prune_cache(
    cache_dir: Path | None = None,
    keep_recent_days: int = 30,
) -> int:
    """Delete cached files not accessed in the last ``keep_recent_days`` days.

    Returns the number of files removed. Safe to run anytime — cache is
    transparently rebuilt on next read.
    """
    import time

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
            except OSError as exc:
                logger.warning("Could not remove cache file %s: %s", p, exc)
    logger.info("Pruned %d cached files older than %d days from %s",
                removed, keep_recent_days, cache_dir)
    return removed
