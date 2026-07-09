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
from vdl_tools.shared_tools import _s3_cache_backend as _b


DEFAULT_CACHE_DIR = Path(
    os.environ.get("VDL_PARQUET_CACHE_DIR", _b.DEFAULT_CACHE_ROOT / "parquet")
)

_JSON_COLS_KEY = b"vdl_json_columns"
_LINEAGE_KEY = b"vdl_lineage"
_SAMPLE_SIZE = 100  # non-null values per column to check when scanning


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


def _scan_columns_full(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Like :func:`_scan_columns` but scans ALL non-null values, not just a sample.

    Used only on the ``from_pandas`` exception path, so the O(rows) cost is paid
    rarely — it catches columns that are type-consistent in their first
    ``_SAMPLE_SIZE`` values but mixed deeper (a stray ``int`` or a buried
    dict/list past the sampling window).
    """
    json_cols, mixed_cols = [], []
    for col in df.columns:
        if df[col].dtype != object:
            continue
        types = {type(v) for v in df[col].to_numpy() if not _is_null(v)}
        if not types:
            continue
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

    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
    except (pa.ArrowTypeError, pa.ArrowInvalid):
        # Sampled scan missed a type-inconsistent column (a stray int or a
        # buried dict/list past _SAMPLE_SIZE). Re-classify with a full scan,
        # coerce only the columns the sampled pass missed, and retry once.
        json2, mixed2 = _scan_columns_full(df)
        json2 = [c for c in json2 if c not in json_cols]
        mixed2 = [c for c in mixed2 if c not in mixed_cols]
        logger.warning(
            "Sampled scan missed type-inconsistent columns; full-scan coercing "
            "json=%s mixed=%s and retrying", json2, mixed2)
        df = df.copy()  # first pass only copied if it found something to coerce
        for col in json2:
            df[col] = df[col].map(_encode_json)
            json_cols.append(col)  # keep _JSON_COLS_KEY metadata accurate
        for col in mixed2:
            df[col] = df[col].map(_to_string)
        table = pa.Table.from_pandas(df, preserve_index=False)

    meta = dict(table.schema.metadata or {})
    meta[_JSON_COLS_KEY] = json.dumps(json_cols).encode()
    meta[_LINEAGE_KEY] = json.dumps(
        {"created_at": datetime.now(timezone.utc).isoformat(), **(lineage or {})},
        default=str,
    ).encode()
    table = table.replace_schema_metadata(meta)

    opts = _b.write_target(uri)

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
    effective_uri, opts = _b.read_target(
        uri, use_cache, cache_dir or DEFAULT_CACHE_DIR, check_remote
    )

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
    effective_uri, opts = _b.read_target(
        uri, use_cache, cache_dir or DEFAULT_CACHE_DIR, check_remote
    )
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
