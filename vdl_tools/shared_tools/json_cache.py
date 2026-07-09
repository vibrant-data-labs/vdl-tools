"""JSON I/O with transparent local caching for S3 reads.

The JSON counterpart to :mod:`parquet_cache`. Same storage backend, same cache
semantics — a drop-in easy way to read/write JSON to local paths, ``file://``,
or ``s3://`` with the remote reads cached locally and ETag-validated on every
open (no silent stale reads).

Two things differ from Parquet, both because plain JSON has no footer:

- **Data files stay pure JSON.** The bytes on disk / in S3 are exactly what
  ``json.dumps`` produced, so a public ``s3://`` object can be fetched straight
  into a browser or ``curl | jq``. Optional ``lineage`` is written to a small
  sidecar (``{uri}.vdl.json``) — never mixed into the data — and read back via
  :func:`get_lineage`.
- **Compression is chosen by extension.** ``…​.json`` is written raw;
  ``…​.json.gz`` is gzipped (JSON compresses ~10x — worth it for internal
  blobs; keep ``.json`` for anything a frontend fetches directly, since fsspec
  writes no ``Content-Encoding`` header).

Cache dir defaults to ``~/.cache/vdl-tools/json``; override with
``VDL_JSON_CACHE_DIR`` or by passing ``cache_dir=``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fsspec

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools import _s3_cache_backend as _b


DEFAULT_CACHE_DIR = Path(
    os.environ.get("VDL_JSON_CACHE_DIR", _b.DEFAULT_CACHE_ROOT / "json")
)

_SIDECAR_SUFFIX = ".vdl.json"


def _compression(uri: str) -> str | None:
    """gzip iff the path ends in ``.gz`` — otherwise raw JSON."""
    return "gzip" if uri.endswith(".gz") else None


# ---------------------------------------------------------------------------
# Public API — single file
# ---------------------------------------------------------------------------

def write_json(
    obj: Any,
    uri: str | Path,
    *,
    lineage: dict | None = None,
    indent: int | None = None,
) -> str:
    """Write ``obj`` as JSON to ``uri``.

    Parameters
    ----------
    obj
        Any JSON-serializable value. Non-serializable leaves fall back to
        ``str`` (matching :mod:`parquet_cache`).
    uri
        Destination. Local path (``str`` or ``Path``), ``file://`` URI, or full
        ``s3://`` URI including bucket. A ``.gz`` suffix triggers gzip.
    lineage
        Optional JSON-serializable dict written to a sidecar
        (``{uri}.vdl.json``) with an auto-added ``created_at``. Retrieve via
        :func:`get_lineage`. When ``None``, no sidecar is written.
    indent
        Passed through to ``json.dumps``. Defaults to compact (no indent);
        S3-served payloads usually want that.

    Returns
    -------
    str
        The URI written.
    """
    uri = str(uri)
    data = json.dumps(obj, default=str, ensure_ascii=False, indent=indent).encode("utf-8")

    opts = _b.write_target(uri)
    with fsspec.open(uri, "wb", compression=_compression(uri), **opts) as f:
        f.write(data)

    if lineage is not None:
        sidecar = uri + _SIDECAR_SUFFIX
        payload = json.dumps(
            {"created_at": datetime.now(timezone.utc).isoformat(), **lineage},
            default=str,
        ).encode("utf-8")
        # sidecar is always small + raw JSON (no compression)
        with fsspec.open(sidecar, "wb", **_b.write_target(sidecar)) as f:
            f.write(payload)

    logger.info("Wrote JSON -> %s%s", uri, " (+lineage)" if lineage is not None else "")
    return uri


def read_json(
    uri: str | Path,
    *,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> Any:
    """Read and parse a JSON file.

    For ``s3://`` sources with ``use_cache=True`` (default), reads go through a
    local filecache with an ETag HEAD on every open. Local paths read directly.

    Parameters
    ----------
    uri
        Source. Local path, ``file://`` URI, or full ``s3://`` URI. ``.gz`` is
        decompressed transparently.
    use_cache
        If False, skip the local cache and read straight from S3 every time.
    cache_dir
        Override the default cache directory
        (``~/.cache/vdl-tools/json``, or ``VDL_JSON_CACHE_DIR``).
    check_remote
        If True (default), HEAD-check the remote ETag on every open so a
        concurrent writer's new version invalidates the local cache. Set False
        for offline use.
    """
    uri = str(uri)
    effective_uri, opts = _b.read_target(
        uri, use_cache, cache_dir or DEFAULT_CACHE_DIR, check_remote
    )
    with fsspec.open(effective_uri, "rb", compression=_compression(uri), **opts) as f:
        return json.loads(f.read())


def get_lineage(
    uri: str | Path,
    *,
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> dict:
    """Return the lineage dict from a file's sidecar (``{uri}.vdl.json``).

    Returns an empty dict if the file was written without ``lineage=``. Same
    cache semantics as :func:`read_json`.
    """
    sidecar = str(uri) + _SIDECAR_SUFFIX
    effective_uri, opts = _b.read_target(
        sidecar, use_cache, cache_dir or DEFAULT_CACHE_DIR, check_remote
    )
    try:
        with fsspec.open(effective_uri, "rb", **opts) as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return {}


# ---------------------------------------------------------------------------
# Public API — multi-file
# ---------------------------------------------------------------------------

def write_jsons(
    objs: dict[str, Any],
    dir_uri: str | Path,
    *,
    lineage: dict | None = None,
    ext: str = ".json",
) -> dict[str, str]:
    """Write each non-None value as ``{dir_uri}/{name}{ext}``.

    Parameters
    ----------
    objs
        Mapping of name -> JSON-serializable value. ``None`` values are skipped.
    dir_uri
        Directory-like container. Local path, ``file://`` URI, or ``s3://`` URI.
    lineage
        Optional dict applied to every file's sidecar; ``name`` is added per file.
    ext
        Filename extension, e.g. ``".json"`` (default) or ``".json.gz"``.

    Returns
    -------
    dict[str, str]
        ``{name: uri_written}``.
    """
    base = str(dir_uri).rstrip("/")
    return {
        name: write_json(
            obj,
            f"{base}/{name}{ext}",
            lineage=None if lineage is None else {**lineage, "name": name},
        )
        for name, obj in objs.items()
        if obj is not None
    }


def read_jsons(
    dir_uri: str | Path,
    names: list[str],
    *,
    ext: str = ".json",
    use_cache: bool = True,
    cache_dir: Path | None = None,
    check_remote: bool = True,
) -> dict[str, Any]:
    """Read ``{dir_uri}/{name}{ext}`` for each name. See :func:`read_json`."""
    base = str(dir_uri).rstrip("/")
    return {
        name: read_json(
            f"{base}/{name}{ext}",
            use_cache=use_cache,
            cache_dir=cache_dir,
            check_remote=check_remote,
        )
        for name in names
    }
