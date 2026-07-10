"""Shared S3 + local-filecache plumbing for the ``*_cache`` modules.

This is the storage-agnostic core extracted from :mod:`parquet_cache`: URI
detection, AWS credential resolution, the ETag-validated local filecache used
for ``s3://`` reads, and bucket/parent-dir creation on write. Both
:mod:`parquet_cache` and :mod:`json_cache` build on it so the credential and
caching behaviour never drifts between them.

Not a public API — import the ``parquet_cache`` / ``json_cache`` wrappers
instead.
"""

from __future__ import annotations

import os
from pathlib import Path

from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.s3_tools import get_s3_client, create_bucket, bucket_exists


# Root for all vdl-tools caches; each module gets its own subdir underneath.
DEFAULT_CACHE_ROOT = Path(
    os.environ.get("VDL_CACHE_DIR", Path.home() / ".cache" / "vdl-tools")
)


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def bucket_of(uri: str) -> str:
    """``s3://bucket/key`` -> ``bucket``."""
    return uri.split("/")[2]


def s3_creds() -> dict:
    """Read AWS creds from config.ini ``[aws]``. Empty dict -> boto3 default chain."""
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


def read_target(
    uri: str,
    use_cache: bool,
    cache_dir: Path,
    check_remote: bool,
) -> tuple[str, dict]:
    """Return ``(effective_uri, fsspec_opts)`` for reading from ``uri``.

    Local paths read directly. ``s3://`` paths with ``use_cache`` go through a
    ``filecache`` that HEAD-checks the remote ETag on every open (``check_remote``),
    so a concurrent writer's new version invalidates the local copy.
    """
    if not is_s3(uri):
        return uri, {}
    if not use_cache:
        return uri, {"s3": s3_creds()}
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return f"filecache::{uri}", {
        "filecache": {
            "cache_storage": str(cache_dir),
            "check_files": check_remote,  # ETag HEAD on every open
            "expiry_time": None,          # no TTL — rely on ETag check
            "same_names": False,
        },
        "s3": s3_creds(),
    }


def write_target(uri: str) -> dict:
    """Prepare ``uri`` for writing and return fsspec opts.

    For ``s3://`` targets, create the bucket if it is missing. For local
    targets, create the parent directory. Returns the fsspec ``open`` kwargs.
    """
    if is_s3(uri):
        bucket = bucket_of(uri)
        if not bucket_exists(bucket):
            create_bucket(get_s3_client(), bucket)
        return {"s3": s3_creds()}
    # Local target: ensure the parent dir exists. Strip a ``file://`` scheme
    # first — ``Path("file:///a/b")`` is a *relative* path rooted at ``file:``,
    # so ``.parent.mkdir`` would spray a junk ``file:`` tree into the cwd
    # instead of creating the real parent.
    local_path = uri[len("file://"):] if uri.startswith("file://") else uri
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    return {}


def target_exists(uri: str) -> bool:
    """Check if the target exists."""
    if is_s3(uri):
        return bucket_exists(bucket_of(uri))
    return Path(uri).exists()
