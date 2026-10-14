"""Build and cache the per-organization index a spec describes.

Each ``Source`` is read once and folded into one frame keyed by ``uid``:
frames outer-merge (so an org present only downstream, or only in the cache,
still gets a row), uid sets become boolean flags, maps become columns. The
result is cached as parquet under ``spec.index_dir`` with a fingerprint per
source (file mtime and size, S3 lineage footer, or prompt-cache row count and
last update) so the next run rebuilds only when something changed.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.why_not import loaders as ld
from vdl_tools.shared_tools.why_not.spec import PipelineSpec, Source

ENGINE_VERSION = 1


@dataclass
class Artifact:
    key: str
    label: str
    uri: str | None
    status: str  # ok | missing | error
    rows: int | None = None
    mtime: str | None = None   # file mtime, S3 lineage created_at, or cache last update (date)
    note: str | None = None


def in_repo(spec: PipelineSpec, fn, *args, **kwargs):
    """Run ``fn`` with cwd pinned to the repo root (project_config reads paths.ini from cwd)."""
    cwd = os.getcwd()
    os.chdir(spec.repo_root)
    try:
        return fn(*args, **kwargs)
    finally:
        os.chdir(cwd)


# ---------------------------------------------------------------------------
# Fingerprints
# ---------------------------------------------------------------------------

def _fingerprint_one(src: Source) -> list:
    try:
        if src.kind == "custom" and src.fingerprint is not None:
            return [src.uri, *src.fingerprint()]
        if src.kind == "relevance_cache":
            return [src.uri, *ld.relevance_snapshot(src.prompt_name, src.model)]
        uri = src.uri if ld.readable(src.uri) else src.fallback
        if ld.is_s3(uri) or (uri and str(uri).endswith(".parquet") and ld.local_exists(uri)):
            lin = ld.lineage_of(uri)
            if lin:
                return [uri, lin.get("row_count"), lin.get("created_at")]
        if ld.local_exists(uri):
            st = Path(str(uri)).stat()
            return [str(uri), int(st.st_mtime), st.st_size]
        return [str(uri), None, None]
    except Exception as exc:  # noqa: BLE001 - an unreachable source is "unknown", not "changed"
        return [str(src.uri), "unreachable", type(exc).__name__]


def fingerprint(sources: list[Source]) -> dict[str, list]:
    return {s.key: _fingerprint_one(s) for s in sources}


# ---------------------------------------------------------------------------
# Loading one source
# ---------------------------------------------------------------------------

def _read(src: Source, uri):
    if src.kind == "frame":
        return ld.read_frame(uri, src.columns)
    if src.kind == "uid_set":
        return ld.read_uid_set(src, uri)
    if src.kind == "uid_map":
        return ld.read_uid_map(uri)
    if src.kind == "relevance_cache":
        if uri == src.uri:
            return ld.read_relevance_cache(src.prompt_name, src.model, src.membership or "has_relevance_text")
        return ld.read_frame(uri, src.columns)  # a legacy file standing in for the cache
    if src.kind == "custom":
        return src.load()
    raise ValueError(f"unknown source kind {src.kind!r} for {src.key}")


def load_source(src: Source, log) -> tuple[object, Artifact, dict]:
    """Read ``uri`` then ``fallback``. Returns (object or None, provenance, lineage)."""
    last_error = None
    candidates = [src.uri, src.fallback] if src.kind not in ("custom",) else [src.uri or "custom"]
    for attempt, uri in enumerate(c for c in candidates if c):
        is_file = src.kind != "custom" and not (src.kind == "relevance_cache" and uri == src.uri)
        if is_file and not ld.readable(uri):
            log(f"  {src.key}: missing ({uri})")
            continue
        try:
            log(f"  {src.key}: reading {uri}")
            obj = _read(src, uri)
            lineage = ld.lineage_of(uri) if (is_file or (src.kind == "custom" and ld.readable(uri))) else {}
            if src.kind == "relevance_cache" and uri == src.uri:
                snap = ld.relevance_snapshot(src.prompt_name, src.model)
                lineage = {"row_count": snap[0], "created_at": snap[1]}
            rows = len(obj) if hasattr(obj, "__len__") else None
            mtime = str(lineage.get("created_at") or "")[:10] or None
            note = f"fallback after {src.uri} failed" if attempt else None
            return obj, Artifact(src.key, src.label, str(uri), "ok", rows=rows, mtime=mtime, note=note), lineage
        except Exception as exc:  # noqa: BLE001 - one bad artifact must not kill the index
            last_error = f"{type(exc).__name__}: {exc}"[:300]
            log(f"  {src.key}: ERROR {last_error}")
    status = "error" if last_error else "missing"
    return None, Artifact(src.key, src.label, src.uri, status, note=last_error), {}


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _fold(idx: pd.DataFrame | None, src: Source, obj, kind: str) -> pd.DataFrame:
    if kind == "frame":
        df = obj.copy()
        if "uid" not in df.columns:
            raise ValueError(f"{src.key}: frame has no 'uid' column after renaming")
        df["uid"] = df["uid"].astype(str)
        df = df.drop_duplicates("uid")
        for col in src.snip:
            if col in df.columns:
                df[col + "_len"] = ld.textlen(df[col])
                df[col] = ld.snip(df[col])
        if src.membership:
            df[src.membership] = True
        if idx is None:
            return df
        overlap = [c for c in df.columns if c in idx.columns and c != "uid"]
        if overlap:
            # a later frame never overwrites an earlier one; it fills gaps (orgs absent earlier)
            merged = idx.merge(df, on="uid", how="outer", suffixes=("", "__new"))
            for col in overlap:
                merged[col] = merged[col].where(merged[col].notna(), merged[col + "__new"])
            return merged.drop(columns=[c + "__new" for c in overlap])
        return idx.merge(df, on="uid", how="outer")
    if kind == "uid_set":
        idx[src.flag] = idx["uid"].isin(obj)
        return idx
    if kind == "uid_map":
        values = idx["uid"].map(obj)
        idx[src.column] = values.map(src.value_map) if src.value_map else values
        return idx
    raise ValueError(f"cannot fold kind {kind!r}")


def default_lineage_summary(lineage_by_key: dict, pull_key: str = "pull", meta_key: str = "meta") -> dict:
    """pull / enrichment dates and whether the pull is newer than the last enrichment."""
    pull = lineage_by_key.get(pull_key, {})
    meta = lineage_by_key.get(meta_key, {})
    out = {
        "pull_created_at": pull.get("created_at"),
        "meta_created_at": (meta.get("meta_lineage") or {}).get("created_at") or meta.get("created_at"),
    }
    p, m = out["pull_created_at"], out["meta_created_at"]
    out["enrichment_behind_pull"] = bool(p and m and str(p) > str(m))
    return out


def add_relevance_text_source(idx: pd.DataFrame) -> pd.DataFrame:
    """Did the classifier see the pull description or scraped text? (prefix comparison)"""
    if "relevance_text" not in idx or "description" not in idx:
        return idx
    head = lambda s: s.map(lambda v: "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)[:60].strip())  # noqa: E731
    same = (head(idx["description"]) != "") & (head(idx["description"]) == head(idx["relevance_text"]))
    has = idx["relevance_text"].notna()
    idx["relevance_text_source"] = None
    idx.loc[has & same, "relevance_text_source"] = "description"
    idx.loc[has & ~same, "relevance_text_source"] = "website summary or other text"
    return idx


def build_index(spec: PipelineSpec, log=print) -> tuple[pd.DataFrame, dict]:
    sources = in_repo(spec, spec.sources)
    log(f"Building why_not index for {spec.title}")
    idx: pd.DataFrame | None = None
    arts: list[Artifact] = []
    lineage_by_key: dict[str, dict] = {}
    membership_flags: list[str] = []
    for src in sources:
        obj, art, lineage = in_repo(spec, load_source, src, log)
        arts.append(art)
        lineage_by_key[src.key] = lineage
        kind = src.as_kind if src.kind == "custom" else ("frame" if src.kind == "relevance_cache" else src.kind)
        if obj is None:
            if src.required:
                raise RuntimeError(f"required source {src.key!r} could not be read ({src.uri}; {art.note})")
            # keep the schema stable so stage rules can test for None
            if idx is not None:
                if kind == "uid_set" and src.flag:
                    idx[src.flag] = None
                if kind == "uid_map" and src.column:
                    idx[src.column] = None
                if src.membership:
                    idx[src.membership] = None
            continue
        if idx is None and kind != "frame":
            raise RuntimeError(f"the first source must be a frame; got {src.key} ({kind})")
        idx = _fold(idx, src, obj, kind)
        if src.membership and src.membership not in membership_flags:
            membership_flags.append(src.membership)
    if idx is None:
        raise RuntimeError("no source could be read")
    for col in membership_flags:
        idx[col] = ld.flag(idx[col])
    if spec.finalize is not None:
        idx = in_repo(spec, spec.finalize, idx, lineage_by_key)
    idx = add_relevance_text_source(idx)

    # lookup helpers
    names = [c for c in spec.name_columns if c in idx.columns]
    idx["name_norm"] = idx[names[0]].map(ld.normalize_name) if names else ""
    idx["alt_norm"] = idx[names[1]].map(ld.normalize_name) if len(names) > 1 else ""
    idx["domain"] = ""
    for col in [c for c in spec.website_columns if c in idx.columns]:
        d = idx[col].map(ld.normalize_domain)
        idx["domain"] = idx["domain"].where(idx["domain"] != "", d)
    idx["linkedin_slug"] = ""
    for col in [c for c in spec.linkedin_columns if c in idx.columns]:
        s = idx[col].map(ld.linkedin_slug)
        idx["linkedin_slug"] = idx["linkedin_slug"].where(idx["linkedin_slug"] != "", s)
    for col in [c for c in spec.int_columns if c in idx.columns]:
        idx[col] = pd.to_numeric(idx[col], errors="coerce").astype("Int64")
    for col in [c for c in spec.float_columns if c in idx.columns]:
        idx[col] = pd.to_numeric(idx[col], errors="coerce")
    # parquet cannot hold mixed bool/None or list cells
    for col in idx.columns:
        if idx[col].dtype == object:
            sample = idx[col].dropna()
            if len(sample) and sample.map(lambda v: isinstance(v, (list, tuple))).any():
                idx[col] = idx[col].map(lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple)) else v)
            elif len(sample) and sample.map(lambda v: isinstance(v, bool)).all():
                idx[col] = idx[col].map(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else bool(v)).astype(object)
    idx = idx.reset_index(drop=True)

    lineage = (spec.lineage or default_lineage_summary)(lineage_by_key)
    counts = spec.counts(idx) if spec.counts else {c: int(idx[c].sum()) for c in membership_flags}
    meta = {
        "pipeline": spec.name, "title": spec.title,
        "index_version": f"{ENGINE_VERSION}.{spec.version}",
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(len(idx)), "counts": counts, "lineage": lineage,
        "fingerprint": in_repo(spec, fingerprint, sources),
        "artifacts": [asdict(a) for a in arts],
    }
    log(f"Index: {meta['rows']} orgs; counts {counts}; lineage {lineage}")
    return idx, meta


def save_index(spec: PipelineSpec, idx: pd.DataFrame, meta: dict) -> None:
    spec.index_dir.mkdir(parents=True, exist_ok=True)
    idx.to_parquet(spec.index_path, index=False)
    spec.index_meta_path.write_text(json.dumps(meta, indent=1, default=str))


def is_stale(spec: PipelineSpec, meta: dict) -> str | None:
    """Reason to rebuild, or None when every source fingerprint still matches."""
    if meta.get("index_version") != f"{ENGINE_VERSION}.{spec.version}":
        return "index version changed"
    try:
        current = in_repo(spec, lambda: fingerprint(spec.sources()))
    except Exception as exc:  # noqa: BLE001
        return f"could not resolve sources: {exc}"
    cached = meta.get("fingerprint", {})
    changed = [k for k, v in current.items()
               if cached.get(k) != v and v[1] != "unreachable" and (cached.get(k) or [None, None])[1] != "unreachable"]
    return f"sources changed: {', '.join(changed)}" if changed else None


def load_index(spec: PipelineSpec, rebuild: bool = False, log=print) -> tuple[pd.DataFrame, dict]:
    if not rebuild and spec.index_path.exists() and spec.index_meta_path.exists():
        meta = json.loads(spec.index_meta_path.read_text())
        reason = is_stale(spec, meta)
        if reason is None:
            return pd.read_parquet(spec.index_path), meta
        log(f"Rebuilding why_not index: {reason}")
    idx, meta = build_index(spec, log=log)
    save_index(spec, idx, meta)
    return idx, meta
