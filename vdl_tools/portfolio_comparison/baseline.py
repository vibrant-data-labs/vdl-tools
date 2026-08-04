"""Pin, snapshot, validate the baseline run; build the baseline universe.

The baseline is a climate-landscape run pinned as two URIs (S3 or local/LFS):
the enriched file (full org list) and the network nodes file, whose only role
is recording which orgs survived the landscape's relevance filtering. The
baseline universe = enriched rows whose id appears in the nodes file. Snapshot
and content hashes make the engagement reproducible even if the landscape
files are later overwritten in place.
"""

import json
import shutil
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.state import PipelineState

ID_COLUMN_CANDIDATES = ["uuid", "id"]
TAXONOMY_COLUMN_PREFIXES = ["all_level", "taxonomy"]


def _fetch(uri: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if uri.startswith("s3://"):
        import boto3

        bucket, _, key = uri.removeprefix("s3://").partition("/")
        boto3.client("s3").download_file(bucket, key, str(dest))
    else:
        src = Path(uri).expanduser()
        if not src.exists():
            raise FileNotFoundError(f"baseline URI not found: {uri}")
        shutil.copyfile(src, dest)
    return dest


def _load_records(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_json(path)


def _find_id_column(df: pd.DataFrame, label: str) -> str:
    for col in ID_COLUMN_CANDIDATES:
        if col in df.columns:
            return col
    raise ValueError(
        f"{label}: no id column found (looked for {ID_COLUMN_CANDIDATES}); "
        f"columns present: {list(df.columns)[:20]}"
    )


def _validate_source_ids(ids: pd.Series, source: str, label: str):
    sample = ids.dropna().astype(str).head(100)
    if sample.empty:
        raise ValueError(f"{label}: id column is empty")
    if source == "crunchbase":
        looks_right = sample.str.len().eq(36).mean() > 0.9
        expected = "36-char Crunchbase UUIDs"
    else:  # nzi
        looks_right = sample.str.fullmatch(r"\d+").mean() > 0.9
        expected = "numeric NZI ids"
    if not looks_right:
        raise ValueError(
            f"{label}: id values do not look like {expected} — declared source "
            f"'{source}' likely does not match the baseline files. Sample: "
            f"{sample.head(3).tolist()}"
        )


def _validate_taxonomy_columns(df: pd.DataFrame, taxonomy_version: str):
    tax_cols = [
        c for c in df.columns
        if any(c.startswith(p) for p in TAXONOMY_COLUMN_PREFIXES)
    ]
    if not tax_cols:
        raise ValueError(
            "enriched file has no taxonomy columns (prefixes "
            f"{TAXONOMY_COLUMN_PREFIXES}) — cannot confirm taxonomy_version "
            f"'{taxonomy_version}' was applied"
        )
    return tax_cols


def _nodes_id_set(nodes_path: Path, source: str) -> set[str]:
    payload = json.loads(nodes_path.read_text())
    nodes = pd.DataFrame(payload["nodes"]) if isinstance(payload, dict) else pd.DataFrame(payload)
    id_col = _find_id_column(nodes, "network nodes file")
    ids = nodes[id_col].dropna().astype(str)
    _validate_source_ids(ids, source, "network nodes file")
    return set(ids)


def pin_baseline(config: EngagementConfig) -> pd.DataFrame:
    """Snapshot + validate the baseline and write the universe artifact.

    Returns the baseline universe DataFrame (enriched rows ∩ nodes ids).
    """
    state = PipelineState(config.root)
    run = config.baseline_run
    snap_dir = config.results_dir() / "baseline"

    enriched_path = _fetch(run.enriched_uri, snap_dir / Path(run.enriched_uri).name)
    nodes_path = _fetch(run.network_nodes_uri, snap_dir / Path(run.network_nodes_uri).name)
    logger.info("Baseline snapshotted to %s", snap_dir)

    enriched = _load_records(enriched_path)
    id_col = _find_id_column(enriched, "enriched file")
    _validate_source_ids(enriched[id_col], run.source, "enriched file")
    tax_cols = _validate_taxonomy_columns(enriched, run.taxonomy_version)

    universe_ids = _nodes_id_set(nodes_path, run.source)
    universe = enriched[enriched[id_col].astype(str).isin(universe_ids)].copy()
    if universe.empty:
        raise ValueError(
            "baseline universe is empty — the nodes file ids do not intersect "
            "the enriched file ids; check that both URIs come from the same run"
        )

    universe_path = config.results_dir() / "baseline_universe.parquet"
    universe.to_parquet(universe_path, index=False)

    state.record_artifact("baseline_enriched", enriched_path, uri=run.enriched_uri)
    state.record_artifact("baseline_nodes", nodes_path, uri=run.network_nodes_uri)
    state.record_artifact("baseline_universe", universe_path)
    state.record_code_versions(engagement_repo=config.root)
    state.record_stage(
        "pin_baseline",
        run=f"{run.name}@{run.version}",
        source=run.source,
        n_enriched=len(enriched),
        n_universe=len(universe),
        n_filtered_out=len(enriched) - len(universe),
        id_col=id_col,
        n_taxonomy_cols=len(tax_cols),
    )
    logger.info(
        "Baseline universe: %d of %d enriched orgs (%d filtered out by landscape)",
        len(universe), len(enriched), len(enriched) - len(universe),
    )
    return universe
