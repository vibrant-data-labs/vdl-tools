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

# uid is VDL's canonical cross-source id (CFT runs); uuid covers CB-only rows;
# bare "id" last — in player-cleaned network files it is positional, which the
# shape check below rejects.
ID_COLUMN_CANDIDATES = ["uid", "uuid", "id"]
TAXONOMY_COLUMN_PREFIXES = ["all_level", "taxonomy", "One Earth"]

# Baseline universes are typically multi-source (e.g. CFT = Crunchbase +
# Candid), so declared-source id validation applies to that source's rows
# when a Data Source column exists, else to a minimum share of all ids.
DATA_SOURCE_COLUMN = "Data Source"
DATA_SOURCE_LABELS = {"crunchbase": "Crunchbase", "nzi": "NZI"}
MIN_SOURCE_ID_SHARE = 0.3


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
    # convert_dates=False: read verbatim — pandas' date inference both
    # mangles lineage and can overflow on large numeric columns.
    return pd.read_json(path, convert_dates=False)


def _source_id_share(ids: pd.Series, source: str) -> float:
    sample = ids.dropna().astype(str)
    if sample.empty:
        return 0.0
    if source == "crunchbase":
        return sample.str.len().eq(36).mean()
    return sample.str.fullmatch(r"\d+").mean()  # nzi


def _find_id_column(df: pd.DataFrame, label: str, source: str) -> str:
    """First candidate column whose values plausibly hold entity ids.

    Rejects columns like the player-cleaned network's positional ``id``
    (0..n ints under a crunchbase source).
    """
    present = [c for c in ID_COLUMN_CANDIDATES if c in df.columns]
    for col in present:
        if _source_id_share(df[col].head(500), source) >= MIN_SOURCE_ID_SHARE:
            return col
    if not present:
        raise ValueError(
            f"{label}: no id column found (looked for {ID_COLUMN_CANDIDATES}); "
            f"columns present: {list(df.columns)[:20]}"
        )
    raise ValueError(
        f"{label}: none of {present} hold ids shaped like source "
        f"'{source}' — declared source likely does not match the baseline files"
    )


def _validate_source_ids(df: pd.DataFrame, id_col: str, source: str, label: str):
    """The declared source's rows must carry that source's id shape.

    Multi-source universes (CFT = Crunchbase + Candid) are expected: when a
    Data Source column exists, validate only the declared source's rows;
    otherwise require a minimum share across all ids.
    """
    ids = df[id_col]
    if DATA_SOURCE_COLUMN in df.columns:
        source_rows = df[df[DATA_SOURCE_COLUMN] == DATA_SOURCE_LABELS[source]]
        if len(source_rows):
            share = _source_id_share(source_rows[id_col], source)
            threshold, scope = 0.9, f"rows with {DATA_SOURCE_COLUMN}={DATA_SOURCE_LABELS[source]}"
        else:
            share, threshold, scope = 0.0, 0.9, "no rows from declared source"
    else:
        share = _source_id_share(ids, source)
        threshold, scope = MIN_SOURCE_ID_SHARE, "all ids"
    if share < threshold:
        raise ValueError(
            f"{label}: only {share:.0%} of ids ({scope}) match source "
            f"'{source}' shape — declared source likely does not match the "
            f"baseline files. Sample: {ids.dropna().astype(str).head(3).tolist()}"
        )


def _taxonomy_columns(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if any(c.startswith(p) for p in TAXONOMY_COLUMN_PREFIXES)
    ]


def _validate_taxonomy_columns(
    enriched: pd.DataFrame, nodes: pd.DataFrame, taxonomy_version: str
) -> dict[str, int]:
    """Taxonomy assignments may live on the enriched file or (as in CFT runs,
    where mapping is merged at network build) only on the nodes."""
    found = {
        "enriched": len(_taxonomy_columns(enriched)),
        "nodes": len(_taxonomy_columns(nodes)),
    }
    if not any(found.values()):
        raise ValueError(
            f"no taxonomy columns (prefixes {TAXONOMY_COLUMN_PREFIXES}) in "
            "either baseline file — cannot confirm taxonomy_version "
            f"'{taxonomy_version}' was applied"
        )
    return found


def _load_nodes(nodes_path: Path) -> pd.DataFrame:
    payload = json.loads(nodes_path.read_text())
    return pd.DataFrame(payload["nodes"]) if isinstance(payload, dict) else pd.DataFrame(payload)


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
    id_col = _find_id_column(enriched, "enriched file", run.source)
    _validate_source_ids(enriched, id_col, run.source, "enriched file")

    nodes = _load_nodes(nodes_path)
    tax_found = _validate_taxonomy_columns(enriched, nodes, run.taxonomy_version)
    nodes_id_col = _find_id_column(nodes, "network nodes file", run.source)
    universe_ids = set(nodes[nodes_id_col].dropna().astype(str))

    universe = enriched[enriched[id_col].astype(str).isin(universe_ids)].copy()
    if universe.empty:
        raise ValueError(
            "baseline universe is empty — the nodes file ids do not intersect "
            "the enriched file ids; check that both URIs come from the same run"
        )
    # Nodes absent from the enriched file mean the two URIs are different
    # vintages (e.g. enriched regenerated after the network was published).
    # The universe is the intersection; drift is recorded, not silently eaten.
    n_drift = len(universe_ids) - len(universe)
    if n_drift:
        logger.warning(
            "%d of %d network nodes are missing from the enriched file — the "
            "baseline URIs are different vintages; universe = intersection",
            n_drift, len(universe_ids),
        )

    # JSON, not parquet: enriched files carry mixed-type object columns
    # (lists and scalars in one column) that parquet rejects; same-format
    # passthrough is lossless.
    universe_path = config.results_dir() / "baseline_universe.json"
    universe.to_json(universe_path, orient="records")

    state.record_artifact("baseline_enriched", enriched_path, uri=run.enriched_uri)
    state.record_artifact("baseline_nodes", nodes_path, uri=run.network_nodes_uri)
    state.record_artifact("baseline_universe", universe_path)
    state.record_code_versions(engagement_repo=config.root)
    state.record_stage(
        "pin_baseline",
        run=f"{run.name}@{run.version}",
        source=run.source,
        n_enriched=len(enriched),
        n_nodes=len(universe_ids),
        n_universe=len(universe),
        n_filtered_out=len(enriched) - len(universe),
        n_nodes_missing_from_enriched=n_drift,
        id_col=id_col,
        taxonomy_cols_found=tax_found,
    )
    logger.info(
        "Baseline universe: %d of %d enriched orgs (%d filtered out by landscape)",
        len(universe), len(enriched), len(enriched) - len(universe),
    )
    return universe
