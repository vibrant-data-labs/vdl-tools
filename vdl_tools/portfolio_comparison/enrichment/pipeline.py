"""Phase 2 orchestrator — one recorded invocation for the whole pipeline.

Runs the enrichment stages in order against the finalized ID Mapping File
and records every stage in the engagement's ``pipeline_state.json`` (the
run ledger): counts, artifact hashes, durations. Agents and humans invoke
THIS, not ad-hoc stage sequences — recovery behavior (scrape self-heal
etc.) lives inside the stages so a rerun reproduces it.

Stages so far: acquire → scrape → summarize. Taxonomy and geocoding join
the sequence as they land (spec: phase2-enrichment.md).
"""

import hashlib
import time
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.finalize import FINAL_BASENAME
from vdl_tools.portfolio_comparison.state import PipelineState
from vdl_tools.portfolio_comparison.enrichment.acquire import (
    ACQUIRED_BASENAME,
    acquire_records,
)
from vdl_tools.portfolio_comparison.enrichment.scrape import (
    SCRAPED_BASENAME,
    scrape_texts,
)
from vdl_tools.portfolio_comparison.enrichment.summarize import (
    SUMMARIES_BASENAME,
    build_general_summaries,
)
from vdl_tools.portfolio_comparison.enrichment.taxonomy_geo import (
    GEOCODED_BASENAME,
    TAXONOMY_BASENAME,
    geocode_rows,
    map_taxonomy,
)

ENRICHED_BASENAME = "enriched_portfolio"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def run_enrich(engagement_root: str | Path) -> pd.DataFrame:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    state = PipelineState(config.root)

    final_path = results_dir / f"{FINAL_BASENAME}.parquet"
    if not final_path.exists():
        raise RuntimeError("no finalized ID Mapping File — run `finalize` first")
    final = pd.read_parquet(final_path)

    t0 = time.time()
    acquired = acquire_records(final, results_dir)
    src_cols = [c for c in acquired.columns
                if c.startswith(("cb_", "nzi_", "gt_")) and c not in ("cb_id", "nzi_id")]
    state.record_stage(
        "enrich_acquire",
        n_rows=len(acquired),
        n_with_source_record=int(acquired[src_cols].notna().any(axis=1).sum()),
        input_sha256=_sha(final_path),
        artifact_sha256=_sha(results_dir / f"{ACQUIRED_BASENAME}.parquet"),
        seconds=int(time.time() - t0),
    )

    t0 = time.time()
    scraped = scrape_texts(acquired, results_dir)
    state.record_stage(
        "enrich_scrape",
        n_rows=len(scraped),
        quality=scraped["text_quality"].value_counts(dropna=False).to_dict(),
        artifact_sha256=_sha(results_dir / f"{SCRAPED_BASENAME}.parquet"),
        seconds=int(time.time() - t0),
    )

    t0 = time.time()
    summaries = build_general_summaries(acquired, scraped, results_dir)
    state.record_stage(
        "enrich_summarize",
        n_rows=len(summaries),
        n_with_taxonomy_text=int(summaries["text_for_taxonomy"].notna().sum()),
        artifact_sha256=_sha(results_dir / f"{SUMMARIES_BASENAME}.parquet"),
        seconds=int(time.time() - t0),
    )

    taxonomy_path = config.enrichment.get("taxonomy_path")
    if taxonomy_path:
        t0 = time.time()
        recovery = bool(config.enrichment.get("recovery"))
        taxonomy = map_taxonomy(
            summaries, results_dir, taxonomy_path,
            recovery=recovery,
            taxonomy_model=config.enrichment.get("taxonomy_model"),
            recovery_model=config.enrichment.get("recovery_model"),
        )
        matched = taxonomy["one_earth_category"].notna() & (
            taxonomy["one_earth_category"] != "NoMatch"
        )
        state.record_stage(
            "enrich_taxonomy",
            n_rows=len(taxonomy),
            n_matched=int(matched.sum()),
            recovery=recovery,
            taxonomy_path=str(taxonomy_path),
            artifact_sha256=_sha(results_dir / f"{TAXONOMY_BASENAME}.parquet"),
            seconds=int(time.time() - t0),
        )
    else:
        taxonomy = None
        logger.warning(
            "enrich: no enrichment.taxonomy_path in engagement.yaml — "
            "taxonomy stage skipped"
        )

    t0 = time.time()
    geocoded = geocode_rows(acquired, results_dir)
    state.record_stage(
        "enrich_geocode",
        n_rows=len(geocoded),
        n_geocoded=int(geocoded["Latitude"].notna().sum())
        if "Latitude" in geocoded.columns else 0,
        artifact_sha256=_sha(results_dir / f"{GEOCODED_BASENAME}.parquet"),
        seconds=int(time.time() - t0),
    )

    # The deliverable: one row per customer_row_id, everything joined.
    enriched = final.merge(
        acquired.drop(columns=[c for c in acquired.columns
                               if c in final.columns and c != "customer_row_id"]),
        on="customer_row_id", how="left",
    ).merge(
        summaries.drop(columns=["customer_name"], errors="ignore"),
        on="customer_row_id", how="left",
    ).merge(geocoded, on="customer_row_id", how="left")
    if taxonomy is not None:
        enriched = enriched.merge(taxonomy, on="customer_row_id", how="left")
    enriched = enriched.astype(object).where(pd.notna(enriched), pd.NA)
    enriched.to_parquet(results_dir / f"{ENRICHED_BASENAME}.parquet", index=False)
    enriched.to_csv(results_dir / f"{ENRICHED_BASENAME}.csv", index=False)

    s3_uri = config.enrichment.get("s3_results_uri")
    if s3_uri:
        published = _publish_to_s3(
            [results_dir / f"{ENRICHED_BASENAME}.parquet",
             results_dir / f"{ENRICHED_BASENAME}.csv",
             results_dir / "id_mapping_final.parquet"],
            s3_uri,
        )
        state.record_artifact(
            "enriched_portfolio_s3",
            results_dir / f"{ENRICHED_BASENAME}.parquet",  # hash the local copy
            s3_uris=published,
        )

    logger.info(
        "enrich: %d rows -> %s (ledger updated in pipeline_state.json)",
        len(enriched), results_dir / f"{ENRICHED_BASENAME}.parquet",
    )
    return enriched


def _publish_to_s3(paths: list[Path], s3_uri: str) -> list[str]:
    """Upload artifacts under {s3_uri}/{filename}; returns uploaded URIs.
    Credentials come from config.ini [aws] (same as every other S3 use)."""
    import boto3

    from vdl_tools.shared_tools.tools.config_utils import get_configuration

    aws = dict(get_configuration()["aws"])
    client = boto3.client(
        "s3",
        aws_access_key_id=aws.get("access_key_id") or aws.get("aws_access_key_id"),
        aws_secret_access_key=aws.get("secret_access_key") or aws.get("aws_secret_access_key"),
    )
    bucket, _, prefix = s3_uri.removeprefix("s3://").partition("/")
    uris = []
    for path in paths:
        key = f"{prefix.rstrip('/')}/{path.name}" if prefix else path.name
        client.upload_file(str(path), bucket, key)
        uris.append(f"s3://{bucket}/{key}")
        logger.info("published %s", uris[-1])
    return uris
