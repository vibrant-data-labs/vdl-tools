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

    logger.info(
        "enrich: %d rows, %d with taxonomy text — ledger updated in pipeline_state.json",
        len(summaries), int(summaries["text_for_taxonomy"].notna().sum()),
    )
    return summaries
