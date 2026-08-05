"""Stage runners — the glue the engagement skills and CLI call into.

Each runner loads ``engagement.yaml`` from the engagement repo root, does one
stage, records it in pipeline state, and writes its artifacts under
``data/results/``.
"""

import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison import baseline as baseline_mod
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.intake.normalize import (
    normalize_domain,
    normalize_ein,
)
from vdl_tools.portfolio_comparison.intake import profile_inputs as pi
from vdl_tools.portfolio_comparison.matching.queue import (
    build_review_queue,
    match_rate_report,
    replay_decisions,
    save_id_mapping,
)
from vdl_tools.portfolio_comparison.matching.universe import UniverseIndex, run_tier1
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS
from vdl_tools.portfolio_comparison.state import PipelineState

# Which input slot a file arrived in determines its default entity type.
INPUT_ENTITY_TYPES = {"companies": "for_profit", "nonprofits": "nonprofit"}


def _load_config(engagement_root: str | Path) -> EngagementConfig:
    return EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")


def _read_customer_file(path: Path) -> pd.DataFrame:
    if path.suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    # Excel year headers arrive as mixed int/str ('2021' vs 2022) — stringify
    # so mappings and JSON serialization are stable.
    df.columns = [str(c) for c in df.columns]
    return df


def run_pin_baseline(engagement_root: str | Path) -> pd.DataFrame:
    config = _load_config(engagement_root)
    return baseline_mod.pin_baseline(config)


def run_intake(engagement_root: str | Path) -> dict:
    config = _load_config(engagement_root)
    config.validate_inputs_exist()
    state = PipelineState(config.root)

    column_overrides = config.intake.get("column_overrides", {})
    dispo_overrides = config.intake.get("disposition_value_overrides", {})

    profiles = []
    for label in config.inputs:
        entity_type = INPUT_ENTITY_TYPES.get(label, "unknown")
        df = _read_customer_file(config.input_path(label))
        mapping = pi.propose_column_mapping(df.columns)
        mapping.update(column_overrides.get(label, {}))
        profiles.append(profile := pi.profile_file(
            df, label, entity_type,
            column_mapping=mapping,
            disposition_overrides=dispo_overrides,
        ))
        if profile.get("blocking"):
            logger.warning("intake blocking issue in %s: %s", label, profile["blocking"])

    out = pi.write_intake_profile(profiles, config.results_dir())
    state.record_artifact("intake_profile", out)
    state.record_stage(
        "intake",
        status="completed",
        n_files=len(profiles),
        n_rows=sum(p["n_rows"] for p in profiles),
    )
    payload = json.loads(out.read_text())
    logger.info("intake pre-flight: %s", payload["preflight"])
    return payload


def _customer_rows(config: EngagementConfig, profiles: dict) -> pd.DataFrame:
    """Build normalized customer rows from the confirmed column mappings."""
    dispo_overrides = config.intake.get("disposition_value_overrides", {})
    ein_ignore = config.intake.get("ein_ignore")

    frames = []
    for profile in profiles["files"]:
        label = profile["file"]
        mapping = profile["column_mapping"]
        inverse = {v: k for k, v in mapping.items() if v != "passthrough"}
        df = _read_customer_file(config.input_path(label))

        name_col = inverse["name"]
        url_col = inverse.get("url")
        ein_col = inverse.get("ein")
        dispo_col = inverse.get("disposition")

        eins = df[ein_col].map(normalize_ein) if ein_col else None
        if eins is not None and ein_ignore and ein_ignore["column"] in df.columns:
            # Rows whose EIN belongs to someone else (e.g. fiscal sponsor):
            # the EIN is context, not identity — blank it for matching.
            not_own = df[ein_ignore["column"]].astype(str).str.contains(
                ein_ignore["pattern"], case=False, na=False
            )
            n_ignored = int((not_own & eins.ne("")).sum())
            if n_ignored:
                logger.info(
                    "%s: ignoring %d EINs for identity (%s ~ %r)",
                    label, n_ignored, ein_ignore["column"], ein_ignore["pattern"],
                )
            eins = eins.mask(not_own, "")

        rows = pd.DataFrame({
            "customer_name": df[name_col],
            "customer_url": df[url_col] if url_col else None,
            "customer_ein": eins,
        })
        rows["customer_row_id"] = [
            pi.make_row_id(label, n, u or "", i)
            for i, (n, u) in enumerate(zip(rows["customer_name"], rows["customer_url"]))
        ]
        rows["entity_type"] = profile["default_entity_type"]
        if dispo_col is not None:
            rows["disposition"] = pi.map_dispositions(df[dispo_col], dispo_overrides)
        else:
            rows["disposition"] = "invested"

        n_excluded = int((rows["disposition"] == "exclude").sum())
        if n_excluded:
            logger.info("%s: excluding %d rows per disposition overrides", label, n_excluded)
        rows = rows[rows["disposition"] != "exclude"]
        rows = rows[rows["customer_name"].fillna("").astype(str).str.strip() != ""]
        # Summary/junk rows the disposition overrides can't catch (e.g. a
        # TOTAL row whose disposition cell is blank).
        for pattern in config.intake.get("exclude_name_patterns", []):
            hit = rows["customer_name"].astype(str).str.contains(pattern, case=False, regex=True)
            if hit.any():
                logger.info("%s: excluding %d rows matching %r", label, int(hit.sum()), pattern)
                rows = rows[~hit]
        frames.append(rows)
    return pd.concat(frames, ignore_index=True)


def run_match(engagement_root: str | Path) -> pd.DataFrame:
    """Tier 1 + nonprofit EIN lane. Tier 2 (source API) is next-sprint work."""
    config = _load_config(engagement_root)
    state = PipelineState(config.root)
    results_dir = config.results_dir()

    profile_path = results_dir / "intake_profile.json"
    if not profile_path.exists():
        raise FileNotFoundError("run intake first — intake_profile.json not found")
    universe_path = results_dir / "baseline_universe.json"
    if not universe_path.exists():
        raise FileNotFoundError("run pin-baseline first — baseline_universe.json not found")

    profiles = json.loads(profile_path.read_text())
    rows = _customer_rows(config, profiles)

    enriched = baseline_mod._load_records(
        results_dir / "baseline" / Path(config.baseline_run.enriched_uri).name
    )
    id_col = baseline_mod._find_id_column(
        enriched, "enriched file", config.baseline_run.source
    )
    universe_ids = set(
        pd.read_json(universe_path, convert_dates=False)[id_col].astype(str)
    )
    index = UniverseIndex(enriched, universe_ids, id_col=id_col)

    id_mapping, candidates = run_tier1(rows, index)

    # Nonprofit EIN lane — optional at this stage: needs GT datamart access.
    nonprofit_rows = rows[rows["entity_type"] == "nonprofit"]
    if not nonprofit_rows.empty:
        try:
            from vdl_tools.portfolio_comparison.matching.nonprofit import match_by_ein

            for row_id, cand in match_by_ein(nonprofit_rows).items():
                mask = id_mapping["customer_row_id"] == row_id
                id_mapping.loc[mask, ["matched_id", "matched_name", "matched_url"]] = (
                    cand.matched_id, cand.matched_name, cand.matched_url,
                )
                id_mapping.loc[mask, ["match_method", "confidence", "status", "decided_by"]] = (
                    cand.method, cand.score, "auto_matched", "auto",
                )
        except Exception as exc:  # no DB access, client missing, etc.
            logger.warning("nonprofit EIN lane skipped: %s", exc)

    id_mapping = id_mapping[ID_MAPPING_COLUMNS]
    # Reruns rebuild from scratch; recorded human decisions always win.
    id_mapping = replay_decisions(id_mapping, results_dir)

    # Tier 2: source-API pre-research BEFORE any human/web research (higher
    # recall than the baseline). Decided rows are excluded by construction.
    try:
        from vdl_tools.portfolio_comparison.matching.source_adapter import get_source_client
        from vdl_tools.portfolio_comparison.matching.tier2 import run_tier2

        client = get_source_client(
            config.baseline_run.source,
            cache_path=results_dir / "source_search_cache.json",
        )
        id_mapping, candidates, _ = run_tier2(id_mapping, client, candidates)
    except NotImplementedError as exc:
        logger.warning("Tier 2 skipped: %s", exc)
    except Exception as exc:
        logger.warning("Tier 2 failed, continuing without it: %s", exc)

    # Nonprofit identity lane (EIN-less + fiscal-sponsor rows): GT datamart
    # ranked identity search — same source-API-first ordering as Tier 2.
    np_pending = id_mapping[
        (id_mapping["entity_type"] == "nonprofit")
        & (id_mapping["status"].isna() | (id_mapping["status"] == "needs_review"))
    ]
    if not np_pending.empty:
        try:
            from vdl_tools.portfolio_comparison.matching.nonprofit import (
                apply_identity_matches,
                match_identity,
            )
            from vdl_tools.portfolio_comparison.matching.tier2 import _dedupe

            gt_matches = match_identity(np_pending, universe_ids=universe_ids)
            for row_id, cands in gt_matches.items():
                candidates[row_id] = _dedupe(candidates.get(row_id, []), cands)
            id_mapping = apply_identity_matches(id_mapping, gt_matches)
            logger.info(
                "GT identity lane: %d of %d pending nonprofit rows got candidates",
                len(gt_matches), len(np_pending),
            )
        except Exception as exc:
            logger.warning("nonprofit identity lane skipped: %s", exc)

    mapping_path = results_dir / "id_mapping.parquet"
    save_id_mapping(id_mapping, results_dir)

    queue = build_review_queue(id_mapping, candidates, results_dir=results_dir)
    queue_path = results_dir / "review_queue.json"
    queue.to_json(queue_path, orient="records", indent=2)

    report = match_rate_report(id_mapping)
    logger.info("match rates:\n%s", report.to_string(index=False))

    state.record_artifact("id_mapping", mapping_path)
    state.record_stage(
        "match",
        status="completed",
        n_rows=len(id_mapping),
        n_auto=int((id_mapping["status"] == "auto_matched").sum()),
        n_review=int((id_mapping["status"] == "needs_review").sum()),
        n_unresolved=int(id_mapping["status"].isna().sum()),
    )
    return id_mapping


def run_status(engagement_root: str | Path) -> str:
    return PipelineState(engagement_root).render_status()
