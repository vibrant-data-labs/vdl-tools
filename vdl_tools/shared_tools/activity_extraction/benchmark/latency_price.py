"""Latency and price, baseline (gpt-5.4-mini medium) against DeepSeek V4.1 Flash.

Wall-clock latency is the probe's own timing of each call (``latency_s``, kept in the
cached response's metadata): on the 300, both models were run at the same time with 8
parallel calls each, so their timings share conditions. On the 153, DeepSeek's timings
are from 2026-11-12 (4 parallel calls) and the baseline has only OpenAI's server
timestamps (``completed_at - created_at``, whole seconds, network excluded), so that
row is a rough comparison only. Price is list price per call from the probe: Baseten's
live catalog at run time, OpenAI's ``MODEL_DATA``.

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.latency_price

Reads the probe's ``probe_calls.csv`` files (sample300_deepseek, sample300_baseline,
cohort153_v2) and writes ``two_model_sample300/latency_price.csv``.
"""

import json

import pandas as pd

from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.benchmark import (
    JUDGES_DIR,
    PROBE_DIR,
    SAMPLE_FILE,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session

DEEPSEEK, BASELINE = "baseten:deepseek-v4.1-flash", "openai:gpt-5.4-mini@medium"


def server_seconds(full: dict) -> float | None:
    """OpenAI's own processing time for a stored response, or None."""
    if full.get("completed_at") and full.get("created_at"):
        return float(full["completed_at"]) - float(full["created_at"])
    return None


def summary_row(label: str, frame: pd.DataFrame, latency: str = "latency_s") -> dict:
    lat = frame[latency].dropna()
    return {
        "group": label,
        "orgs": len(frame),
        "latency median s": round(lat.median(), 1) if len(lat) else None,
        "latency p90 s": round(lat.quantile(0.9), 1) if len(lat) else None,
        "$ per 1k orgs": round(frame["cost_usd"].mean() * 1000, 2),
        "input tok (med)": int(frame["input_tokens"].median()),
        "output tok (med)": int(frame["output_tokens"].median()),
        "reasoning tok (med)": int(frame["reasoning_tokens"].median()),
    }


def main() -> None:
    strata = {o["org_id"]: o["stratum"] for o in json.loads(SAMPLE_FILE.read_text())["orgs"]}
    calls = pd.concat(
        [
            pd.read_csv(PROBE_DIR / "sample300_deepseek" / "probe_calls.csv"),
            pd.read_csv(PROBE_DIR / "sample300_baseline" / "probe_calls.csv"),
        ]
    )
    calls = calls[calls["route"].isin([DEEPSEEK, BASELINE]) & calls["valid"].astype(bool)]
    calls["group"] = calls["org_id"].map(strata)
    cohort = pd.read_csv(PROBE_DIR / "cohort153_v2" / "probe_calls.csv")
    cohort = cohort[(cohort["route"] == DEEPSEEK) & cohort["valid"].astype(bool)]

    with get_session() as s:
        stored = {
            o.org_id: o
            for o in probe.load_orgs(s, list(strata)) + probe.load_cohort(s, n_orgs=10_000)
        }
        s.rollback()

    rows = []
    for model, route in (("baseline", BASELINE), ("deepseek V4.1", DEEPSEEK)):
        g = calls[calls["route"] == route]
        rows.append({"model": model, **summary_row("300, all", g)})
        for stratum, gs in g.groupby("group"):
            rows.append({"model": model, **summary_row(f"300, {stratum}", gs)})
    table = pd.DataFrame(rows)
    print("Same-time wall-clock latency and list price, 300 orgs\n")
    print(table.to_string(index=False))

    # Paired per org: the same input, both models.
    wide = calls.pivot_table(index="org_id", columns="route", values="latency_s")
    ratio = (wide[DEEPSEEK] / wide[BASELINE]).dropna()
    print(
        f"\nPer org, DeepSeek / baseline latency: median ratio {ratio.median():.2f}, "
        f"DeepSeek slower on {(ratio > 1).mean():.0%} of {len(ratio)} orgs"
    )

    # The baseline's server timestamps on the same 300, against its wall-clock re-run.
    srv = pd.Series(
        {o: server_seconds(stored[o].baseline_full) for o in strata if o in stored}
    ).dropna()
    base_wall = calls[calls["route"] == BASELINE].set_index("org_id")["latency_s"]
    print(
        f"Baseline on the 300: stored server time median {srv.median():.1f} s, "
        f"wall-clock re-run median {base_wall.median():.1f} s"
    )

    # The 153: DeepSeek wall-clock from 2026-11-12, baseline server time only.
    srv153 = pd.Series(
        {o: server_seconds(stored[o].baseline_full) for o in cohort["org_id"] if o in stored}
    ).dropna()
    print(
        f"\n153 cohort (rough): DeepSeek wall-clock median {cohort['latency_s'].median():.1f} s "
        f"(p90 {cohort['latency_s'].quantile(0.9):.1f}); baseline server time median "
        f"{srv153.median():.1f} s (p90 {srv153.quantile(0.9):.1f}); DeepSeek "
        f"${cohort['cost_usd'].mean() * 1000:.2f} per 1k orgs"
    )
    table.to_csv(JUDGES_DIR / "two_model_sample300" / "latency_price.csv", index=False)


if __name__ == "__main__":
    main()
