"""
Reports on the open-weight benchmark, rebuilt from the prompt cache.
====================================================================

``model_probe`` runs the extractions and ``model_judges`` the judges; every call is
cached in Postgres. The modules here read those rows back (only ``pooled_misses
--run`` makes calls, and a rerun reads its own rows), so every result file below can
be rebuilt at no cost. Results go to the gitignored ``data_out/`` folders; the
benchmark's results and decisions are in ``docs/ACTIVITY_MODEL_BENCHMARK.md``.

Rebuild order, from the repository root (baseline against DeepSeek V4.1 Flash, on the
153 cohort and the 300 sample)::

    python -m vdl_tools.shared_tools.activity_extraction.model_judges \\
        --out-dir data_out/activity_extraction_judges/two_model_cohort153
    python -m vdl_tools.shared_tools.activity_extraction.model_judges \\
        --org-file data_out/activity_extraction_probe/sample300.json \\
        --out-dir data_out/activity_extraction_judges/two_model_sample300
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_reference \\
        --dir two_model_cohort153
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_reference \\
        --dir two_model_sample300 --org-file data_out/activity_extraction_probe/sample300.json
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_misses \\
        --dir two_model_cohort153 --tag _2m --run
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_misses \\
        --dir two_model_sample300 --tag _2m --run \\
        --org-file data_out/activity_extraction_probe/sample300.json
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.latency_price
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.two_model_summary
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.error_analysis
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.failure_dossier --round 1
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.failure_dossier --round 2
"""

from pathlib import Path

# Relative to the repository root, like model_probe's and model_judges' defaults.
PROBE_DIR = Path("data_out/activity_extraction_probe")
JUDGES_DIR = Path("data_out/activity_extraction_judges")
SAMPLE_FILE = PROBE_DIR / "sample300.json"
# The two folders the two-model reports read, one per set of orgs.
TWO_MODEL_DIRS = {"cohort153": "two_model_cohort153", "sample300": "two_model_sample300"}
