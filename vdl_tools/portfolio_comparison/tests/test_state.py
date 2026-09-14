"""Run ledger: every stage records the code it ran with."""

import json
import subprocess

from vdl_tools.portfolio_comparison.state import PipelineState


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)


def test_record_stage_stamps_code_versions(tmp_path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q",
         "--allow-empty", "-m", "first")
    sha1 = _git(tmp_path, "rev-parse", "HEAD").stdout.strip()

    state = PipelineState(tmp_path)
    state.record_stage("compare", n_rows=3)
    saved = json.loads((tmp_path / "pipeline_state.json").read_text())
    assert saved["code_versions"]["engagement_repo"] == sha1
    assert saved["code_versions"]["vdl_tools"]  # the checkout this test runs from
    assert saved["stages"]["compare"]["code"].startswith("vdl_tools@")
    assert f"engagement_repo@{sha1[:9]}" in saved["stages"]["compare"]["code"]

    # A later stage after a new engagement commit refreshes the top-level line
    # (the old behaviour stamped it once, at pin_baseline, and never again).
    _git(tmp_path, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q",
         "--allow-empty", "-m", "second")
    sha2 = _git(tmp_path, "rev-parse", "HEAD").stdout.strip()
    PipelineState(tmp_path).record_stage("finalize", status="finalized")
    saved = json.loads((tmp_path / "pipeline_state.json").read_text())
    assert saved["code_versions"]["engagement_repo"] == sha2
    assert saved["stages"]["compare"]["code"].endswith(sha1[:9])  # history kept
    assert "code:" in PipelineState(tmp_path).render_status()


def test_record_stage_outside_git(tmp_path):
    PipelineState(tmp_path).record_stage("intake", n_files=1)
    saved = json.loads((tmp_path / "pipeline_state.json").read_text())
    assert saved["code_versions"]["engagement_repo"] is None
    assert "engagement_repo@unknown" in saved["stages"]["intake"]["code"]
