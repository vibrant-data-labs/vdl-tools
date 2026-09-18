"""Run ledger: every stage records the code it ran with."""

import json
import re
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


# --- uncommitted edits: the stamp must not claim HEAD ran ------------------

def _repo_with_commit(path):
    """A git repo whose HEAD tracks one config file (and, like an engagement
    repo, the ledger). Returns HEAD's SHA."""
    _git(path, "init", "-q")
    (path / "engagement.yaml").write_text("customer: contoso\n")
    PipelineState(path).save()
    _git(path, "add", "engagement.yaml", "pipeline_state.json")
    _git(path, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "first")
    return _git(path, "rev-parse", "HEAD").stdout.strip()


def _engagement_stamp(path, stage="compare"):
    PipelineState(path).record_stage(stage)
    saved = json.loads((path / "pipeline_state.json").read_text())
    return saved["code_versions"]["engagement_repo"], saved["stages"][stage]["code"]


def test_clean_tree_has_no_dirty_marker(tmp_path):
    sha = _repo_with_commit(tmp_path)
    version, code = _engagement_stamp(tmp_path)
    assert version == sha
    assert code.endswith(f"engagement_repo@{sha[:9]}")


def test_modified_tracked_file_marks_dirty(tmp_path):
    sha = _repo_with_commit(tmp_path)
    (tmp_path / "engagement.yaml").write_text("customer: fabrikam\n")
    version, code = _engagement_stamp(tmp_path)
    assert re.fullmatch(rf"{sha}\+dirty\.[0-9a-f]{{8}}", version)
    assert code.endswith(f"engagement_repo@{sha[:9]}+dirty.{version[-8:]}")

    # Same edits → same stamp; different edits → a stamp they can be told apart by.
    assert _engagement_stamp(tmp_path, "sourcing")[0] == version
    (tmp_path / "engagement.yaml").write_text("customer: fabrikam\nmode: pilot\n")
    assert _engagement_stamp(tmp_path, "map")[0] not in (sha, version)


def test_staged_new_file_marks_dirty(tmp_path):
    sha = _repo_with_commit(tmp_path)
    (tmp_path / "new_stage.py").write_text("def run(): ...\n")
    _git(tmp_path, "add", "new_stage.py")
    version, _ = _engagement_stamp(tmp_path)
    assert version.startswith(f"{sha}+dirty.")


def test_untracked_only_file_has_no_dirty_marker(tmp_path):
    sha = _repo_with_commit(tmp_path)
    (tmp_path / "scratch.csv").write_text("name\nContoso\n")
    version, code = _engagement_stamp(tmp_path)
    assert version == sha
    assert code.endswith(f"engagement_repo@{sha[:9]}")


def test_ledger_rewrite_does_not_mark_dirty(tmp_path):
    # The ledger is tracked in an engagement repo and every record_stage
    # rewrites it; a multi-stage run must not stamp its own ledger as dirty.
    sha = _repo_with_commit(tmp_path)
    _engagement_stamp(tmp_path, "enrich_acquire")
    assert _git(tmp_path, "status", "--porcelain").stdout.strip()  # ledger changed
    version, code = _engagement_stamp(tmp_path, "enrich_scrape")
    assert version == sha
    assert code.endswith(f"engagement_repo@{sha[:9]}")
