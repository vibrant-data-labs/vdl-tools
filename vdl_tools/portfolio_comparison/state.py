"""Engagement pipeline state — ``pipeline_state.json`` read/write and status view.

The state file is the engagement's run ledger: which stages ran, against which
artifact hashes and code versions. It lives at the engagement repo root so
`git log` on the file doubles as a run history.
"""

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

STAGES = [
    "pin_baseline",
    "intake",
    "match",
    "review_vdl",
    "review_customer",
    "finalize",
    # Phase 2 (enrichment) — each stage is one ledger entry.
    "enrich_acquire",
    "enrich_scrape",
    "enrich_summarize",
    "enrich_taxonomy",
    "enrich_geocode",
    # Phase 3
    "compare",
    "sourcing",
    "map_input",
    "map",
]

STATE_FILENAME = "pipeline_state.json"


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha(repo_dir: Path) -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir, capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _git_code_version(repo_dir: Path, exclude: tuple[str, ...] = ()) -> str | None:
    """HEAD's SHA, suffixed ``+dirty.<hash8>`` when tracked files differ from HEAD.

    A stage that runs on uncommitted edits did not run on HEAD, so the stamp
    says so; ``<hash8>`` is a SHA-256 prefix of ``git diff HEAD``, which tells
    two dirty runs apart. Untracked files don't count — a new module shows up
    once it is ``git add``-ed. ``exclude`` lists paths (relative to
    ``repo_dir``) left out of the check: the ledger, which ``record_stage``
    itself rewrites. If the check fails, the stamp says ``+dirty`` rather than
    claim a clean HEAD nobody verified.
    """
    sha = _git_sha(repo_dir)
    if sha is None:
        return None
    pathspec = ["--", ":/", *(f":(exclude){p}" for p in exclude)]
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no", *pathspec],
            cwd=repo_dir, capture_output=True, check=True,
        ).stdout
        if not status.strip():
            return sha
        diff = subprocess.run(
            ["git", "diff", "HEAD", "--binary", "--no-color", "--no-ext-diff", *pathspec],
            cwd=repo_dir, capture_output=True, check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return f"{sha}+dirty"
    return f"{sha}+dirty.{hashlib.sha256(diff).hexdigest()[:8]}"


def _short(version: str | None) -> str:
    """``<sha9>`` plus any ``+dirty…`` marker; ``unknown`` outside git."""
    if not version:
        return "unknown"
    sha, plus, marker = version.partition("+")
    return sha[:9] + plus + marker


class PipelineState:
    def __init__(self, engagement_root: str | Path):
        self.root = Path(engagement_root)
        self.path = self.root / STATE_FILENAME
        if self.path.exists():
            self.data = json.loads(self.path.read_text())
        else:
            self.data = {"stages": {}, "artifacts": {}, "code_versions": {}}

    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True) + "\n")

    def record_stage(self, stage: str, status: str = "completed", **details):
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
        # Every stage carries the code it ran with; the top-level line
        # refreshes too, so a later engine pull never leaves it stale.
        versions = self.record_code_versions(save=False)
        self.data["stages"][stage] = {
            "status": status,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "code": ",".join(f"{k}@{_short(v)}" for k, v in versions.items()),
            **details,
        }
        self.save()

    def record_artifact(self, name: str, path: str | Path, **details):
        path = Path(path)
        self.data["artifacts"][name] = {
            "path": str(path),
            "sha256": sha256_file(path),
            **details,
        }
        self.save()

    def record_code_versions(self, engagement_repo: Path | None = None,
                             save: bool = True) -> dict:
        import vdl_tools

        versions = {}
        vdl_tools_dir = Path(vdl_tools.__file__).resolve().parent.parent
        versions["vdl_tools"] = _git_code_version(vdl_tools_dir)
        versions["engagement_repo"] = _git_code_version(
            Path(engagement_repo or self.root), exclude=(STATE_FILENAME,),
        )
        self.data["code_versions"].update(versions)
        if save:
            self.save()
        return versions

    def render_status(self) -> str:
        lines = ["Engagement pipeline status", "=" * 26]
        for stage in STAGES:
            info = self.data["stages"].get(stage)
            if info is None:
                lines.append(f"  {stage:<16} —")
                continue
            detail = ", ".join(
                f"{k}={v}" for k, v in info.items() if k not in ("status", "at")
            )
            lines.append(
                f"  {stage:<16} {info['status']:<10} {info['at']}"
                + (f"  ({detail})" if detail else "")
            )
        if self.data["artifacts"]:
            lines.append("artifacts:")
            for name, art in sorted(self.data["artifacts"].items()):
                lines.append(f"  {name}: {art['path']} sha256={art['sha256'][:12]}…")
        if self.data["code_versions"]:
            versions = ", ".join(
                f"{k}@{_short(v)}" for k, v in self.data["code_versions"].items()
            )
            lines.append(f"code: {versions}")
        return "\n".join(lines)
