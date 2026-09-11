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
        self.data["stages"][stage] = {
            "status": status,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
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

    def record_code_versions(self, engagement_repo: Path | None = None):
        import vdl_tools

        versions = {}
        vdl_tools_dir = Path(vdl_tools.__file__).resolve().parent.parent
        versions["vdl_tools"] = _git_sha(vdl_tools_dir)
        if engagement_repo is not None:
            versions["engagement_repo"] = _git_sha(Path(engagement_repo))
        self.data["code_versions"].update(versions)
        self.save()

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
                f"{k}@{(v or 'unknown')[:9]}" for k, v in self.data["code_versions"].items()
            )
            lines.append(f"code: {versions}")
        return "\n".join(lines)
