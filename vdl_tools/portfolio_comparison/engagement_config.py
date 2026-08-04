"""Load and validate an engagement repo's ``engagement.yaml``."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from vdl_tools.portfolio_comparison.schema import SOURCES


@dataclass
class BaselineRun:
    name: str
    version: str
    enriched_uri: str
    network_nodes_uri: str
    source: str
    taxonomy: str
    taxonomy_version: str


@dataclass
class EngagementConfig:
    customer: str
    vertical: str
    baseline_run: BaselineRun
    inputs: dict[str, str]
    confidentiality: dict[str, str] = field(default_factory=dict)
    root: Path = field(default_factory=Path.cwd)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "EngagementConfig":
        path = Path(path)
        raw = yaml.safe_load(path.read_text())
        try:
            eng = raw["engagement"]
            baseline = BaselineRun(**eng["baseline_run"])
            config = cls(
                customer=eng["customer"],
                vertical=eng["vertical"],
                baseline_run=baseline,
                inputs=eng["inputs"],
                confidentiality=eng.get("confidentiality", {}),
                root=path.parent,
            )
        except (KeyError, TypeError) as exc:
            raise ValueError(f"{path}: malformed engagement.yaml ({exc})") from exc
        config.validate()
        return config

    def validate(self):
        problems = []
        if self.baseline_run.source not in SOURCES:
            problems.append(
                f"baseline_run.source must be one of {sorted(SOURCES)}, "
                f"got {self.baseline_run.source!r}"
            )
        if not self.inputs:
            problems.append("inputs is empty — at least one customer file is required")
        for label, rel in self.inputs.items():
            if not (self.root / rel).exists():
                problems.append(f"inputs.{label}: file not found at {self.root / rel}")
        if problems:
            raise ValueError("engagement.yaml invalid: " + "; ".join(problems))

    def input_path(self, label: str) -> Path:
        return self.root / self.inputs[label]

    def results_dir(self) -> Path:
        out = self.root / "data" / "results"
        out.mkdir(parents=True, exist_ok=True)
        return out
