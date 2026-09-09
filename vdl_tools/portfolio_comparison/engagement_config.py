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


MATCH_OBJECTIVES = {"text", "financials"}


@dataclass
class EngagementConfig:
    customer: str
    vertical: str
    baseline_run: BaselineRun
    inputs: dict[str, str]
    # What matching must deliver, per the downstream analysis:
    #   "text"       — taxonomy mapping needs descriptive text; any adequate
    #                  text bundle (scrapeable URL / LinkedIn / any source
    #                  description) makes a row enrichment-ready; CB and NZI
    #                  ids may mix freely (no financials are compared).
    #   "financials" — funding comparisons need one consistent canonical
    #                  source; a row is ready only with a matched id.
    match_objective: str = "financials"
    confidentiality: dict[str, str] = field(default_factory=dict)
    # Optional per-engagement intake rulings (decisions are data):
    #   column_overrides: {input_label: {customer_column: canonical_field}}
    #   disposition_value_overrides: {raw_value_lower: invested|passed|exclude}
    #     ("" covers blank cells)
    #   ein_ignore: {column: <header>, pattern: <substr>} — rows whose column
    #     matches carry someone else's EIN (e.g. fiscal sponsor); ignore it
    #     for identity matching
    intake: dict = field(default_factory=dict)
    # Optional enrichment settings:
    #   taxonomy_path: absolute path to the pinned OE taxonomy xlsx (same
    #     vintage as the baseline run, so portfolio and ecosystem taxonomies
    #     stay comparable)
    enrichment: dict = field(default_factory=dict)
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
                match_objective=eng.get("match_objective", "financials"),
                confidentiality=eng.get("confidentiality", {}),
                intake=eng.get("intake", {}),
                enrichment=eng.get("enrichment", {}),
                root=path.parent,
            )
        except (KeyError, TypeError) as exc:
            raise ValueError(f"{path}: malformed engagement.yaml ({exc})") from exc
        config.validate()
        return config

    def validate(self):
        problems = []
        if self.match_objective not in MATCH_OBJECTIVES:
            problems.append(
                f"match_objective must be one of {sorted(MATCH_OBJECTIVES)}, "
                f"got {self.match_objective!r}"
            )
        if self.baseline_run.source not in SOURCES:
            problems.append(
                f"baseline_run.source must be one of {sorted(SOURCES)}, "
                f"got {self.baseline_run.source!r}"
            )
        if not self.inputs:
            problems.append("inputs is empty — at least one customer file is required")
        if problems:
            raise ValueError("engagement.yaml invalid: " + "; ".join(problems))

    def validate_inputs_exist(self):
        """Called at intake — pin_baseline may run before customer files arrive."""
        missing = [
            f"inputs.{label}: file not found at {self.root / rel}"
            for label, rel in self.inputs.items()
            if not (self.root / rel).exists()
        ]
        if missing:
            raise ValueError("engagement.yaml invalid: " + "; ".join(missing))

    def input_path(self, label: str) -> Path:
        return self.root / self.inputs[label]

    def results_dir(self) -> Path:
        out = self.root / "data" / "results"
        out.mkdir(parents=True, exist_ok=True)
        return out
