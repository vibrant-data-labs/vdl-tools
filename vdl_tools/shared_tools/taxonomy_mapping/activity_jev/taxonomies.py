"""Taxonomy data and a parameterized workbook reader, independent of any taxonomy."""

from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
import hashlib

import pandas as pd

from ...fingerprints import fingerprint


@dataclass
class Taxonomy:
    name: str
    columns: tuple[str, ...]
    levels: list[dict]
    nodes: pd.DataFrame
    sha256: str
    path: str
    variant: str
    subject_kind: str | None = None

    def __post_init__(self):
        self.columns = tuple(self.columns)
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("Taxonomy levels must be nonempty and unique")
        required = {"node_id", "depth", "level", "name", "question", *self.columns}
        if required - set(self.nodes):
            raise ValueError(f"Missing taxonomy node columns: {sorted(required - set(self.nodes))}")
        if (
            self.nodes.empty
            or self.nodes.node_id.isna().any()
            or self.nodes.node_id.duplicated().any()
        ):
            raise ValueError("Taxonomy node IDs must be nonempty and unique")
        paths = set()
        for row in self.nodes.to_dict("records"):
            depth = row["depth"]
            if (
                isinstance(depth, bool)
                or not isinstance(depth, int)
                or not 0 <= depth < len(self.columns)
            ):
                raise ValueError("Taxonomy node depth is outside configured levels")
            values = [row[c] for c in self.columns]
            if any(pd.isna(v) or not str(v).strip() for v in values[: depth + 1]):
                raise ValueError("Taxonomy path has a blank ancestor")
            if any(pd.notna(v) and v != "" for v in values[depth + 1 :]):
                raise ValueError("Taxonomy path extends beyond its node depth")
            branch = tuple(values[: depth + 1])
            if branch in paths:
                raise ValueError(f"Duplicate taxonomy path: {branch}")
            paths.add(branch)
            if not isinstance(row["question"], dict) or not row["question"].get("instructions"):
                raise ValueError("Every taxonomy node needs an explicit question")
        for branch in paths:
            if len(branch) > 1 and branch[:-1] not in paths:
                raise ValueError(f"Orphan taxonomy path: {branch}")
        if len(self.levels) != len(self.columns):
            raise ValueError("Taxonomy level definitions do not match path columns")
        if any(
            level.get("output_col") != column
            for level, column in zip(self.levels, self.columns, strict=True)
        ):
            raise ValueError("Taxonomy level output columns do not match path columns")

    @property
    def questions_sha256(self):
        return fingerprint({str(r.node_id): r.question for r in self.nodes.itertuples()})


ActivityTaxonomy = Taxonomy  # Backward-compatible public name.


def load_workbook(
    name,
    path,
    *,
    columns,
    sheets,
    question_builder,
    variant="base",
    level_names=None,
    attributes=(),
    subject_kind=None,
):
    """Read explicitly configured sheets; suppress excluded descendant paths."""
    columns, sheets = tuple(columns), tuple(sheets)
    level_names = tuple(level_names or columns)
    if not columns or len(columns) != len(sheets) or len(level_names) != len(columns):
        raise ValueError("Configure one sheet and level name per path column")
    path = Path(path)
    payload = path.read_bytes()
    if payload.startswith(b"version https://git-lfs.github.com/spec/"):
        raise ValueError(f"Taxonomy is an unresolved Git LFS pointer: {path}")
    rows, included, known = [], set(), set()
    with pd.ExcelFile(path) as book:
        for depth, sheet in enumerate(sheets):
            table = pd.read_excel(book, sheet_name=sheet)
            required = [*columns[: depth + 1], "Definition"]
            if missing := set(required) - set(table):
                raise ValueError(f"{sheet} is missing columns: {sorted(missing)}")
            for raw in table.to_dict("records"):
                if any(pd.isna(raw[c]) or not str(raw[c]).strip() for c in columns[: depth + 1]):
                    raise ValueError(f"Blank path in {sheet}")
                branch = tuple(str(raw[c]).strip() for c in columns[: depth + 1])
                if branch in known:
                    raise ValueError(f"Duplicate taxonomy path: {branch}")
                known.add(branch)
                if depth and branch[:-1] not in known:
                    raise ValueError(f"Orphan taxonomy path: {branch}")
                if pd.to_numeric(raw.get("Exclude", 0), errors="coerce") == 1:
                    continue
                if depth and branch[:-1] not in included:
                    continue
                if pd.isna(raw["Definition"]) or not str(raw["Definition"]).strip():
                    raise ValueError(f"Blank definition in {sheet}")
                included.add(branch)
                rows.append(
                    {
                        "node_id": fingerprint([name, branch])[:24],
                        "level": columns[depth],
                        "depth": depth,
                        "name": branch[-1],
                        "Definition": str(raw["Definition"]).strip(),
                        **{c: branch[i] if i <= depth else None for i, c in enumerate(columns)},
                        **{c: raw.get(c) if pd.notna(raw.get(c)) else None for c in attributes},
                    }
                )
    if not rows:
        raise ValueError("Taxonomy has no included nodes")
    nodes = question_builder(pd.DataFrame(rows), columns, variant)
    levels = [
        dict(
            idx=i,
            name=level_names[i],
            key_col=c,
            output_col=c,
            sheet=sheets[i],
            parent_filters=list(columns[:i]),
        )
        for i, c in enumerate(columns)
    ]
    return Taxonomy(
        name,
        columns,
        levels,
        nodes,
        hashlib.sha256(payload).hexdigest(),
        str(path),
        variant,
        subject_kind,
    )


def load_taxonomy(name, path, *, variant=None):
    """Compatibility dispatcher importing only the selected built-in configuration.

    New taxonomies pass a Taxonomy directly or use load_workbook with their builder.
    This convenience function is not used by the generic scorer or selector.
    """
    if name not in ("one_earth", "drawdown"):
        raise ValueError(
            f"Unknown built-in taxonomy: {name}; supply a Taxonomy or use load_workbook"
        )
    return import_module(f"{__package__}.{name}").load_taxonomy(path, variant=variant)
