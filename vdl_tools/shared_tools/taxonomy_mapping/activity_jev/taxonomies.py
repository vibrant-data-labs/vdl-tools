"""The ``Taxonomy`` structure and a parameterized workbook reader; no taxonomy is named here."""

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ...tools.unique_ids import create_deterministic_sha256


@dataclass
class Taxonomy:
    """One taxonomy with its hierarchy, node definitions and built Jev questions.

    Attributes
    ----------
    name : str
        Short identifier, e.g. ``one_earth``; prefixes the exported status column.
    columns : tuple[str, ...]
        One path column per level, shallowest first, e.g.
        ``("Pillar", "Sub-Pillar", "Solution")``.
    nodes : pandas.DataFrame
        One row per node: ``node_id`` (hash of taxonomy name and full ancestor path,
        so same-named children do not collide), ``level`` (the path column name),
        ``depth`` (0-based), ``name``, ``Definition``, ``question`` (dict with ``type``,
        ``instructions`` and optional ``criteria``), the path columns (None below the
        node's depth) and any loader-supplied attributes.
    sha256 : str
        Hash of the source payload (the workbook bytes); part of every score's lineage.
    path : str
        Where the payload was read from.
    """

    name: str
    columns: tuple[str, ...]
    nodes: pd.DataFrame
    sha256: str
    path: str

    def __post_init__(self):
        self.columns = tuple(self.columns)
        missing = [
            r["node_id"]
            for r in self.nodes.to_dict("records")
            if not isinstance(r["question"], dict) or not r["question"].get("instructions")
        ]
        if missing:
            raise ValueError(f"Taxonomy nodes without a question: {missing[:5]}")

    @property
    def levels(self):
        """Level specs in the shape ``attach_mapping_columns`` and funding allocation read."""
        return [{"idx": i, "key_col": c, "output_col": c} for i, c in enumerate(self.columns)]

    @property
    def questions_sha256(self):
        """Hash of every node's question, keyed by node ID; changes when wording changes."""
        return create_deterministic_sha256(
            {str(r.node_id): r.question for r in self.nodes.itertuples()}
        )


def load_workbook(name, path, *, columns, sheets, question_builder, attributes=()):
    """Read one sheet per level from an Excel workbook and build a ``Taxonomy``.

    Parameters
    ----------
    name : str
        Taxonomy identifier stored on the result and mixed into every ``node_id``.
    path : str or pathlib.Path
        Workbook to read. An unresolved Git LFS pointer is rejected.
    columns : sequence[str]
        Path column per level, shallowest first. Sheet ``i`` must contain
        ``columns[:i + 1]`` and ``Definition``; it may contain ``Exclude``.
    sheets : sequence[str]
        Sheet name per level, aligned with ``columns``.
    question_builder : callable
        ``question_builder(nodes, columns) -> nodes`` adds the ``question`` column (a
        dict with ``type``, ``instructions`` and optional ``criteria``) to the node
        frame described under ``Taxonomy.nodes``.
    attributes : sequence[str]
        Extra sheet columns copied onto nodes where present (None elsewhere).

    Returns
    -------
    Taxonomy
        A row with ``Exclude == 1`` and all of its descendants are omitted. Every
        other row becomes a node whose ``node_id`` is the first 24 hex characters of
        the SHA-256 of ``[name, full path]``.

    Raises
    ------
    ValueError
        Mismatched ``columns`` / ``sheets`` lengths, a missing required column, a
        blank path or definition, a duplicate or orphan path, or a workbook with no
        included nodes.
    """
    columns, sheets = tuple(columns), tuple(sheets)
    if len(columns) != len(sheets):
        raise ValueError("Configure one sheet per path column")
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
                    continue  # descendant of an excluded node
                if pd.isna(raw["Definition"]) or not str(raw["Definition"]).strip():
                    raise ValueError(f"Blank definition in {sheet}")
                included.add(branch)
                rows.append(
                    {
                        "node_id": create_deterministic_sha256([name, branch])[:24],
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
    return Taxonomy(
        name,
        columns,
        question_builder(pd.DataFrame(rows), columns),
        hashlib.sha256(payload).hexdigest(),
        str(path),
    )
