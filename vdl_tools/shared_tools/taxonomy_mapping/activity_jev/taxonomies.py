"""The ``Taxonomy`` structure and a parameterized workbook reader; no taxonomy is named here."""

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ...fingerprints import fingerprint


@dataclass
class Taxonomy:
    """One taxonomy with its hierarchy, node definitions and built Jev questions.

    Attributes
    ----------
    name : str
        Short identifier, e.g. ``one_earth``; prefixes the exported status column.
    columns : tuple[str, ...]
        One path column per level, shallowest first, e.g.
        ``("Pillar", "Sub-Pillar", "Solution")``. Depth is ``len(columns) - 1``.
    levels : list[dict]
        One spec per level in the shape ``attach_mapping_columns`` and
        ``distribute_funding_from_matches`` expect: ``idx``, ``name``, ``key_col``,
        ``output_col`` (equal to the path column), optional ``sheet``,
        ``parent_filters``.
    nodes : pandas.DataFrame
        One row per node: ``node_id`` (hash of name and full ancestor path, so
        same-named children do not collide), ``level`` (the path column name),
        ``depth`` (0-based), ``name``, ``question`` (dict with ``type``,
        ``instructions`` and optional ``criteria``), the path columns (None below the
        node's depth) and any loader-supplied attributes.
    sha256 : str
        Hash of the source payload (the workbook bytes); part of every score's lineage.
    path : str
        Where the payload was read from.
    variant : str
        Question wording variant the ``question`` column was built with.
    subject_kind : str, optional
        What the questions address (``activity``, ``organization``). When set and the
        subjects carry ``kind``, the two must agree.

    Raises
    ------
    ValueError
        Empty or duplicate path columns, missing node columns, blank or duplicate
        node IDs, a depth outside the configured levels, a blank ancestor, a path
        extending past its depth, duplicate or orphan paths, a node without an
        explicit question, or level specs that do not line up with the path columns.
    """

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
        """Hash of every node's question, keyed by node ID; changes when wording changes."""
        return fingerprint({str(r.node_id): r.question for r in self.nodes.itertuples()})


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
        ``question_builder(nodes, columns, variant) -> nodes`` adds the ``question``
        column (a dict with ``type``, ``instructions`` and optional ``criteria``) to
        the node frame described under ``Taxonomy.nodes``.
    variant : str
        Passed through to ``question_builder`` and recorded on the taxonomy.
    level_names : sequence[str], optional
        Display name per level for the ``levels`` specs; defaults to ``columns``.
    attributes : sequence[str]
        Extra sheet columns copied onto nodes where present (None elsewhere).
    subject_kind : str, optional
        Recorded on the taxonomy; see ``Taxonomy.subject_kind``.

    Returns
    -------
    Taxonomy
        A row with ``Exclude == 1`` and all of its descendants are omitted. Every
        other row becomes a node whose ``node_id`` is the first 24 hex characters of
        the fingerprint of ``[name, full path]``.

    Raises
    ------
    ValueError
        Mismatched ``columns`` / ``sheets`` / ``level_names`` lengths, a missing
        required column, a blank path or definition, a duplicate or orphan path, a
        workbook with no included nodes, or anything ``Taxonomy`` itself rejects.
    """
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
        {
            "idx": i,
            "name": level_names[i],
            "key_col": c,
            "output_col": c,
            "sheet": sheets[i],
            "parent_filters": list(columns[:i]),
        }
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
