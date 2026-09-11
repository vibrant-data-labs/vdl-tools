"""Column selection shared by the company / funding-round / investor processors.

Each processor keeps a hand-maintained list of the raw NZI fields it carries
through, renamed ``camelCase`` -> ``snake_case`` + suffix. The three
``filter_format_columns`` functions were copy-paste identical and did
``df[keep_columns]``, which raises ``KeyError`` on any listed field a record
lacks. That was invisible on the v1 API, whose records always carried every
listed field, and fatal on v2, which dropped eight of the company fields
(``directURL``, ``eutopiaScore``, …) that no analysis reads.

``select_and_rename`` is lenient — missing fields become NA — but never
silent: every missing field is logged with the entity name, so schema drift
shows up in the run log instead of as a crash or as a quietly empty column.
"""

from typing import Iterable, List

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake


def missing_columns(df: pd.DataFrame, columns: Iterable[str]) -> List[str]:
    """The listed columns absent from ``df``, in list order."""
    present = set(df.columns)
    return [c for c in columns if c not in present]


def select_and_rename(
    df: pd.DataFrame,
    columns: Iterable[str],
    keep_suffix: str,
    entity: str,
) -> pd.DataFrame:
    """Keep ``columns`` (renamed to ``snake_case + keep_suffix``) plus every
    column already carrying ``keep_suffix``.

    Columns in ``columns`` that ``df`` lacks are added as NA and logged, so a
    processor never crashes on an upstream schema change but the change is
    always visible in the log.
    """
    columns = list(columns)
    missing = missing_columns(df, columns)
    if missing:
        logger.warning(
            "NZI %s records lack %d expected column(s), filled with NA: %s",
            entity, len(missing), missing,
        )
    extra = [c for c in df.columns if c.endswith(keep_suffix) and c not in columns]
    keep = list(dict.fromkeys(columns + extra))  # order-preserving dedupe
    out = df.reindex(columns=keep)
    return out.rename(columns={c: f"{camel_to_snake(c)}{keep_suffix}" for c in columns})
