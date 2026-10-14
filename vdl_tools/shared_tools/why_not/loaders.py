"""Readers for the kinds of source a pipeline declares, plus small helpers.

Parquet (local or S3, with the ``vdl_lineage`` footer), json records, xlsx,
json uid sets / maps, and the shared prompt cache. Every reader is plain and
raises on failure; ``index.build_index`` decides what a failure means.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.why_not.spec import Source

SNIPPET = 600

_SUFFIX = re.compile(
    r"\b(incorporated|inc|llc|l l c|ltd|limited|corp|corporation|co|company|pbc|plc|gmbh|the)\b"
)


def normalize_name(value) -> str:
    """Lower-case, strip punctuation and legal suffixes, collapse spaces."""
    if _isna(value):
        return ""
    s = str(value).lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_domain(value) -> str:
    """``https://www.Example.org/path`` -> ``example.org``; '' when not a URL."""
    if _isna(value):
        return ""
    s = str(value).strip().lower()
    if not s or " " in s:
        return ""
    s = re.sub(r"^[a-z]+://", "", s)
    s = s.split("/")[0].split("?")[0]
    s = s[4:] if s.startswith("www.") else s
    return s if "." in s else ""


def linkedin_slug(value) -> str:
    if _isna(value):
        return ""
    m = re.search(r"linkedin\.com/(?:company|school|showcase)/([^/?#]+)", str(value).lower())
    return m.group(1) if m else ""


def _isna(v) -> bool:
    return v is None or v is pd.NA or (isinstance(v, float) and pd.isna(v))


def is_s3(uri) -> bool:
    return str(uri).startswith("s3://")


def local_exists(uri) -> bool:
    return bool(uri) and not is_s3(uri) and Path(str(uri)).exists()


def readable(uri) -> bool:
    return bool(uri) and (is_s3(uri) or local_exists(uri))


def snip(series: pd.Series) -> pd.Series:
    return series.map(lambda v: None if _isna(v) else str(v)[:SNIPPET])


def textlen(series: pd.Series) -> pd.Series:
    return series.map(lambda v: 0 if _isna(v) else len(str(v)))


def flag(series: pd.Series) -> pd.Series:
    """True/False/None object column -> plain bool (None counts as False)."""
    return series.map(lambda v: False if _isna(v) else bool(v)).astype(bool)


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def read_frame(uri, columns: dict[str, str] | None = None) -> pd.DataFrame:
    """Parquet / json-records / xlsx -> DataFrame, renamed to index columns."""
    uri = str(uri)
    if is_s3(uri) or uri.endswith(".parquet"):
        from vdl_tools.shared_tools.parquet_cache import read_dataframe
        df = read_dataframe(uri)
    elif uri.endswith(".json"):
        df = pd.read_json(uri, convert_dates=False)
    elif uri.endswith(".xlsx"):
        df = pd.read_excel(uri)
    else:
        raise ValueError(f"unsupported artifact: {uri}")
    if columns:
        df = df[[c for c in columns if c in df.columns]].rename(columns=columns)
    return df


def read_uid_set(src: Source, uri) -> set[str]:
    uri = str(uri)
    if uri.endswith(".json"):
        data = json.load(open(uri))
        if isinstance(data, dict):
            keys = [k for k, v in data.items() if src.json_value is None or v == src.json_value]
        else:
            keys = list(data)
        return {str(k) for k in keys}
    df = read_frame(uri)
    if src.row_filter is not None:
        df = src.row_filter(df)
    return set(df[src.uid_col].astype(str))


def read_uid_map(uri) -> dict:
    data = json.load(open(str(uri)))
    return {str(k): v for k, v in data.items()}


def lineage_of(uri) -> dict:
    """Lineage footer for parquets; file mtime for other local files."""
    if is_s3(uri) or str(uri).endswith(".parquet"):
        try:
            from vdl_tools.shared_tools.parquet_cache import get_lineage
            return get_lineage(str(uri)) or {}
        except Exception:  # noqa: BLE001
            return {}
    if local_exists(uri):
        return {"created_at": datetime.fromtimestamp(Path(str(uri)).stat().st_mtime).isoformat(timespec="seconds")}
    return {}


# ---------------------------------------------------------------------------
# Prompt cache (relevance verdicts)
# ---------------------------------------------------------------------------

def parse_verdict(response_text) -> int | None:
    """Mirror of RelevanceCache._parse_prediction_and_probability: int(float(text)) in {0, 1}."""
    try:
        v = int(float(str(response_text).strip()))
    except (TypeError, ValueError):
        return None
    return v if v in (0, 1) else None


def relevance_snapshot(prompt_name: str, model: str) -> list:
    """[row count, last update] of a classifier's verdicts: the cache fingerprint."""
    from sqlalchemy import text

    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    sql = text("""SELECT count(*) AS n, max(date_updated) AS last FROM prompt_response
                  WHERE model_name = :m AND prompt_id IN (SELECT id FROM prompt WHERE name = :p)""")
    with get_session() as session:
        row = session.execute(sql, {"m": model, "p": prompt_name}).mappings().first()
    return [int(row["n"]), str(row["last"])[:19] if row["last"] else None]


def read_relevance_cache(prompt_name: str, model: str, membership: str = "has_relevance_text") -> pd.DataFrame:
    """Latest verdict per org from the shared prompt cache.

    One row per ``given_id`` (the org id), picked the way the cache picks
    (``_row_preference``: a success row beats an error row, newer beats
    older). ``relevance_prob`` is ``exp(logprob)`` when the stored response
    carries one; fine-tuned classifiers usually store none.
    """
    from sqlalchemy import text

    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    sql = text(f"""
        SELECT DISTINCT ON (given_id)
               given_id, response_text, num_errors, date_updated,
               left(input_text, {SNIPPET}) AS input_text, length(input_text) AS input_len,
               coalesce(response_full #>> '{{output,0,content,0,logprobs,0,logprob}}',
                        response_full #>> '{{choices,0,logprobs,content,0,logprob}}') AS logprob
        FROM prompt_response
        WHERE model_name = :m AND prompt_id IN (SELECT id FROM prompt WHERE name = :p)
        ORDER BY given_id, (coalesce(num_errors, 0) = 0) DESC, date_updated DESC NULLS LAST""")
    with get_session() as session:
        rows = session.execute(sql, {"m": model, "p": prompt_name}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    if df.empty:
        raise RuntimeError(f"no verdicts in the prompt cache for prompt {prompt_name!r} / model {model!r}")
    out = pd.DataFrame({
        "uid": df["given_id"].astype(str),
        "relevance_pred": df["response_text"].map(parse_verdict),
        "relevance_prob": df["logprob"].map(lambda v: None if v is None else round(math.exp(float(v)), 3)),
        "relevance_raw": df["response_text"].map(lambda v: None if v is None else str(v)[:40]),
        "relevance_text": df["input_text"],
        "relevance_text_len": df["input_len"],
        "relevance_verdict_date": df["date_updated"].map(lambda v: None if v is None else str(v)[:10]),
    })
    out[membership] = True
    return out
