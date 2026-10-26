"""Draw about 2,000 organizations at population proportions for the end-to-end pilot.

Source, in order of preference:

1. ``--source cft`` (default): the US Climate Finance Tracker org parquet,
   ``cb_cd_li_meta_mapped.parquet`` under climate-landscape's current S3
   results version (``paths.ini`` there: ``s3_project_version``), rows with
   a non-empty ``Website`` and ``prediction_relevant`` true (the published
   tracker population). One org per domain, platform and free-mail domains
   dropped (``url_keys.website_domain``), uniform random sample.
2. ``--source cache``: uniform random index rows of ``web_pages_scraped``.

No hard-case stratification: credits per org should come out representative.

Each org is joined (read-only) to the cache for the historical comparison:
the home page's index row (status, extracted text, its verdict), the
org's ``web_pages_parsed.combined_text`` and its verdict, and the subpaths
of the cached subpages. Written as ``sample_orgs.parquet`` with lineage.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_orgs
"""

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from vdl_tools.scrape_enrich.scraper.scrape_websites import ensure_url_scheme
from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.url_keys import extract_website_name, website_domain
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.logger import logger

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR, git_sha

ORGS_FILENAME = "sample_orgs.parquet"
CFT_URI = "s3://climate-landscape-data/2026_08_04_oe_hierarchy_update/results/cb_cd_li_meta_mapped.parquet"
CFT_COLUMNS = ["id", "Website", "prediction_relevant", "Data Source", "Org Type"]

CACHE_SQL = """
SELECT s.cleaned_key, s.home_url AS cache_home_url, s.response_status_code, s.num_errors,
       s.parsed_html, p.combined_text, p.num_errors AS parsed_num_errors
FROM web_pages_scraped s
LEFT JOIN web_pages_parsed p ON p.cleaned_home_key = s.cleaned_key
WHERE s.page_type = 'PageType.INDEX' AND s.cleaned_key = ANY(:keys)
"""
# Subpage rows are keyed '<home key>/<clean path>' (scrape_websites_psql).
CACHE_SUBPAGES_SQL = """
SELECT split_part(cleaned_key, '/', 1) AS home_key, subpath, response_status_code,
       length(coalesce(parsed_html, '')) AS parsed_len
FROM web_pages_scraped
WHERE page_type = 'PageType.PAGE' AND split_part(cleaned_key, '/', 1) = ANY(:keys)
"""
CACHE_RANDOM_SQL = """
SELECT home_url AS "Website", cleaned_key AS id
FROM web_pages_scraped
WHERE page_type = 'PageType.INDEX' AND home_url IS NOT NULL AND lower(home_url) NOT LIKE '%%.pdf'
ORDER BY random() LIMIT :limit
"""


def draw_cft(n: int, seed: int) -> tuple[pd.DataFrame, dict]:
    df = read_dataframe(CFT_URI, columns=CFT_COLUMNS)
    pop = {"rows": int(len(df))}
    if "prediction_relevant" in df.columns and df["prediction_relevant"].notna().any():
        rel = df["prediction_relevant"]
        rel = rel.map(lambda v: str(v).strip().lower() in ("true", "1", "1.0", "yes")) if rel.dtype == object else rel.astype(bool)
        df = df[rel]
        pop["relevant"] = int(len(df))
    df = df[df["Website"].fillna("").str.strip() != ""]
    pop["with_website"] = int(len(df))
    df = df.assign(domain=df["Website"].map(website_domain))
    df = df[df["domain"] != ""].drop_duplicates("domain")
    pop["unique_domains"] = int(len(df))
    sample = df.sample(n=min(n, len(df)), random_state=seed).copy()
    sample["org_id"] = sample["id"].astype(str)
    sample["source"] = "cft:" + CFT_URI
    return sample, pop


def draw_cache(n: int, seed: float) -> tuple[pd.DataFrame, dict]:
    with get_session() as session:
        session.execute(text("SELECT setseed(:seed)"), {"seed": seed})
        rows = session.execute(text(CACHE_RANDOM_SQL), {"limit": int(n * 1.5) + 10}).mappings().all()
    df = pd.DataFrame(rows)
    df = df.assign(domain=df["Website"].map(website_domain))
    df = df[df["domain"] != ""].drop_duplicates("domain").head(n).copy()
    df["org_id"] = df["id"].astype(str)
    df["source"] = "cache:web_pages_scraped index rows"
    return df, {"sampled_index_rows": int(len(df))}


def join_cache(sample: pd.DataFrame) -> pd.DataFrame:
    keys = sample["cleaned_home_key"].tolist()
    with get_session() as session:
        idx = pd.DataFrame(session.execute(text(CACHE_SQL), {"keys": keys}).mappings().all())
        sub = pd.DataFrame(session.execute(text(CACHE_SUBPAGES_SQL), {"keys": keys}).mappings().all())
    if idx.empty:
        idx = pd.DataFrame(columns=["cleaned_key", "cache_home_url", "response_status_code", "num_errors",
                                    "parsed_html", "combined_text", "parsed_num_errors"])
    idx = idx.rename(columns={"cleaned_key": "cleaned_home_key", "response_status_code": "cache_status"})
    idx["cache_parsed_len"] = idx["parsed_html"].fillna("").str.len()
    idx["cache_home_verdict"] = [classify_text_quality(t, e or 0) for t, e in zip(idx["parsed_html"].fillna(""), idx["num_errors"])]
    idx["cache_combined_len"] = idx["combined_text"].fillna("").str.len()
    idx["cache_combined_verdict"] = [classify_text_quality(t, e or 0) for t, e in zip(idx["combined_text"].fillna(""), idx["parsed_num_errors"])]
    idx = idx.drop(columns=["parsed_html", "combined_text"])
    if not sub.empty:
        subpaths = sub.groupby("home_key")["subpath"].apply(lambda s: sorted(set(x for x in s if x))).rename("cache_subpaths")
        idx = idx.merge(subpaths, left_on="cleaned_home_key", right_index=True, how="left")
    else:
        idx["cache_subpaths"] = None
    out = sample.merge(idx, on="cleaned_home_key", how="left")
    out["in_cache"] = out["cache_status"].notna()
    out["cache_subpaths"] = out["cache_subpaths"].map(lambda v: v if isinstance(v, list) else [])
    out["cache_n_subpages"] = out["cache_subpaths"].map(len)
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--source", choices=["cft", "cache"], default="cft")
    args = parser.parse_args(argv)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.source == "cft":
        try:
            sample, population = draw_cft(args.n, args.seed)
        except Exception as exc:  # noqa: BLE001 - fall back, but say so
            logger.warning("CFT parquet unavailable (%s); drawing from the cache instead", exc)
            args.source = "cache"
    if args.source == "cache":
        sample, population = draw_cache(args.n, args.seed / 100.0)

    sample["home_url"] = sample["Website"].astype(str).str.strip().map(ensure_url_scheme)
    sample["cleaned_home_key"] = sample["home_url"].map(extract_website_name)
    sample = sample[sample["cleaned_home_key"].notna()].drop_duplicates("cleaned_home_key")
    sample = join_cache(sample)
    sample = sample.sample(frac=1, random_state=args.seed).reset_index(drop=True)
    sample["sample_order"] = range(len(sample))
    keep = ["org_id", "source", "home_url", "cleaned_home_key", "domain", "sample_order", "in_cache",
            "cache_home_url", "cache_status", "cache_parsed_len", "cache_home_verdict",
            "cache_combined_len", "cache_combined_verdict", "cache_subpaths", "cache_n_subpages"]
    keep += [c for c in ("Data Source", "Org Type") if c in sample.columns]
    sample = sample[keep]
    out = args.out_dir / ORGS_FILENAME
    write_dataframe(sample, out, lineage={
        "source": sample["source"].iloc[0],
        "created_by": "vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_orgs",
        "git_sha": git_sha(),
        "seed": args.seed,
        "requested_n": args.n,
        "population": population,
        "filters": "non-empty Website; prediction_relevant true (cft source); one org per domain; platform/free-mail domains dropped",
        "cache_join": "web_pages_scraped index row + web_pages_parsed by cleaned_home_key; subpages by cleaned_key prefix",
    })
    print(f"source: {sample['source'].iloc[0]}")
    print(f"{len(sample)} orgs; in cache: {int(sample['in_cache'].sum())}; "
          f"cache combined verdicts: {sample['cache_combined_verdict'].value_counts(dropna=False).to_dict()}")
    print(f"cache subpages per org: median {sample['cache_n_subpages'].median():.0f}, mean {sample['cache_n_subpages'].mean():.1f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
