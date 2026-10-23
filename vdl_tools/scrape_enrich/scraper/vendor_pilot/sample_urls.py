"""Draw a stratified sample of home URLs from the scrape cache (read-only).

Strata are assigned in priority order so they are disjoint, and are drawn
from ``web_pages_scraped`` index rows (``page_type = 'PageType.INDEX'``)
left-joined to ``web_pages_parsed`` on the home key:

====================  ======================================================
stratum               definition (index row unless stated)
====================  ======================================================
status_429            response_status_code = 429 (rate-limited us)
status_0              response_status_code = 0 (no HTTP response at all:
                      DNS, TLS, timeout)
status_404            response_status_code = 404
empty_combined        any other status, but the home's
                      web_pages_parsed.combined_text is empty or missing
                      (fetch "succeeded", extraction produced nothing)
ok_200                status 200, non-empty parsed_html, non-empty
                      combined_text (the population the pipeline is happy
                      with today)
====================  ======================================================

Platform domains (LinkedIn, Facebook, ...) and PDF home URLs are excluded:
the former are known bot walls that identify no org, the latter never go
through HTML extraction.

The sample is written as a parquet with a lineage footer (seed, per-stratum
population sizes, query) via ``vdl_tools.shared_tools.parquet_cache``.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_urls
"""

import argparse
import json
import subprocess
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.url_keys import website_domain
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.parquet_cache import write_dataframe
from vdl_tools.shared_tools.tools.logger import logger

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    DEFAULT_OUT_DIR,
    SAMPLE_FILENAME,
    git_sha,
)

DEFAULT_SIZES = {
    "ok_200": 500,
    "empty_combined": 200,
    "status_429": 100,
    "status_0": 100,
    "status_404": 100,
}

# One pass over the index rows; the CASE order is the stratum priority.
STRATUM_SQL = """
WITH idx AS (
    SELECT
        s.cleaned_key,
        s.home_url,
        s.full_path,
        s.response_status_code,
        s.num_errors,
        s.parsed_html,
        p.combined_text,
        p.num_errors AS parsed_num_errors,
        CASE
            WHEN s.response_status_code = 429 THEN 'status_429'
            WHEN s.response_status_code = 0 THEN 'status_0'
            WHEN s.response_status_code = 404 THEN 'status_404'
            WHEN coalesce(p.combined_text, '') = '' THEN 'empty_combined'
            WHEN s.response_status_code = 200
                 AND coalesce(s.parsed_html, '') <> '' THEN 'ok_200'
            ELSE 'other'
        END AS stratum
    FROM web_pages_scraped s
    LEFT JOIN web_pages_parsed p ON p.cleaned_home_key = s.cleaned_key
    WHERE s.page_type = 'PageType.INDEX'
      AND s.home_url IS NOT NULL
      AND lower(s.home_url) NOT LIKE '%%.pdf'
)
SELECT * FROM idx WHERE stratum = :stratum
ORDER BY random()
LIMIT :limit
"""

POPULATION_SQL = """
SELECT
    CASE
        WHEN s.response_status_code = 429 THEN 'status_429'
        WHEN s.response_status_code = 0 THEN 'status_0'
        WHEN s.response_status_code = 404 THEN 'status_404'
        WHEN coalesce(p.combined_text, '') = '' THEN 'empty_combined'
        WHEN s.response_status_code = 200
             AND coalesce(s.parsed_html, '') <> '' THEN 'ok_200'
        ELSE 'other'
    END AS stratum,
    count(*) AS n
FROM web_pages_scraped s
LEFT JOIN web_pages_parsed p ON p.cleaned_home_key = s.cleaned_key
WHERE s.page_type = 'PageType.INDEX'
GROUP BY 1
"""


def draw_sample(sizes: dict[str, int], seed: float, oversample: float = 1.5) -> tuple[pd.DataFrame, dict]:
    """Draw ``sizes[stratum]`` rows per stratum. Oversamples so the
    platform/PDF exclusions applied in pandas still leave enough rows."""
    frames = []
    with get_session() as session:
        population = dict(session.execute(text(POPULATION_SQL)).fetchall())
        # setseed makes ORDER BY random() reproducible within this connection.
        session.execute(text("SELECT setseed(:seed)"), {"seed": seed})
        for stratum, n in sizes.items():
            rows = session.execute(
                text(STRATUM_SQL),
                {"stratum": stratum, "limit": int(n * oversample) + 10},
            ).mappings().all()
            df = pd.DataFrame(rows)
            if df.empty:
                logger.warning("Stratum %s is empty in the cache", stratum)
                continue
            df["domain"] = df["home_url"].map(website_domain)
            df = df[df["domain"] != ""]  # drops platforms, free-mail, placeholders
            df = df.drop_duplicates("domain").head(n)
            logger.info("%s: population %s, sampled %s", stratum, population.get(stratum), len(df))
            frames.append(df)
    sample = pd.concat(frames, ignore_index=True)
    sample["parsed_html"] = sample["parsed_html"].fillna("")
    sample["combined_text"] = sample["combined_text"].fillna("")
    sample["parsed_html_len"] = sample["parsed_html"].str.len()
    sample["combined_text_len"] = sample["combined_text"].str.len()
    sample["ours_verdict"] = [
        classify_text_quality(t, n or 0)
        for t, n in zip(sample["parsed_html"], sample["num_errors"])
    ]
    sample["ours_combined_verdict"] = [
        classify_text_quality(t, n or 0)
        for t, n in zip(sample["combined_text"], sample["parsed_num_errors"])
    ]
    # Shuffle so a partial vendor run is still stratified.
    sample = sample.sample(frac=1, random_state=int(seed * 1e6)).reset_index(drop=True)
    sample["sample_order"] = range(len(sample))
    return sample, {str(k): int(v) for k, v in population.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--seed", type=float, default=0.42, help="postgres setseed() value in [-1, 1]")
    parser.add_argument("--sizes", type=json.loads, default=DEFAULT_SIZES, help="JSON dict stratum -> n")
    args = parser.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    sample, population = draw_sample(args.sizes, args.seed)
    out = args.out_dir / SAMPLE_FILENAME
    write_dataframe(
        sample,
        out,
        lineage={
            "source": "postgres web_pages_scraped (PageType.INDEX) LEFT JOIN web_pages_parsed",
            "created_by": "vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_urls",
            "git_sha": git_sha(),
            "seed": args.seed,
            "requested_sizes": args.sizes,
            "population_by_stratum": population,
            "sampled_by_stratum": sample["stratum"].value_counts().to_dict(),
            "exclusions": "platform/free-mail domains (url_keys.website_domain == ''), PDF home URLs, duplicate domains",
            "stratum_sql": STRATUM_SQL,
        },
    )
    print(sample["stratum"].value_counts().to_string())
    print(f"wrote {len(sample)} rows -> {out}")


if __name__ == "__main__":
    main()
