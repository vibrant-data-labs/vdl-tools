"""Shared harness for the vendor runners: config, rate limiting, resumable
JSONL output, parquet results with lineage.

Vendors are called over plain REST with ``httpx`` (already a vdl-tools
dependency) instead of their SDKs: the pilot is throwaway, and a new
dependency needs a team decision first.

API keys live in ``config.ini`` under::

    [scraping_vendors]
    firecrawl_api_key = ...
    spider_api_key = ...

and are never logged or printed.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

from vdl_tools.scrape_enrich.scraper.scrape_websites import ensure_url_scheme
from vdl_tools.shared_tools.parquet_cache import write_dataframe
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger

DEFAULT_OUT_DIR = Path(".cache/vendor_pilot")
SAMPLE_FILENAME = "sample_urls.parquet"
CONFIG_SECTION = "scraping_vendors"

# Retry policy for the vendor API call itself (not the target site).
MAX_ATTEMPTS = 4
RETRYABLE_API_STATUSES = {429, 500, 502, 503, 504}
# 402 = out of credits: stop the run instead of burning retries.
STOP_API_STATUSES = {401, 402}


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:  # noqa: BLE001 - lineage only
        return "unknown"


def get_api_key(vendor: str) -> str:
    """``[scraping_vendors] <vendor>_api_key`` from config.ini. Raises with the
    section/key name (never the value) when missing."""
    key_name = f"{vendor}_api_key"
    config = get_configuration()
    value = ""
    if CONFIG_SECTION in config:
        value = config[CONFIG_SECTION].get(key_name, "").strip()
    if not value:
        raise RuntimeError(
            f"No API key for {vendor}: add `{key_name} = ...` under [{CONFIG_SECTION}] "
            "in config.ini (VDL_GLOBAL_CONFIG_PATH or ./config.ini)."
        )
    return value


class RateLimiter:
    """Spaces calls ``60 / rpm`` seconds apart across all tasks. ``rpm=None`` disables."""

    def __init__(self, rpm: float | None):
        self.interval = 60.0 / rpm if rpm else 0.0
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        if not self.interval:
            return
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + self.interval
        if wait > 0:
            await asyncio.sleep(wait)


@dataclass
class FetchResult:
    vendor: str
    url: str
    cleaned_key: str = ""
    stratum: str = ""
    sample_order: int = -1
    success: bool = False
    api_status: int | None = None     # HTTP status of the vendor API call
    http_status: int | None = None    # status of the target page, as the vendor reports it
    final_url: str | None = None
    markdown: str = ""
    raw_html: str = ""
    links: list = field(default_factory=list)
    markdown_len: int = 0
    raw_html_len: int = 0
    n_links: int = 0
    wall_seconds: float = 0.0
    credits: float | None = None      # per-response billed credits when the API reports it
    cost_usd: float | None = None     # per-response cost when the API reports it
    error: str | None = None
    attempts: int = 1
    vendor_metadata: dict = field(default_factory=dict)
    fetched_at: str = ""

    def finalize(self) -> "FetchResult":
        self.markdown = self.markdown or ""
        self.raw_html = self.raw_html or ""
        self.links = list(self.links or [])
        self.markdown_len = len(self.markdown)
        self.raw_html_len = len(self.raw_html)
        self.n_links = len(self.links)
        self.fetched_at = self.fetched_at or datetime.now(timezone.utc).isoformat()
        return self


class StopRun(Exception):
    """Raised when the vendor says to stop (out of credits, bad key)."""


class ApiRetry(Exception):
    """Raised when the vendor API call should be retried (429, 5xx, transport)."""

    def __init__(self, message: str, retry_after: float | None = None, api_status: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after
        self.api_status = api_status


class VendorRunner:
    """Subclass per vendor: implement ``fetch`` and ``credits``."""

    name = "vendor"
    base_url = ""
    default_concurrency = 20
    default_rpm: float | None = None
    timeout_seconds = 120.0
    max_attempts = MAX_ATTEMPTS  # API-level attempts per URL (429/5xx/transport)

    def __init__(self, api_key: str):
        self._api_key = api_key

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}

    async def fetch(self, client: httpx.AsyncClient, url: str) -> FetchResult:
        raise NotImplementedError

    async def credits(self, client: httpx.AsyncClient) -> dict | None:
        """Account balance as the vendor reports it (raw JSON), or None."""
        return None

    async def start(self):
        """Called once before the first fetch (open browsers, pools, ...)."""

    async def stop(self):
        """Called once after the last fetch."""

    async def run_context(self) -> dict | None:
        """Where/how the run happened, stored in the run meta (host, egress IP, ...)."""
        return None

    @staticmethod
    def raise_for_api_status(response: httpx.Response):
        status = response.status_code
        if status in STOP_API_STATUSES:
            raise StopRun(f"vendor API returned {status}: {response.text[:300]}")
        if status in RETRYABLE_API_STATUSES:
            retry_after = response.headers.get("Retry-After")
            raise ApiRetry(
                f"vendor API returned {status}: {response.text[:300]}",
                retry_after=float(retry_after) if retry_after and retry_after.replace('.', '', 1).isdigit() else None,
                api_status=status,
            )


async def _fetch_with_retry(runner: VendorRunner, client: httpx.AsyncClient, url: str,
                            retry_counter: "Counter | None" = None) -> FetchResult:
    """``retry_counter`` (a ``collections.Counter``) gets one count per retried
    API status, so a run can report how many 429s/5xx it absorbed."""
    t0 = time.monotonic()
    last_error = None
    max_attempts = runner.max_attempts
    for attempt in range(1, max_attempts + 1):
        try:
            result = await runner.fetch(client, url)
            result.attempts = attempt
            result.wall_seconds = time.monotonic() - t0
            return result.finalize()
        except ApiRetry as exc:
            last_error = f"{exc} (api_status={exc.api_status})"
            if retry_counter is not None:
                retry_counter[str(exc.api_status)] += 1
            if attempt == max_attempts:
                break
            delay = exc.retry_after if exc.retry_after is not None else min(60, 2 ** attempt)
            logger.warning("%s: %s; retry %s/%s in %.0fs", runner.name, url, attempt, max_attempts, delay)
            await asyncio.sleep(delay)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt == max_attempts:
                break
            logger.warning("%s: %s; transport error on attempt %s: %s", runner.name, url, attempt, last_error)
            await asyncio.sleep(min(30, 2 ** attempt))
    return FetchResult(
        vendor=runner.name, url=url, success=False, error=last_error,
        attempts=max_attempts, wall_seconds=time.monotonic() - t0,
    ).finalize()


def _results_paths(out_dir: Path, vendor: str) -> tuple[Path, Path, Path]:
    return (
        out_dir / f"{vendor}_results.jsonl",
        out_dir / f"{vendor}_results.parquet",
        out_dir / f"{vendor}_run_meta.json",
    )


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as fh:
        return [json.loads(line) for line in fh if line.strip()]


def plan_run(sample: pd.DataFrame, jsonl_path: Path, *, limit: int | None = None,
             resume: bool = True) -> tuple[pd.DataFrame, set]:
    """Which sampled URLs a run would fetch: the sample in ``sample_order``,
    cut to ``limit``, minus the keys already in the vendor's JSONL when
    resuming. Returns ``(todo, done_keys)``."""
    todo = sample.sort_values("sample_order")
    if limit:
        todo = todo.head(limit)
    done_keys = {r["cleaned_key"] for r in load_jsonl(jsonl_path)} if resume else set()
    return todo[~todo["cleaned_key"].isin(done_keys)], done_keys


def api_calls_so_far(jsonl_path: Path) -> int:
    """API calls a vendor's JSONL accounts for (one row per URL, ``attempts`` each)."""
    return sum(int(r.get("attempts") or 1) for r in load_jsonl(jsonl_path))


async def run_vendor(
    runner: VendorRunner,
    sample: pd.DataFrame,
    out_dir: Path,
    *,
    concurrency: int | None = None,
    rpm: float | None = None,
    limit: int | None = None,
    resume: bool = True,
) -> pd.DataFrame:
    """Fetch every sampled home URL once through ``runner``.

    Appends one JSON line per URL to ``<vendor>_results.jsonl`` as it goes (so
    a crash or a credit stop keeps what was paid for), then writes
    ``<vendor>_results.parquet`` with lineage and ``<vendor>_run_meta.json``
    with timing and the account balance before/after.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path, parquet_path, meta_path = _results_paths(out_dir, runner.name)
    concurrency = concurrency or runner.default_concurrency
    rpm = rpm if rpm is not None else runner.default_rpm

    todo, done_keys = plan_run(sample, jsonl_path, limit=limit, resume=resume)
    if not resume:
        for path in (jsonl_path, meta_path):  # a fresh start discards the history too
            if path.exists():
                path.unlink()
    logger.info("%s: %s URLs to fetch (%s already done), concurrency=%s rpm=%s",
                runner.name, len(todo), len(done_keys), concurrency, rpm)

    sem = asyncio.Semaphore(concurrency)
    limiter = RateLimiter(rpm)
    write_lock = asyncio.Lock()
    stop_event = asyncio.Event()
    progress = {"n": 0, "ok": 0}
    stop_reason = None

    async with httpx.AsyncClient(headers=runner.headers(), timeout=runner.timeout_seconds) as client:
        await runner.start()
        context = await runner.run_context()
        credits_before = await _safe_credits(runner, client)
        started = datetime.now(timezone.utc)
        t_start = time.monotonic()

        async def one(row):
            nonlocal stop_reason
            if stop_event.is_set():
                return
            async with sem:
                if stop_event.is_set():
                    return
                await limiter.acquire()
                try:
                    # Same normalization production applies before fetching.
                    result = await _fetch_with_retry(runner, client, ensure_url_scheme(row.home_url))
                except StopRun as exc:
                    stop_reason = str(exc)
                    stop_event.set()
                    logger.error("%s: stopping run: %s", runner.name, stop_reason)
                    return
                result.cleaned_key = row.cleaned_key
                result.stratum = row.stratum
                result.sample_order = int(row.sample_order)
                async with write_lock:
                    with jsonl_path.open("a") as fh:
                        fh.write(json.dumps(asdict(result), ensure_ascii=False, default=str) + "\n")
                    progress["n"] += 1
                    progress["ok"] += int(result.success)
                    if progress["n"] % 25 == 0 or progress["n"] == len(todo):
                        elapsed = time.monotonic() - t_start
                        logger.info("%s: %s/%s done, %s ok, %.1f min, %.2f pages/s",
                                    runner.name, progress["n"], len(todo), progress["ok"],
                                    elapsed / 60, progress["n"] / max(elapsed, 1e-6))

        try:
            await asyncio.gather(*(one(row) for row in todo.itertuples(index=False)))
        finally:
            t_end = time.monotonic()
            ended = datetime.now(timezone.utc)
            await runner.stop()
        credits_after = await _safe_credits(runner, client)

    rows = load_jsonl(jsonl_path)
    df = pd.DataFrame(rows)
    meta = {
        "vendor": runner.name,
        "git_sha": git_sha(),
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "wall_seconds_this_run": round(t_end - t_start, 2),
        "n_fetched_this_run": progress["n"],
        "n_total_in_jsonl": len(rows),
        "pages_per_second_this_run": round(progress["n"] / max(t_end - t_start, 1e-6), 4),
        "concurrency": concurrency,
        "rpm": rpm,
        "runner_context": context,
        "credits_before": credits_before,
        "credits_after": credits_after,
        "stop_reason": stop_reason,
    }
    # Keep every run's meta: later runs (resume) append to the list.
    history = json.loads(meta_path.read_text()) if meta_path.exists() else []
    history.append(meta)
    meta_path.write_text(json.dumps(history, indent=2, default=str))

    if not df.empty:
        write_dataframe(
            df,
            parquet_path,
            lineage={
                "source": f"{runner.name} API, one fetch per sampled home URL, vendor cache disabled",
                "created_by": f"vdl_tools.scrape_enrich.scraper.vendor_pilot.run_{runner.name}",
                "git_sha": git_sha(),
                "sample": str(out_dir / SAMPLE_FILENAME),
                "runs": history,
            },
        )
    logger.info("%s: wrote %s rows -> %s", runner.name, len(df), parquet_path)
    return df


async def _safe_credits(runner: VendorRunner, client: httpx.AsyncClient):
    try:
        return await runner.credits(client)
    except Exception as exc:  # noqa: BLE001 - balance is best effort
        logger.warning("%s: could not read account balance: %s", runner.name, exc)
        return {"error": str(exc)[:200]}


def add_runner_args(parser, runner_cls: type[VendorRunner]):
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--concurrency", type=int, default=runner_cls.default_concurrency)
    parser.add_argument("--rpm", type=float, default=runner_cls.default_rpm,
                        help="max vendor API calls per minute (0 = unlimited)")
    parser.add_argument("--limit", type=int, default=None, help="only the first N sampled URLs (smoke test)")
    parser.add_argument("--no-resume", action="store_true", help="discard previous JSONL results and start over")
    parser.add_argument("--dry-run", action="store_true",
                        help="print what the run would fetch (and any budget) without calling the vendor")


def run_cli(runner_cls: type[VendorRunner], argv=None, extra_args=None, build=None):
    """``extra_args(parser)`` adds vendor flags; ``build(args)`` constructs the
    runner from them (default: ``runner_cls(get_api_key(runner_cls.name))``)."""
    import argparse

    from vdl_tools.shared_tools.parquet_cache import read_dataframe

    parser = argparse.ArgumentParser(description=runner_cls.__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    add_runner_args(parser, runner_cls)
    if extra_args:
        extra_args(parser)
    args = parser.parse_args(argv)
    sample = read_dataframe(args.out_dir / SAMPLE_FILENAME)
    runner = build(args) if build else runner_cls(get_api_key(runner_cls.name))
    if args.dry_run:
        jsonl_path, _, _ = _results_paths(args.out_dir, runner.name)
        todo, done_keys = plan_run(sample, jsonl_path, limit=args.limit, resume=not args.no_resume)
        print(f"{runner.name}: would fetch {len(todo)} URLs ({len(done_keys)} already in {jsonl_path.name}, "
              f"{api_calls_so_far(jsonl_path) if not args.no_resume else 0} API calls so far); "
              f"concurrency={args.concurrency} rpm={args.rpm or None}")
        print(todo["stratum"].value_counts().to_string())
        extra = asyncio.run(runner.run_context())
        if extra:
            print("runner context:", json.dumps(extra, default=str))
        return
    df = asyncio.run(run_vendor(
        runner, sample, args.out_dir,
        concurrency=args.concurrency, rpm=args.rpm or None,
        limit=args.limit, resume=not args.no_resume,
    ))
    if not df.empty:
        print(df.groupby("stratum")["success"].agg(["count", "mean"]).to_string())
