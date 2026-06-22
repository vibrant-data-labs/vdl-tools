#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-time backfill: S3 geocode cache -> Postgres `geocode` table.

Loads the existing S3 cache (bucket `geocode-cache`) into the new Postgres table so
already-geocoded addresses are not re-billed to Google after switching to v2.

S3 key layout written by the v1 cache (`geocode_cache.GeocodeCache`):
  - `<address>.json`         -> successful result (JSON body: latitude/longitude/
                                city/state/country/raw)
  - `errors/<address>.json`  -> failed lookup (empty body)
  - `hashed_<uuid>.json`     -> result for an address >100 chars; the original
                                address is NOT recoverable from the key, so it
                                cannot be re-keyed to match future lookups -> SKIPPED
                                (logged). Those few re-geocode on next encounter.

Errors are upserted first, then successes, so an address that has both an old error
marker and a later successful result ends up successful (num_errors cleared).

Run:
    python -m vdl_tools.scrape_enrich.geocode_cache_backfill --limit 500   # dry-ish test
    python -m vdl_tools.scrape_enrich.geocode_cache_backfill               # full backfill
"""
import argparse
import json
from multiprocessing.pool import ThreadPool

import boto3
from more_itertools import chunked

import vdl_tools.shared_tools.tools.config_utils as config_tools
from vdl_tools.scrape_enrich.geocode_cache import (
    S3_DEFAULT_BUCKET_NAME,
    S3_DEFAULT_BUCKET_REGION,
)
from vdl_tools.scrape_enrich.geocode_cache_sql import GeocodeCache, DEFAULT_PROVIDER
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger

_JSON_SUFFIX = ".json"
_ERROR_PREFIX = "errors/"


def _build_s3_client(config=None):
    """Boto3 S3 client + bucket name, resolved the same way as v1 GeocodeCache."""
    config = config or config_tools.get_configuration()
    bucket = config_tools.get_configuration_value(
        config, 'aws.geocode_cache_bucket', S3_DEFAULT_BUCKET_NAME)
    region = config_tools.get_configuration_value(
        config, 'aws.region', S3_DEFAULT_BUCKET_REGION)
    client = boto3.client(
        "s3",
        aws_access_key_id=config_tools.get_configuration_value(config, 'aws.access_key_id'),
        aws_secret_access_key=config_tools.get_configuration_value(config, 'aws.secret_access_key'),
        region_name=region,
    )
    return client, bucket


def _list_all_keys(client, bucket, prefix=None, limit=None):
    paginator = client.get_paginator('list_objects_v2')
    kwargs = {'Bucket': bucket}
    if prefix:
        kwargs['Prefix'] = prefix
    keys = []
    for page in paginator.paginate(**kwargs):
        keys.extend(obj['Key'] for obj in page.get('Contents', []))
        if limit and len(keys) >= limit:  # stop early for --limit test runs
            break
    return keys


def _get_body(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')


def backfill(
    config=None,
    prefix=None,
    limit=None,
    n_per_commit=1000,
    max_workers=20,
    provider=DEFAULT_PROVIDER,
):
    config = config or config_tools.get_configuration()
    client, bucket = _build_s3_client(config)

    logger.info("Listing objects in s3 bucket '%s'%s", bucket, f" (prefix={prefix})" if prefix else "")
    keys = _list_all_keys(client, bucket, prefix=prefix, limit=limit)
    if limit:
        keys = keys[:limit]
    logger.info("Found %s objects", len(keys))

    # classify keys
    success_keys: list[str] = []
    error_addresses: list[str] = []
    skipped_hashed = 0
    for key in keys:
        if not key.endswith(_JSON_SUFFIX):
            continue
        if key.startswith(_ERROR_PREFIX):
            addr = key[len(_ERROR_PREFIX):-len(_JSON_SUFFIX)]
            if addr.startswith("hashed_"):
                skipped_hashed += 1
                continue
            error_addresses.append(addr)
        else:
            stem = key[:-len(_JSON_SUFFIX)]
            if stem.startswith("hashed_"):
                skipped_hashed += 1
                continue
            success_keys.append(key)

    logger.info(
        "%s success objects, %s error markers, %s hashed (skipped)",
        len(success_keys), len(error_addresses), skipped_hashed,
    )

    # The cache class needs Google creds to build its geolocator, but the backfill
    # never geocodes — pass empty creds (no network call happens at construction).
    with get_session() as session:
        cache = GeocodeCache(session, user="backfill", key="backfill", provider=provider)

        # Errors first so a later successful result wins (clears num_errors).
        n_errors = 0
        for chunk in chunked(error_addresses, n_per_commit):
            rows = [cache._build_error_row(addr) for addr in chunk]
            cache._upsert_error_rows(rows)
            session.commit()
            n_errors += len(rows)
            logger.info("Upserted %s/%s error rows", n_errors, len(error_addresses))

        # Successes: fetch bodies in parallel, parse, bulk upsert per chunk.
        def _fetch_and_build(key):
            try:
                body = _get_body(client, bucket, key)
                data = json.loads(body)
            except Exception as ex:  # noqa: BLE001 - skip unreadable/garbage objects
                logger.warning("Skipping unreadable object %s: %s", key, ex)
                return None
            address = key[:-len(_JSON_SUFFIX)]
            # stored location_data already has latitude/longitude/city/state/country/raw
            return cache._build_success_row(address, data)

        n_success = 0
        with ThreadPool(processes=max_workers) as pool:
            for chunk in chunked(success_keys, n_per_commit):
                rows = [r for r in pool.map(_fetch_and_build, chunk) if r]
                cache._upsert_success_rows(rows)
                session.commit()
                n_success += len(rows)
                logger.info("Upserted %s/%s success rows", n_success, len(success_keys))

    logger.info(
        "Backfill complete: %s success, %s errors, %s hashed skipped",
        n_success, n_errors, skipped_hashed,
    )
    return {"success": n_success, "errors": n_errors, "skipped_hashed": skipped_hashed}


def main():
    parser = argparse.ArgumentParser(description="Backfill S3 geocode cache into Postgres.")
    parser.add_argument("--prefix", default=None, help="Only backfill keys under this S3 prefix.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N keys (test runs).")
    parser.add_argument("--n-per-commit", type=int, default=1000, help="Rows per bulk upsert/commit.")
    parser.add_argument("--max-workers", type=int, default=20, help="Parallel S3 GET workers.")
    args = parser.parse_args()
    backfill(
        prefix=args.prefix,
        limit=args.limit,
        n_per_commit=args.n_per_commit,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
