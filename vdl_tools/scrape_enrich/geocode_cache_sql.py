# -*- coding: utf-8 -*-
"""Postgres-backed geocode cache.

Mirrors `vdl_tools.shared_tools.openai.embedding_cache.EmbeddingCache`: one bulk
SQL read for all addresses up front, geocode only the misses, then chunked bulk
upserts (`INSERT ... ON CONFLICT DO UPDATE`). Replaces the one-object-at-a-time
S3/filesystem cache in `geocode_cache.py`.

Like `EmbeddingCache` owns the OpenAI call, this class owns the Google geocoder.
Google geocodes one address per request (and is rate-limited), so the "run the
misses" step is a sequential rate-limited loop; the speedup is on the cache I/O.
"""
import datetime as dt
from typing import Any

from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from vdl_tools.scrape_enrich.geocode_cache import get_component
from vdl_tools.shared_tools.database_cache.database_models.geocode import Geocode
from vdl_tools.shared_tools.tools.logger import logger


DEFAULT_PROVIDER = 'google'


class GeocodeCache:
    """SQL-backed cache + Google geocoder for forward geocoding."""

    def __init__(
        self,
        session: Session,
        user: str,
        key: str,
        provider: str = DEFAULT_PROVIDER,
        min_delay_seconds: float = 1 / 50,
    ):
        self.session = session
        self.provider = provider
        self.geolocator = GoogleV3(user_agent=user, api_key=key)
        # auto rate limiter (same cadence as geocode.py v1)
        self.geocode_rate_limited = RateLimiter(
            self.geolocator.geocode, min_delay_seconds=min_delay_seconds
        )

    def geocode_one(self, address: str):
        """Single rate-limited Google geocode call.

        Returns a location dict (latitude, longitude, city, state, country, raw)
        on a hit, or None when Google returns no result.
        """
        data = self.geocode_rate_limited(address)
        if not data:
            return None
        return {
            'latitude': data.latitude,
            'longitude': data.longitude,
            'city': get_component(data, "locality"),
            'state': get_component(data, "administrative_area_level_1"),
            'country': get_component(data, "country"),
            'raw': data.raw,
        }

    def get_geocode_obj(self, address: str):
        address_id = Geocode.create_address_id(address)
        return (
            self.session
            .query(Geocode)
            .filter(
                Geocode.provider == self.provider,
                Geocode.address_id == address_id,
            )
            .first()
        )

    def get_geocode_obj_bulk(self, addresses: list[str]):
        """Bulk-read cached rows for many addresses.

        Returns (found_rows, unfound_ids_or_errors):
          - found_rows: successful (non-errored) Geocode rows present in the DB
          - unfound_ids_or_errors: {address_id -> num_errors} for address_ids that
            are absent (0) or present-but-errored (>0)
        """
        logger.info(
            "Starting to pull %s previous results for provider: %s",
            len(addresses),
            self.provider,
        )
        address_ids = [Geocode.create_address_id(a) for a in addresses]
        all_rows = (
            self.session
            .query(Geocode)
            .filter(
                Geocode.provider == self.provider,
                Geocode.address_id.in_(address_ids),
            )
            .all()
        )

        found_rows_to_errors = {x.address_id: x.num_errors for x in all_rows}
        found_rows = [x for x in all_rows if not found_rows_to_errors.get(x.address_id)]

        found_rows_keys = found_rows_to_errors.keys()
        unfound_ids_or_errors = {
            x: found_rows_to_errors.get(x, 0) for x in address_ids
            if x not in found_rows_keys or found_rows_to_errors.get(x)
        }
        logger.info("%s previous found, %s unfound", len(found_rows), len(unfound_ids_or_errors))
        return found_rows, unfound_ids_or_errors

    def _build_success_row(self, address: str, location: dict) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "address_id": Geocode.create_address_id(address),
            "address": address,
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
            "city": location.get("city"),
            "state": location.get("state"),
            "country": location.get("country"),
            "response_full": location.get("raw"),
            "num_errors": None,
        }

    def _build_error_row(self, address: str, response_full: dict = None) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "address_id": Geocode.create_address_id(address),
            "address": address,
            "latitude": None,
            "longitude": None,
            "city": None,
            "state": None,
            "country": None,
            "response_full": response_full or {"message": "No geocode result"},
            "num_errors": 1,
        }

    def _upsert_success_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert successful rows via PG ON CONFLICT.

        Composite PK is (provider, address_id). On conflict, overwrites the
        location fields, clears num_errors, and bumps date_updated (Core doesn't
        fire the ORM `onupdate` hook through `ON CONFLICT DO UPDATE`).
        """
        if not rows:
            return
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            deduped[(row["provider"], row["address_id"])] = row
        rows = list(deduped.values())

        # Columns are naive `DateTime` (BaseMixin); strip tz to keep round-trip identical.
        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(Geocode).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["provider", "address_id"],
            set_={
                "address": stmt.excluded.address,
                "latitude": stmt.excluded.latitude,
                "longitude": stmt.excluded.longitude,
                "city": stmt.excluded.city,
                "state": stmt.excluded.state,
                "country": stmt.excluded.country,
                "response_full": stmt.excluded.response_full,
                "num_errors": None,
                # date_added intentionally NOT in the SET clause — preserve the
                # original "first cached at" timestamp on refresh.
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    def _upsert_error_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert error rows, incrementing num_errors on conflict."""
        if not rows:
            return
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            deduped[(row["provider"], row["address_id"])] = row
        rows = list(deduped.values())

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(Geocode).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["provider", "address_id"],
            set_={
                "response_full": stmt.excluded.response_full,
                "num_errors": func.coalesce(Geocode.num_errors, 0) + 1,
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    @staticmethod
    def _component_from_response(response_full, component_type: str):
        """Read one address component out of a stored Google response.

        ``get_component`` takes a geopy object (via ``.raw``); the cached
        ``response_full`` is already a plain dict, so it needs its own reader.

        Parameters
        ----------
        response_full : dict or None
            The stored raw Google response.
        component_type : str
            e.g. ``'administrative_area_level_2'``.

        Returns
        -------
        str or None
            The component's ``long_name``.
        """
        if not isinstance(response_full, dict):
            return None
        for component in response_full.get("address_components", []):
            if component_type in component.get("types", []):
                return component.get("long_name")
        return None

    @classmethod
    def _row_to_location(cls, row) -> dict:
        """Map a Geocode row (or success-row dict) to the pipeline location dict."""
        getter = row.get if isinstance(row, dict) else lambda k: getattr(row, k)
        return {
            "latitude": getter("latitude"),
            "longitude": getter("longitude"),
            "city": getter("city"),
            "state": getter("state"),
            "country": getter("country"),
            # County (US) / second-level admin area elsewhere. Read from the
            # stored response rather than a new column, so there is no
            # migration and every already-cached address gains it for free.
            # Google omits it exactly where the city IS the county equivalent
            # (Washington DC, Baltimore city, Virginia's independent cities),
            # so callers needing full coverage fall back to `city`.
            "county": cls._component_from_response(
                getter("response_full"), "administrative_area_level_2"
            ),
        }

    def bulk_get_cache_or_run(
        self,
        addresses: list[str],
        use_cached_result: bool = True,
        n_per_commit: int = 500,
        max_errors: int = 1,
    ) -> dict[str, dict]:
        """Bulk lookup/geocode.

        For each (normalized) address, return the cached location or geocode it and
        store the result. Addresses already cached with >= max_errors are treated as
        permanent failures and are NOT re-geocoded (default max_errors=1 means a
        single recorded failure is respected — fixes v1 re-paying Google every run).

        Returns: {address -> {latitude, longitude, city, state, country}}
        """
        # dedupe (preserve order) and drop empties
        unique_addresses = list(dict.fromkeys(a for a in addresses if a))
        if not unique_addresses:
            return {}

        if use_cached_result:
            found_rows, unfound_ids_errors = self.get_geocode_obj_bulk(unique_addresses)
            unfound_addresses = []
            for address in unique_addresses:
                address_id = Geocode.create_address_id(address)
                errors_for_id = unfound_ids_errors.get(address_id, 0)
                if (
                    address_id in unfound_ids_errors and
                    (errors_for_id == 0 or errors_for_id < max_errors)
                ):
                    unfound_addresses.append(address)
        else:
            unfound_addresses = unique_addresses
            found_rows = []

        res = {x.address: self._row_to_location(x) for x in found_rows}

        len_unfound = len(unfound_addresses)
        logger.info("Found %s cached geocodes", len(res))
        logger.info("Need to geocode %s addresses", len_unfound)

        if not unfound_addresses:
            return res

        success_rows: list[dict[str, Any]] = []
        error_rows: list[dict[str, Any]] = []
        n_run = 0
        try:
            for address in unfound_addresses:
                location = self.geocode_one(address)
                if location:
                    row = self._build_success_row(address, location)
                    success_rows.append(row)
                    res[address] = self._row_to_location(row)
                else:
                    error_rows.append(self._build_error_row(address))
                n_run += 1

                if n_run % n_per_commit == 0:
                    self._upsert_success_rows(success_rows)
                    self._upsert_error_rows(error_rows)
                    self.session.commit()
                    logger.info("Committed %s of %s geocodes", n_run, len_unfound)
                    success_rows, error_rows = [], []
        except KeyboardInterrupt:
            logger.warning(
                "(Geocode) Received KeyboardInterrupt, committing partial progress..."
            )

        # flush remainder
        self._upsert_success_rows(success_rows)
        self._upsert_error_rows(error_rows)
        self.session.commit()
        logger.info("Total geocodes in result: %s", len(res))
        return res
