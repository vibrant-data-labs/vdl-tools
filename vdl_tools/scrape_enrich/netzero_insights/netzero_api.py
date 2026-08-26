
import asyncio
import hashlib
import json
import math
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Type

import aiohttp
import requests

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.netzero_insights.filters import (
    Sorting, MainFilter,
)
from vdl_tools.scrape_enrich.netzero_insights import api_v2
from vdl_tools.shared_tools.json_cache import read_json, write_json, target_exists

from vdl_tools.shared_tools.database_cache.database_models import (
    CompanyCommercialDeal,
    CompanyFundingRounds,
    Investor,
    Startup,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Legacy ("[OLD]" in NZI's docs) API. Supported by NZI until 2027-02-28.
PROD_BASE_URL = "https://api.netzeroinsights.com"
SANDBOX_BASE_URL = "https://20.108.20.67"

# Current API. See api_v2 for the endpoint/filter/response differences.
PROD_BASE_URL_V2 = api_v2.PROD_BASE_URL_V2
SANDBOX_BASE_URL_V2 = api_v2.STAGE_BASE_URL_V2

# Default API version for new clients. Still "v1" because the legacy API is
# supported until 2027-02-28 and the v2 response mapping has not yet been
# checked against live credentials — see README before flipping this.
DEFAULT_API_VERSION = os.environ.get("NZI_API_VERSION", "v1")

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
# v1 expired *session cookies* could surface as either 401 or 403, so both
# triggered a re-login. v2 documents 403 as "insufficient access level" — a
# permissions error that re-authenticating cannot fix — so only 401 means
# "token expired" there.
AUTH_STATUS_CODES = frozenset({401, 403})
AUTH_STATUS_CODES_V2 = frozenset({401})

DEFAULT_SEARCH_CHECKPOINT_DIR = os.environ.get(
    "NZI_SEARCH_CHECKPOINT_DIR",
    os.path.expanduser("~/.cache/vdl-tools/nzi_search"),
)


def _normalize_deal_list(payload):
    """Normalise `GET /deals/company/{id}`, which returns a bare list."""
    if isinstance(payload, list):
        return [api_v2.normalize_deal(deal) for deal in payload]
    return payload


class SearchCheckpoint:
    """Per-page checkpoint store for a paginated search.

    Pages are keyed by offset under a directory derived from a hash of the
    full search payload, so a checkpoint can never be resumed against a
    different query. Storage goes through ``json_cache`` (local path or
    ``s3://``).
    """

    def __init__(self, base_dir: str, endpoint: str, payload: Dict, page_size: int):
        self.payload = payload
        self.page_size = page_size
        self.endpoint = endpoint
        key_material = json.dumps(
            {"endpoint": endpoint, "payload": payload, "page_size": page_size},
            sort_keys=True,
            default=str,
        )
        self.search_key = hashlib.sha256(key_material.encode("utf-8")).hexdigest()[:16]
        self.base_uri = f"{str(base_dir).rstrip('/')}/{self.search_key}"

    def _page_uri(self, offset: int) -> str:
        return f"{self.base_uri}/page_{offset:09d}.json.gz"

    @property
    def _meta_uri(self) -> str:
        return f"{self.base_uri}/meta.json"

    def load_meta(self, max_age: timedelta) -> Optional[Dict]:
        """Return the checkpoint metadata, or None if absent or older than max_age."""
        try:
            meta = read_json(self._meta_uri)
        except FileNotFoundError:
            return None
        created_at = datetime.fromisoformat(meta["created_at"])
        if datetime.now(timezone.utc) - created_at > max_age:
            logger.info(
                "Search checkpoint %s is older than %s — refetching all pages",
                self.base_uri, max_age,
            )
            return None
        return meta

    def write_meta(self, total_count: int) -> None:
        write_json(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "endpoint": self.endpoint,
                "payload": self.payload,
                "page_size": self.page_size,
                "total_count": total_count,
            },
            self._meta_uri,
        )

    def has_page(self, offset: int) -> bool:
        return target_exists(self._page_uri(offset))

    def read_page(self, offset: int) -> Dict:
        return read_json(self._page_uri(offset))

    def write_page(self, offset: int, page: Dict) -> None:
        write_json(page, self._page_uri(offset))


class NetZeroAPI:
    """Client for interacting with the NetZero Insights API."""

    def __init__(
        self,
        username: str,
        password: str,
        use_sandbox: bool = False,
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        max_concurrent_requests: int = 10,
        api_version: str = None,
    ):
        """Initialize the API client with credentials.

        Args:
            username: NetZero Insights API username (the account email — v2
                sends it as the ``email`` login parameter)
            password: NetZero Insights API password
            use_sandbox: Whether to use the sandbox/staging environment
            max_concurrent_requests: Cap on simultaneous requests per detail
                batch. Note that stages fetched in parallel (e.g. startup
                details + funding rounds) each get their own cap, so peak
                concurrency against the API can be a small multiple of this.
            api_version: ``"v1"`` for the legacy cookie-authenticated API
                (default, supported by NZI until 2027-02-28) or ``"v2"`` for
                the current bearer-token API.
        """
        api_version = (api_version or DEFAULT_API_VERSION).lower()
        if api_version not in ("v1", "v2"):
            raise ValueError(f"api_version must be 'v1' or 'v2', got {api_version!r}")
        self.api_version = api_version
        self.is_v2 = api_version == "v2"

        logger.info(
            "Initializing NetZero API client (%s, %s environment)",
            api_version, "sandbox" if use_sandbox else "production",
        )
        self.username = username
        self.password = password
        self.session = requests.Session()
        if self.is_v2:
            self.base_url = SANDBOX_BASE_URL_V2 if use_sandbox else PROD_BASE_URL_V2
        else:
            self.base_url = SANDBOX_BASE_URL if use_sandbox else PROD_BASE_URL
        self.endpoints = api_v2.ENDPOINTS_V2 if self.is_v2 else api_v2.ENDPOINTS_V1
        self.auth_status_codes = AUTH_STATUS_CODES_V2 if self.is_v2 else AUTH_STATUS_CODES
        self.read_from_cache = read_from_cache
        self.write_to_cache = write_to_cache
        self.use_sandbox = use_sandbox
        self.max_concurrent_requests = max_concurrent_requests
        # The v1 sandbox is a bare IP with an expired certificate. The v2
        # environments are both proper hosts, so certificates are always
        # verified there.
        self.verify_ssl = self.is_v2 or not use_sandbox

        self._access_token = None
        self._auth_lock = threading.Lock()
        self._auth_generation = 0
        self._authenticate()

    @staticmethod
    def _extract_access_token(response: requests.Response) -> Optional[str]:
        """Pull the v2 JWT out of a login response.

        NZI's docs show the token in the response headers (that is what the
        ``-v`` flag in their curl example is for), but the JSON body carries it
        too on some deployments, so both are checked.
        """
        for header in ("access_token", "Access-Token", "Authorization", "authorization"):
            value = response.headers.get(header)
            if value:
                return value.replace("Bearer ", "").strip()
        try:
            body = response.json()
        except ValueError:
            return None
        if isinstance(body, dict):
            for key in ("access_token", "accessToken", "token"):
                if body.get(key):
                    return str(body[key])
        return None

    def _auth_headers(self) -> Dict:
        """Bearer header for v2; v1 authenticates with the session cookie."""
        if self.is_v2 and self._access_token:
            return {"Authorization": f"Bearer {self._access_token}"}
        return {}

    def _authenticate(self) -> None:
        """Authenticate with the API, storing a bearer token (v2) or cookie (v1)."""
        logger.info("Authenticating with NetZero API (%s)", self.api_version)
        try:
            if self.is_v2:
                # v2 takes credentials as query params, not a form body.
                response = self.session.post(
                    f"{self.base_url}/{self.endpoints['login']}",
                    params={"email": self.username, "password": self.password},
                    verify=self.verify_ssl,
                )
                response.raise_for_status()
                token = self._extract_access_token(response)
                if not token:
                    raise RuntimeError(
                        "NZI v2 login returned no access_token in headers or body"
                    )
                self._access_token = token
            else:
                response = self.session.post(
                    f"{self.base_url}/{self.endpoints['login']}",
                    data={
                        "username": self.username,
                        "password": self.password
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    verify=self.verify_ssl,
                )
                response.raise_for_status()
            logger.info("Successfully authenticated with NetZero API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to authenticate with NetZero API: {str(e)}")
            raise

    def logout(self) -> None:
        """Logout from the API session."""
        logger.info("Logging out from NetZero API")
        try:
            url = f"{self.base_url}/{self.endpoints['logout']}"
            if self.is_v2:
                # v2's docs give the path as GET /auth/logout in prose but POST
                # in the curl example; POST is what their example actually runs.
                response = self.session.post(
                    url, headers=self._auth_headers(), verify=self.verify_ssl,
                )
            else:
                response = self.session.get(url, verify=self.verify_ssl)
            response.raise_for_status()
            self._access_token = None
            logger.info("Successfully logged out from NetZero API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to logout from NetZero API: {str(e)}")
            raise

    def _reauthenticate(self, seen_generation: int) -> int:
        """Re-login after an auth failure, at most once per expiry.

        Many threads/tasks can hit a 401 from the same expired cookie at the
        same time; the generation counter makes only the first one actually
        re-login, the rest just pick up the fresh session.
        """
        with self._auth_lock:
            if self._auth_generation == seen_generation:
                self._authenticate()
                self._auth_generation += 1
            return self._auth_generation

    def _request_with_retries(
        self,
        method: str,
        endpoint: str,
        max_retries: int = 4,
        **kwargs,
    ) -> Dict:
        """Issue a request, retrying transient failures and refreshing auth.

        Retries 429/5xx and connection errors with exponential backoff +
        jitter. A 401/403 triggers a single re-authentication (the session
        cookie expires on long runs) before retrying.
        """
        # Not os.path.join: an endpoint with a leading slash would make it
        # discard the base URL entirely.
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        reauthed = False
        response = None
        attempt = 0
        while attempt < max_retries:
            auth_generation = self._auth_generation
            request_kwargs = dict(kwargs)
            auth_headers = self._auth_headers()
            if auth_headers:
                request_kwargs["headers"] = {
                    **(request_kwargs.get("headers") or {}), **auth_headers,
                }
            try:
                response = self.session.request(
                    method, url, verify=self.verify_ssl, **request_kwargs,
                )
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Request to {endpoint} failed after {max_retries} attempts: {e}")
                    raise
                wait = 2 ** attempt + random.random()
                logger.warning(f"Request to {endpoint} failed ({e}), retrying in {wait:.1f}s")
                time.sleep(wait)
                attempt += 1
                continue

            if response.status_code in self.auth_status_codes and not reauthed:
                logger.warning(f"HTTP {response.status_code} from {endpoint} — re-authenticating")
                self._reauthenticate(auth_generation)
                reauthed = True
                # A re-auth doesn't consume a retry slot — otherwise a 401 on the
                # final attempt would refresh the session and then raise without
                # ever retrying with the fresh cookie.
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                wait = 2 ** attempt + random.random()
                logger.warning(f"HTTP {response.status_code} from {endpoint}, retrying in {wait:.1f}s")
                time.sleep(wait)
                attempt += 1
                continue

            response.raise_for_status()
            return response.json()

        # Reached only if the retry budget is exhausted; surface the last response.
        response.raise_for_status()
        return response.json()

    def _get(
        self,
        endpoint: str,
        params: Dict = None,
        headers: Dict = None
    ) -> Dict:
        """Get a resource from the API."""
        return self._request_with_retries("GET", endpoint, params=params, headers=headers)

    def _post(
        self,
        endpoint: str,
        payload: Dict,
        headers: Dict = None,
        params: Dict = None,
    ) -> Dict:
        """Post a resource to the API."""
        return self._request_with_retries(
            "POST", endpoint, json=payload, headers=headers, params=params,
        )

    def _resolve_cache_params(
        self,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> tuple[bool, bool]:
        if read_from_cache is None:
            read_from_cache = self.read_from_cache
        if write_to_cache is None:
            write_to_cache = self.write_to_cache
        return read_from_cache, write_to_cache

    def _search_entities(
        self,
        operation: str,
        filter: Optional[MainFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_max_age_days: float = 7.0,
        max_concurrent_pages: int = 8,
        unique_key: Optional[str] = None,
    ) -> Dict:
        """Base method for searching entities.

        Args:
            operation: Logical search operation — 'search_companies',
                'search_deals' or 'search_investors'. Resolved to a concrete
                path through the version's endpoint map.
            filter: Filter criteria for the entities
            sorting: Sorting criteria. Strongly recommended when checkpointing
                or fetching pages concurrently — without a stable order the API
                may shift rows between pages.
            limit: Maximum number of results to return (None for all results)
            offset: Number of results to skip
            page_size: Number of items per page when using pagination
            max_pages: Maximum number of pages to fetch (None for all pages)
            checkpoint_dir: If set, each fetched page is persisted under
                ``{checkpoint_dir}/{hash(query)}/`` and a re-run of the same
                query resumes from the pages already on disk / S3.
            checkpoint_max_age_days: Checkpoints older than this are refetched.
            max_concurrent_pages: Number of pages fetched in parallel.
            unique_key: Field name used to drop duplicate results across pages
                (e.g. ``clientID``); duplicates can appear when the underlying
                data shifts between page fetches.

        Returns:
            Dict containing:
                - total_count: Total number of available results
                - count: Number of results in this response
                - results: List of matching entities
        """
        endpoint = self.endpoints[operation]
        logger.info(f"Fetching {endpoint} with offset={offset}")

        filter = filter or MainFilter()
        if self.is_v2:
            # v2 renames and re-nests the filter, and moves limit/offset and
            # sorting out of the body into query params.
            payload = api_v2.to_v2_payload(operation, filter)
        else:
            payload = filter.model_dump()
            if sorting:
                payload["sorting"] = sorting.model_dump()
            # limit/offset are set per page request
            payload.pop("limit", None)
            payload.pop("offset", None)

        def request_page(page_offset: int, size: int) -> Dict:
            """Fetch one page, normalising both versions to {count, results}."""
            if self.is_v2:
                page_number, remainder = divmod(page_offset, size)
                if remainder:
                    raise ValueError(
                        f"NZI v2 paginates by page number, so offset ({page_offset}) "
                        f"must be a multiple of page_size ({size})."
                    )
                data = self._post(
                    endpoint=endpoint,
                    payload=payload,
                    headers={"Content-Type": "application/json"},
                    params=api_v2.to_v2_query_params(page_number, size, sorting),
                )
                return {
                    "count": data.get("totalElements", 0),
                    "results": api_v2.normalize_search_results(
                        operation, data.get("content") or [],
                    ),
                }
            data = self._post(
                endpoint=endpoint,
                payload={**payload, "limit": size, "offset": page_offset},
                headers={"Content-Type": "application/json"},
            )
            return {"count": data.get("count", 0), "results": data.get("results", [])}

        if limit is not None and limit <= page_size:
            # Small enough for a single request — no pagination or checkpoint needed
            page = request_page(offset, limit)
            results = page["results"]
            logger.info(f"Successfully fetched {len(results)} `{endpoint}`")
            return {
                "total_count": page["count"],
                "count": len(results),
                "results": results,
            }

        checkpoint = None
        meta = None
        if checkpoint_dir:
            if sorting is None:
                logger.warning(
                    "Checkpointed search without explicit sorting — if the "
                    "underlying data changes between runs, resumed pages may "
                    "contain duplicates or miss rows."
                )
            # The API version is part of the key: v1 and v2 return different
            # field names for the same query, so their pages must never be
            # resumed against each other.
            checkpoint = SearchCheckpoint(
                checkpoint_dir, f"{self.api_version}:{endpoint}", payload, page_size,
            )
            meta = checkpoint.load_meta(timedelta(days=checkpoint_max_age_days))
            if meta:
                logger.info(f"Resuming search from checkpoint {checkpoint.base_uri}")

        def fetch_page(page_offset: int, allow_cached: bool) -> Dict:
            if allow_cached and checkpoint:
                # Read straight through instead of has_page()+read_page(): the
                # existence probe is a redundant stat/HEAD (2 extra S3 round trips
                # per page on resume) that the read itself already performs.
                try:
                    return checkpoint.read_page(page_offset)
                except FileNotFoundError:
                    pass
            page = request_page(page_offset, page_size)
            if checkpoint:
                checkpoint.write_page(page_offset, page)
            return page

        # Only trust existing page files when resuming a fresh checkpoint —
        # otherwise stale pages from an expired run would be read back
        resuming = meta is not None

        # First page establishes total_count (from checkpoint meta when resuming)
        first_page = fetch_page(offset, allow_cached=resuming)
        total_count = meta["total_count"] if resuming else first_page["count"]
        if checkpoint and not resuming:
            checkpoint.write_meta(total_count)

        available = max(total_count - offset, 0)
        target = available if limit is None else min(limit, available)
        n_pages = math.ceil(target / page_size) if target else 1
        if max_pages is not None and n_pages > max_pages:
            logger.info(f"Capping fetch at max_pages={max_pages}")
            n_pages = max_pages

        pages = {offset: first_page}
        remaining_offsets = [offset + i * page_size for i in range(1, n_pages)]
        if remaining_offsets:
            logger.info(
                f"Fetching {len(remaining_offsets)} more pages of `{endpoint}` "
                f"({max_concurrent_pages} concurrent)"
            )
            with ThreadPoolExecutor(max_workers=max_concurrent_pages) as executor:
                futures = {
                    executor.submit(fetch_page, page_offset, resuming): page_offset
                    for page_offset in remaining_offsets
                }
                try:
                    for done, future in enumerate(as_completed(futures), start=2):
                        pages[futures[future]] = future.result()
                        if done % 25 == 0:
                            logger.info(f"Fetched {done}/{n_pages} pages of `{endpoint}`")
                except Exception:
                    # Completed pages are already checkpointed; a re-run resumes
                    executor.shutdown(cancel_futures=True)
                    raise

        results = []
        for page_offset in sorted(pages):
            # Don't break on an empty page: with concurrent fetches an interior
            # page can come back empty (transient/shifted data) while later
            # offsets returned valid rows — breaking here would silently drop them.
            results.extend(pages[page_offset].get("results", []))

        if unique_key:
            seen = set()
            deduped = []
            for result in results:
                key = result.get(unique_key)
                if key is not None and key in seen:
                    continue
                seen.add(key)
                deduped.append(result)
            if len(deduped) < len(results):
                logger.warning(
                    f"Dropped {len(results) - len(deduped)} duplicate "
                    f"`{endpoint}` results by {unique_key}"
                )
            results = deduped

        if limit is not None:
            results = results[:limit]

        logger.info(f"Fetched {len(results)} `{endpoint}` (total available: {total_count})")
        return {
            "total_count": total_count,
            "count": len(results),
            "results": results,
        }

    def get_startup_count(self, main_filter: MainFilter = None) -> int:
        """Get the total number of startups matching the specified criteria.

        v1 has a dedicated (undocumented) ``getStartupCount`` endpoint. v2 has
        none, so the count comes from ``totalElements`` on a one-row search —
        the same number, for one row of transfer.
        """
        if self.is_v2:
            return self._search_entities(
                operation="search_companies", filter=main_filter, limit=1,
            )["total_count"]
        response = self._post(
            endpoint="getStartupCount",
            payload=main_filter.model_dump() if main_filter else {},
        )
        return response["count"]

    def search_startups(
        self,
        main_filter: MainFilter = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_max_age_days: float = 7.0,
        max_concurrent_pages: int = 8,
    ) -> Dict:
        """Get a list of startups matching the specified criteria."""
        return self._search_entities(
            operation="search_companies",
            filter=main_filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages,
            checkpoint_dir=checkpoint_dir,
            checkpoint_max_age_days=checkpoint_max_age_days,
            max_concurrent_pages=max_concurrent_pages,
            unique_key="clientID",
        )

    def search_deals(
        self,
        filter: Optional[MainFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_max_age_days: float = 7.0,
        max_concurrent_pages: int = 8,
    ) -> Dict:
        """Get a list of deals matching the specified criteria."""
        return self._search_entities(
            operation="search_deals",
            filter=filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages,
            checkpoint_dir=checkpoint_dir,
            checkpoint_max_age_days=checkpoint_max_age_days,
            max_concurrent_pages=max_concurrent_pages,
        )

    def search_investors(
        self,
        filter: Optional[MainFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        checkpoint_dir: Optional[str] = None,
        checkpoint_max_age_days: float = 7.0,
        max_concurrent_pages: int = 8,
    ) -> Dict:
        """Get a list of investors matching the specified criteria."""
        return self._search_entities(
            operation="search_investors",
            filter=filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages,
            checkpoint_dir=checkpoint_dir,
            checkpoint_max_age_days=checkpoint_max_age_days,
            max_concurrent_pages=max_concurrent_pages,
            unique_key="investorID",
        )

    async def _get_details_batch(
        self,
        ids: List[int],
        endpoint: str,
        model_class: Type,
        primary_key_field: str = "clientID",
        read_from_cache: bool = None,
        write_to_cache: bool = None,
        batch_size: int = 20,
        normalizer=None,
    ) -> List[Dict]:
        """Efficiently fetch multiple entities with caching and async API requests.

        Args:
            ids: List of entity IDs to fetch
            endpoint: The API endpoint to call
            model_class: The class to instantiate with the response data
            read_from_cache: Whether to read from cache
            write_to_cache: Whether to write to cache
            batch_size: Number of entities to commit to database at once
            normalizer: Applied to each raw response before it is cached, so
                the DB holds one field vocabulary regardless of API version.
                Only used on v2 — v1 responses already use the legacy names.

        Returns:
            List of Dicts containing the entity details
        """
        logger.info(f"Fetching details for {len(ids)} `{endpoint}`s")
        results = {}
        missing_ids = set(ids)

        read_from_cache, write_to_cache = self._resolve_cache_params(read_from_cache, write_to_cache)

        if read_from_cache:
            with get_session() as session:
                logger.info(f"Checking database for {len(ids)} `{endpoint}`s")
                entities = session.query(model_class).filter(getattr(model_class, primary_key_field).in_(ids)).all()
                for entity in entities:
                    entity_id = getattr(entity, primary_key_field)
                    results[entity_id] = entity.to_dict()
                    missing_ids.remove(entity_id)
                logger.info(f"Found {len(results)} `{endpoint}`s in database")

        if not missing_ids:
            return [results[id] for id in ids]

        auth_state = {
            "cookies": self.session.cookies.get_dict(),
            "headers": self._auth_headers(),
            "generation": self._auth_generation,
        }
        if not auth_state["cookies"] and not auth_state["headers"]:
            logger.warning("No session cookies or bearer token found — requests may fail auth")
        request_timeout = aiohttp.ClientTimeout(total=90)
        max_retries = 3
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def fetch_entity(session: aiohttp.ClientSession, id: int) -> Dict:
            async with semaphore:
                reauthed = False
                for attempt in range(max_retries):
                    # Snapshot the auth generation *before* the request so the
                    # generation guard works: if a peer coroutine re-logins
                    # while this request is in flight, our seen generation is
                    # now stale and _reauthenticate skips a redundant re-login.
                    seen_generation = auth_state["generation"]
                    try:
                        async with session.get(
                            f"{self.base_url}/{endpoint.rstrip('/')}/{id}",
                            cookies=auth_state["cookies"],
                            headers={
                                "Accept": "application/json, text/plain, */*",
                                **auth_state["headers"],
                            },
                            timeout=request_timeout,
                            ssl=self.verify_ssl,
                        ) as response:
                            if response.status in self.auth_status_codes and not reauthed:
                                # Session cookie likely expired mid-run; refresh
                                # once (generation-guarded, so concurrent tasks
                                # trigger a single re-login) and retry
                                logger.warning(f"HTTP {response.status} for {endpoint} {id} — re-authenticating")
                                auth_state["generation"] = await asyncio.to_thread(
                                    self._reauthenticate, seen_generation
                                )
                                auth_state["cookies"] = self.session.cookies.get_dict()
                                auth_state["headers"] = self._auth_headers()
                                reauthed = True
                                continue
                            if response.status in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                                wait = 2 ** attempt + random.random()
                                logger.warning(
                                    f"HTTP {response.status} for {endpoint} {id} "
                                    f"(attempt {attempt + 1}/{max_retries}), retrying in {wait:.1f}s"
                                )
                                await asyncio.sleep(wait)
                                continue
                            if response.status >= 400:
                                body = (await response.text())[:500]
                                logger.error(f"Failed {endpoint} {id}: HTTP {response.status} | {body!r}")
                                return id, None
                            data = await response.json()
                            if normalizer is not None and self.is_v2:
                                data = normalizer(data)
                            args = {}
                            valid_columns = model_class.__table__.columns.keys()
                            base_cls = model_class.__bases__[0]
                            if isinstance(data, dict):
                                for k, v in data.items():
                                    if k in valid_columns and not hasattr(base_cls, k):
                                        args[k] = v
                            args[primary_key_field] = id
                            args["fullData"] = data
                            return id, args
                    except (TimeoutError, asyncio.TimeoutError, aiohttp.ClientError) as e:
                        if attempt < max_retries - 1:
                            logger.warning(
                                f"{type(e).__name__} {endpoint} {id} "
                                f"(attempt {attempt + 1}/{max_retries}), retrying..."
                            )
                            await asyncio.sleep(2 ** attempt + random.random())
                        else:
                            logger.error(f"Failed {endpoint} {id}: {type(e).__name__} after {max_retries} attempts")
                            return id, None
                    except Exception as e:
                        logger.error(f"Failed {endpoint} {id}: {type(e).__name__}: {e}")
                        return id, None
                return id, None

        async def process_batch(batch: List[Dict]):
            if not write_to_cache:
                return
            with get_session() as session:
                for id, data in batch:
                    if data is None:
                        continue
                    # Get the args for the model class but remove the base mixin args
                    # We filter for columns that exist in the model but are not defined in the base mixin
                    args = {}
                    valid_columns = model_class.__table__.columns.keys()
                    base_cls = model_class.__bases__[0]

                    for k, v in data.items():
                        if k in valid_columns and not hasattr(base_cls, k):
                            args[k] = v
                    args[primary_key_field] = id
                    args["fullData"] = data["fullData"]

                    entity = model_class(**args)
                    session.merge(entity)
                session.commit()

        async with aiohttp.ClientSession() as session:
            tasks = [fetch_entity(session, id) for id in missing_ids]
            batch = []
            for future in asyncio.as_completed(tasks):
                id, data = await future
                if data is not None:
                    results[id] = data
                    batch.append((id, data))
                    if len(batch) >= batch_size:
                        await process_batch(batch)
                        batch = []
            if batch:
                await process_batch(batch)

        unfound_ids = set(ids) - set(results.keys())
        total = len(ids)
        failed = len(unfound_ids)
        logger.info(f"Fetched {total - failed}/{total} `{endpoint}`s")
        if failed:
            logger.warning(f"{failed}/{total} `{endpoint}`s failed ({100 * failed / total:.1f}%): {unfound_ids}")
        return [results.get(id) for id in ids if id in results]

    async def get_startup_details(
        self,
        startup_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
        flatten: bool = True,
    ) -> List[Dict]:
        """Get detailed information about multiple startups."""
        startup_objects = await self._get_details_batch(
            ids=startup_ids,
            primary_key_field="clientID",
            endpoint=self.endpoints["company_details"],
            model_class=Startup,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            normalizer=api_v2.normalize_company,
        )
        if flatten:
            flat_data = []
            for startup in startup_objects:
                flat_data.append(startup['fullData'])
            return flat_data
        return startup_objects

    async def get_investor_details(
        self,
        investor_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
        flatten: bool = True,
    ) -> List[Dict]:
        """Get detailed information about multiple investors."""
        investor_objects = await self._get_details_batch(
            ids=investor_ids,
            endpoint=self.endpoints["investor_details"],
            model_class=Investor,
            primary_key_field="investorID",
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            normalizer=api_v2.normalize_investor,
        )
        if flatten:
            flat_data = []
            for investor in investor_objects:
                flat_data.append(investor['fullData'])
            return flat_data
        return investor_objects

    async def get_company_commercial_deals(
        self, company_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
        flatten: bool = True,
    ) -> List[Dict]:
        """Get commercial deals for a specific company."""
        deals = await self._get_details_batch(
            ids=company_ids,
            primary_key_field="clientID",
            endpoint=self.endpoints["company_commercial_deals"],
            model_class=CompanyCommercialDeal,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )
        if flatten:
            flat_data = []
            for company_deal_list in deals:
                for deal in company_deal_list['fullData']:
                    deal['clientID'] = company_deal_list['clientID']
                    flat_data.append(deal)
            return flat_data
        return deals

    async def get_company_funding_rounds(
        self,
        company_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
        flatten: bool = True,
    ) -> List[Dict]:
        """Get funding rounds for a specific company."""
        company_rounds_objects = await self._get_details_batch(
            ids=company_ids,
            primary_key_field="clientID",
            endpoint=self.endpoints["company_deals"],
            model_class=CompanyFundingRounds,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            normalizer=_normalize_deal_list,
        )
        if flatten:
            flat_data = []
            for company_rounds in company_rounds_objects:
                flat_data.extend(company_rounds['fullData'])
            return flat_data
        return company_rounds_objects

    async def _get_entity_details(
        self,
        id: int,
        endpoint: str,
        model_class: Type,
        primary_key_field: str = "clientID",
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> Dict:
        """Get detailed information about a specific entity."""
        entity = await self._get_details_batch(
            ids=[id],
            endpoint=endpoint,
            model_class=model_class,
            primary_key_field=primary_key_field,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )
        return entity[0]

    async def get_funding_round_details(
        self,
        funding_round_id: int,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> List[Dict]:
        """Get detailed information about multiple funding rounds."""
        if self.is_v2:
            # v2 documents no single-deal-by-ID endpoint; deals are reachable
            # per company (`GET /deals/company/{id}`) or via the deal search.
            raise NotImplementedError(
                "NZI v2 has no deal-details-by-ID endpoint. Use "
                "get_company_funding_rounds(company_ids) or search_deals()."
            )
        funding_round = await self._get_entity_details(
            id=funding_round_id,
            endpoint="fundingRound",
            model_class=CompanyFundingRounds,
            primary_key_field="id",
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )
        return funding_round

    def get_all_taxonomy_items(self) -> List[Dict]:
        """Get every taxonomy item (``GET /taxonomy/itemDtos``).

        Each item carries both an ``id`` and a separate ``tagID``. That
        distinction matters for migration: v1's ``taxonomyItems`` filter took
        item ``id``s, while v2's replacement ``tagIDs`` filter takes ``tagID``s.
        Use :meth:`get_taxonomy_item_tag_ids` to translate between them.
        """
        return self._get(endpoint="taxonomy/itemDtos")

    def get_taxonomy_item_tag_ids(self) -> Dict[int, int]:
        """Map taxonomy item ``id`` -> ``tagID`` for v1 -> v2 filter translation."""
        return {
            item["id"]: item["tagID"]
            for item in self.get_all_taxonomy_items()
            if item.get("id") is not None and item.get("tagID") is not None
        }

    def get_taxonomy_children(self, parent_id: int) -> List[Dict]:
        """Get taxonomy for a specific parent ID.

        NZI documents this as ``GET /taxonomy/graph/{parentID}`` and has not
        republished the taxonomy endpoints under the v2 host, so v2 clients
        issue the documented GET. v1 keeps the POST-with-body form that is
        already in production use here.
        """
        if self.is_v2:
            return self._get(endpoint=f"taxonomy/graph/{parent_id}")
        payload = {
            'onlyVisible': True,
            'onlyAdvancedFilters': False,
            'mainFilter': {
                'include': {},
                'exclude': {},
                'fundingRoundInclude': {},
                'fundingRoundExclude': {},
                'investorInclude': {},
                'investorExclude': {},
            },
            'onlySearchable': True
        }
        return self._post(
            endpoint=f"taxonomy/graph/{parent_id}",
            payload=payload
        )

    def get_taxonomy_children_recursive(self, parent_id: int, limit: int = 10, current_depth: int = 0) -> List[Dict]:
        """Get taxonomy for a specific parent ID and all its children."""
        children = self.get_taxonomy_children(parent_id)
        for child in children:
            if current_depth >= limit:
                break
            child['children'] = self.get_taxonomy_children_recursive(child['id'], limit, current_depth + 1)
        return children
