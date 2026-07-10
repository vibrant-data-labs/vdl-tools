
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


PROD_BASE_URL = "https://api.netzeroinsights.com"
SANDBOX_BASE_URL = "https://20.108.20.67"

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
AUTH_STATUS_CODES = frozenset({401, 403})

DEFAULT_SEARCH_CHECKPOINT_DIR = os.environ.get(
    "NZI_SEARCH_CHECKPOINT_DIR",
    os.path.expanduser("~/.cache/vdl-tools/nzi_search"),
)


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
        if not target_exists(self._meta_uri):
            return None
        meta = read_json(self._meta_uri)
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
    ):
        """Initialize the API client with credentials.

        Args:
            username: NetZero Insights API username
            password: NetZero Insights API password
            use_sandbox: Whether to use the sandbox environment
            max_concurrent_requests: Cap on simultaneous requests per detail
                batch. Note that stages fetched in parallel (e.g. startup
                details + funding rounds) each get their own cap, so peak
                concurrency against the API can be a small multiple of this.
        """
        logger.info(f"Initializing NetZero API client with {'sandbox' if use_sandbox else 'production'} environment")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.base_url = SANDBOX_BASE_URL if use_sandbox else PROD_BASE_URL
        self.read_from_cache = read_from_cache
        self.write_to_cache = write_to_cache
        self.use_sandbox = use_sandbox
        self.max_concurrent_requests = max_concurrent_requests
        # The production ssl certifcate seems to expired too
        self.verify_ssl = not use_sandbox
        # self.verify_ssl = False

        self._auth_lock = threading.Lock()
        self._auth_generation = 0
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the API and store the session cookie."""
        logger.info("Authenticating with NetZero API")
        try:
            response = self.session.post(
                f"{self.base_url}/security/formLogin",
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
            response = self.session.get(f"{self.base_url}/security/logout", verify=self.verify_ssl)
            response.raise_for_status()
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
        url = os.path.join(self.base_url, endpoint)
        reauthed = False
        response = None
        for attempt in range(max_retries):
            auth_generation = self._auth_generation
            try:
                response = self.session.request(
                    method, url, verify=self.verify_ssl, **kwargs,
                )
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Request to {endpoint} failed after {max_retries} attempts: {e}")
                    raise
                wait = 2 ** attempt + random.random()
                logger.warning(f"Request to {endpoint} failed ({e}), retrying in {wait:.1f}s")
                time.sleep(wait)
                continue

            if response.status_code in AUTH_STATUS_CODES and not reauthed:
                logger.warning(f"HTTP {response.status_code} from {endpoint} — re-authenticating")
                self._reauthenticate(auth_generation)
                reauthed = True
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                wait = 2 ** attempt + random.random()
                logger.warning(f"HTTP {response.status_code} from {endpoint}, retrying in {wait:.1f}s")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        # Only reachable if the final attempt was spent on a re-auth retry
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
        headers: Dict = None
    ) -> Dict:
        """Post a resource to the API."""
        return self._request_with_retries("POST", endpoint, json=payload, headers=headers)

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
        endpoint: str,
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
            endpoint: The API endpoint to call (e.g., 'companies', 'fundingRounds', 'investors')
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
        logger.info(f"Fetching {endpoint} with offset={offset}")

        filter = filter or MainFilter()
        payload = filter.model_dump()
        if sorting:
            payload["sorting"] = sorting.model_dump()
        # limit/offset are set per page request
        payload.pop("limit", None)
        payload.pop("offset", None)

        if limit is not None and limit <= page_size:
            # Small enough for a single request — no pagination or checkpoint needed
            data = self._post(
                endpoint=endpoint,
                payload={**payload, "limit": limit, "offset": offset},
                headers={"Content-Type": "application/json"},
            )
            results = data.get("results", [])
            logger.info(f"Successfully fetched {len(results)} `{endpoint}`")
            return {
                "total_count": data.get("count", 0),
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
            checkpoint = SearchCheckpoint(checkpoint_dir, endpoint, payload, page_size)
            meta = checkpoint.load_meta(timedelta(days=checkpoint_max_age_days))
            if meta:
                logger.info(f"Resuming search from checkpoint {checkpoint.base_uri}")

        def fetch_page(page_offset: int, allow_cached: bool) -> Dict:
            if allow_cached and checkpoint and checkpoint.has_page(page_offset):
                return checkpoint.read_page(page_offset)
            data = self._post(
                endpoint=endpoint,
                payload={**payload, "limit": page_size, "offset": page_offset},
                headers={"Content-Type": "application/json"},
            )
            page = {"count": data.get("count", 0), "results": data.get("results", [])}
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
            page_results = pages[page_offset].get("results", [])
            if not page_results:
                break
            results.extend(page_results)

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
        """Get the total number of startups matching the specified criteria."""
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
            endpoint="companies",
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
            endpoint="fundingRounds",
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
            endpoint="investors",
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
        batch_size: int = 20
    ) -> List[Dict]:
        """Efficiently fetch multiple entities with caching and async API requests.

        Args:
            ids: List of entity IDs to fetch
            endpoint: The API endpoint to call
            model_class: The class to instantiate with the response data
            read_from_cache: Whether to read from cache
            write_to_cache: Whether to write to cache
            batch_size: Number of entities to commit to database at once

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
            "generation": self._auth_generation,
        }
        if not auth_state["cookies"]:
            logger.warning("No session cookies found — requests may fail auth")
        request_timeout = aiohttp.ClientTimeout(total=90)
        max_retries = 3
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def fetch_entity(session: aiohttp.ClientSession, id: int) -> Dict:
            async with semaphore:
                reauthed = False
                for attempt in range(max_retries):
                    try:
                        async with session.get(
                            f"{self.base_url}/{endpoint.rstrip('/')}/{id}",
                            cookies=auth_state["cookies"],
                            headers={"Accept": "application/json, text/plain, */*"},
                            timeout=request_timeout,
                            ssl=self.verify_ssl,
                        ) as response:
                            if response.status in AUTH_STATUS_CODES and not reauthed:
                                # Session cookie likely expired mid-run; refresh
                                # once (generation-guarded, so concurrent tasks
                                # trigger a single re-login) and retry
                                logger.warning(f"HTTP {response.status} for {endpoint} {id} — re-authenticating")
                                auth_state["generation"] = await asyncio.to_thread(
                                    self._reauthenticate, auth_state["generation"]
                                )
                                auth_state["cookies"] = self.session.cookies.get_dict()
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
            endpoint="getStartup",
            model_class=Startup,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
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
            endpoint="getInvestor",
            model_class=Investor,
            primary_key_field="investorID",
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
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
            endpoint="commercial-deals/connected-entities/company",
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
            endpoint="fundingRound/prints",
            model_class=CompanyFundingRounds,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
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
        funding_round = await self._get_entity_details(
            id=funding_round_id,
            endpoint="fundingRound",
            model_class=CompanyFundingRounds,
            primary_key_field="id",
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )
        return funding_round

    def get_taxonomy_children(self, parent_id: int) -> List[Dict]:
        """Get taxonomy for a specific parent ID."""
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
