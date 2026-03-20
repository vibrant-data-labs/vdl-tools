
import os
import requests
import aiohttp
import asyncio
from typing import Dict, List, Optional, Union, Generator, Type
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.netzero_insights.filters import (
    Sorting, MainFilter,
)

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


class NetZeroAPI:
    """Client for interacting with the NetZero Insights API."""

    def __init__(
        self,
        username: str,
        password: str,
        use_sandbox: bool = False,
        read_from_cache: bool = True,
        write_to_cache: bool = True
    ):
        """Initialize the API client with credentials.

        Args:
            username: NetZero Insights API username
            password: NetZero Insights API password
            use_sandbox: Whether to use the sandbox environment
        """
        logger.info(f"Initializing NetZero API client with {'sandbox' if use_sandbox else 'production'} environment")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.base_url = SANDBOX_BASE_URL if use_sandbox else PROD_BASE_URL
        self.read_from_cache = read_from_cache
        self.write_to_cache = write_to_cache
        self.use_sandbox = use_sandbox
        # The production ssl certifcate seems to expired too
        self.verify_ssl = not use_sandbox
        # self.verify_ssl = False

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

    def _get(
        self,
        endpoint: str,
        params: Dict = None,
        headers: Dict = None
    ) -> Dict:
        """Get a resource from the API."""
        response = self.session.get(
            os.path.join(self.base_url, endpoint),
            params=params,
            verify=self.verify_ssl,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()

    def _post(
        self,
        endpoint: str,
        payload: Dict,
        headers: Dict = None
    ) -> Dict:
        """Post a resource to the API."""
        response = self.session.post(
            os.path.join(self.base_url, endpoint),
            json=payload,
            verify=self.verify_ssl,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()

    def _paginate(
        self,
        endpoint: str,
        payload: Dict,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """Helper method to handle pagination for list endpoints.

        Args:
            endpoint: The API endpoint to call
            payload: The base payload for the request
            page_size: Number of items per page
            max_pages: Maximum number of pages to fetch (None for all pages)
        Yields:
            Dict containing the results for each page
        """
        offset = 0
        page = 1
        total_count = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached maximum page limit of {max_pages}")
                break

            # Update payload with current pagination parameters
            current_payload = payload.copy()
            current_payload.update({
                "limit": page_size,
                "offset": offset
            })

            try:
                data = self._post(
                    endpoint=endpoint,
                    payload=current_payload,
                    headers={"Content-Type": "application/json"},
                )
                # Get total count on first page
                if total_count is None:
                    total_count = data.get("count", 0)
                    logger.info(f"Total items available: {total_count}")

                results = data.get("results", [])
                if not results:
                    logger.info("No more results available")
                    break

                logger.info(f"Fetched page {page} with {len(results)} items")
                yield data

                # Check if we've reached the end
                if offset + page_size >= total_count:
                    logger.info("Reached end of results")
                    break

                offset += page_size
                page += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch page {page}: {str(e)}")
                raise

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
        filter: Optional[Union[MainFilter]] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Dict:
        """Base method for searching entities.

        Args:
            endpoint: The API endpoint to call (e.g., 'companies', 'fundingRounds', 'investors')
            filter: Filter criteria for the entities
            sorting: Sorting criteria
            limit: Maximum number of results to return (None for all results)
            offset: Number of results to skip
            page_size: Number of items per page when using pagination
            max_pages: Maximum number of pages to fetch (None for all pages)

        Returns:
            Dict containing:
                - total_count: Total number of available results
                - count: Number of results in this response
                - results: List of matching entities
        """
        logger.info(f"Fetching {endpoint} with offset={offset}")

        # Handle different filter types
        if isinstance(filter, MainFilter):
            payload = filter.model_dump()
            if sorting:
                payload["sorting"] = sorting.model_dump()

        if limit is not None and limit < 100:
            # Single request with limit
            payload.update({
                "limit": limit,
                "offset": offset
            })
            try:
                data = self._post(
                    endpoint=endpoint,
                    payload=payload,
                    headers={"Content-Type": "application/json"},
                )
                logger.info(f"Successfully fetched {len(data.get('results', []))} `{endpoint}`")
                return {
                    "total_count": data.get("count", 0),
                    "count": len(data.get("results", [])),
                    "results": data.get("results", [])
                }
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch `{endpoint}`: {str(e)}")
                raise
        else:
            if limit is not None:
                max_pages = limit // page_size
            # Paginated requests
            paginated_results = self._paginate(
                endpoint=endpoint,
                payload=payload,
                page_size=page_size,
                max_pages=max_pages
            )

            count_in_result = 0
            results = []
            total_count = None

            for page in paginated_results:
                if total_count is None:
                    total_count = page.get("count", 0)
                page_results = page.get("results", [])
                results.extend(page_results)
                count_in_result += len(page_results)

            return {
                "total_count": total_count,
                "count": count_in_result,
                "results": results
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
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of startups matching the specified criteria."""
        return self._search_entities(
            endpoint="companies",
            filter=main_filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages
        )

    def search_deals(
        self,
        filter: Optional[MainFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of deals matching the specified criteria."""
        return self._search_entities(
            endpoint="fundingRounds",
            filter=filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages
        )

    def search_investors(
        self,
        filter: Optional[MainFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of investors matching the specified criteria."""
        return self._search_entities(
            endpoint="investors",
            filter=filter,
            sorting=sorting,
            limit=limit,
            offset=offset,
            page_size=page_size,
            max_pages=max_pages
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

        cookies = self.session.cookies.get_dict()
        if not cookies:
            logger.warning("No session cookies found — requests may fail auth")
        request_timeout = aiohttp.ClientTimeout(total=90)
        max_retries = 3
        max_concurrent = 10
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_entity(session: aiohttp.ClientSession, id: int) -> Dict:
            async with semaphore:
                for attempt in range(max_retries):
                    try:
                        async with session.get(
                            f"{self.base_url}/{endpoint.rstrip('/')}/{id}",
                            cookies=cookies,
                            headers={"Accept": "application/json, text/plain, */*"},
                            timeout=request_timeout,
                            ssl=self.verify_ssl,
                        ) as response:
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
                    except (TimeoutError, asyncio.TimeoutError):
                        if attempt < max_retries - 1:
                            logger.warning(f"Timeout {endpoint} {id} (attempt {attempt + 1}/{max_retries}), retrying...")
                            await asyncio.sleep(2 ** attempt)
                        else:
                            logger.error(f"Failed {endpoint} {id}: timeout after {max_retries} attempts")
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
                if not company_rounds:
                    import ipdb; ipdb.set_trace()
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
