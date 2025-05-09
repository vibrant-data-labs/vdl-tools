
import os
import requests
import aiohttp
import asyncio
from typing import Dict, List, Optional, Union, Generator, Type
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.netzero_insights.filters import (
    Sorting, MainFilter,
)

from vdl_tools.shared_tools.database_cache.database_models import Startup
from vdl_tools.shared_tools.database_cache.database_utils import get_session


class Deal:
    pass

class Investor:
    pass

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
        self.verify = not use_sandbox

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
                verify=self.verify,
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
            response = self.session.get(f"{self.base_url}/security/logout", verify=self.verify)
            response.raise_for_status()
            logger.info("Successfully logged out from NetZero API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to logout from NetZero API: {str(e)}")
            raise

    def _get(
        self,
        endpoint: str,
        params: Dict,
        headers: Dict = None
    ) -> Dict:
        """Get a resource from the API."""
        response = self.session.get(
            os.path.join(self.base_url, endpoint),
            params=params,
            verify=self.verify,
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
            verify=self.verify,
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
                logger.info(f"Successfully fetched {len(data.get('results', []))} {endpoint}")
                return {
                    "total_count": data.get("count", 0),
                    "count": len(data.get("results", [])),
                    "results": data.get("results", [])
                }
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch {endpoint}: {str(e)}")
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
        return self._post(
            endpoint="getStartupCount",
            payload=main_filter.model_dump() if main_filter else {},
        )["count"]

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

    def _get_detail(
        self,
        id: int,
        endpoint: str,
        model_class: type,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> Dict:
        """Base method for getting detailed information about an entity.

        Args:
            id: The ID of the entity to fetch
            endpoint: The API endpoint to call
            model_class: The class to instantiate with the response data
            read_from_cache: Whether to read from cache
            write_to_cache: Whether to write to cache

        Returns:
            Dict containing the entity details
        """
        logger.info(f"Fetching details for {endpoint} with ID {id}")

        read_from_cache, write_to_cache = self._resolve_cache_params(read_from_cache, write_to_cache)

        if read_from_cache:
            with get_session() as session:
                logger.info(f"Checking database for {endpoint} with ID {id}")
                entity = session.query(model_class).filter(model_class.clientID == id).first()
                if entity:
                    logger.info(f"Found {endpoint} in database")
                    return entity.to_dict()

        try:
            data = self._get(
                endpoint=f"{endpoint}/{id}",
            )
            logger.info(f"Successfully fetched details for {endpoint} {id}")
            entity = model_class(**data)
            if write_to_cache:
                with get_session() as session:
                    session.add(entity)
                    session.commit()
            return entity.to_dict()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {endpoint} details for ID {id}: {str(e)}")
            raise
    
    def get_startup_detail(
        self,
        startup_id: int,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> Dict:
        """Get detailed information about a specific startup."""
        return self._get_detail(
            id=startup_id,
            endpoint="getStartup",
            model_class=Startup,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )

    def get_investor_detail(
        self,
        investor_id: int,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> Dict:
        """Get detailed information about a specific investor."""
        return self._get_detail(
            id=investor_id,
            endpoint="investors",
            model_class=Investor,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )

    def get_deal_detail(
        self,
        deal_id: int,
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> Dict:
        """Get detailed information about a specific deal."""
        return self._get_detail(
            id=deal_id,
            endpoint="fundingRounds",
            model_class=Deal,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )

    async def _get_details_batch(
        self,
        ids: List[int],
        endpoint: str,
        model_class: Type,
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
        logger.info(f"Fetching details for {len(ids)} {endpoint}s")
        results = {}
        missing_ids = set(ids)

        read_from_cache, write_to_cache = self._resolve_cache_params(read_from_cache, write_to_cache)

        if read_from_cache:
            with get_session() as session:
                logger.info(f"Checking database for {len(ids)} {endpoint}s")
                entities = session.query(model_class).filter(model_class.clientID.in_(ids)).all()
                for entity in entities:
                    results[entity.clientID] = entity.to_dict()
                    missing_ids.remove(entity.clientID)
                logger.info(f"Found {len(results)} {endpoint}s in database")

        if not missing_ids:
            return [results[id] for id in ids]

        # Get cookies from the authenticated session
        cookies = self.session.cookies.get_dict()

        # Fetch remaining entities from API
        async def fetch_entity(session: aiohttp.ClientSession, id: int) -> Dict:
            try:
                async with session.get(
                    f"{self.base_url}/{endpoint}/{id}",
                    cookies=cookies,
                    ssl=self.verify,
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return id, data
            except Exception as e:
                logger.error(f"Failed to fetch {endpoint} {id}: {str(e)}")
                return id, None

        async def process_batch(batch: List[Dict]):
            if not write_to_cache:
                return
            with get_session() as session:
                for id, data in batch:
                    if data is None:
                        continue
                    entity = model_class(**data)
                    session.add(entity)
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

        logger.info(f"Successfully fetched {len(results)} {endpoint}s")
        return [results.get(id) for id in ids]

    async def get_startup_details(
        self,
        startup_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> List[Dict]:
        """Get detailed information about multiple startups."""
        return await self._get_details_batch(
            ids=startup_ids,
            endpoint="getStartup",
            model_class=Startup,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )

    async def get_deal_details(
        self,
        deal_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> List[Dict]:
        """Get detailed information about multiple deals."""
        return await self._get_details_batch(
            ids=deal_ids,
            endpoint="fundingRounds",
            model_class=Deal,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )

    async def get_investor_details(
        self,
        investor_ids: List[int],
        read_from_cache: bool = None,
        write_to_cache: bool = None,
    ) -> List[Dict]:
        """Get detailed information about multiple investors."""
        return await self._get_details_batch(
            ids=investor_ids,
            endpoint="investors",
            model_class=Investor,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache
        )


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
