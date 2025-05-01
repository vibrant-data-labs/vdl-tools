import requests
from typing import Dict, List, Optional, Union, Generator
from dataclasses import dataclass
from datetime import datetime
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.netzero_insights.filters import (
    Sorting, MainFilter, StartupFilter, DealFilter, InvestorFilter,
    ContactFilter, InvestorContactFilter, TagFilter, create_filter_dict
)

from vdl_tools.shared_tools.database_cache.database_models import Startup
from vdl_tools.shared_tools.database_cache.database_utils import get_session



PROD_BASE_URL = "https://api.netzeroinsights.com"
SANDBOX_BASE_URL = "https://20.108.20.67"


class NetZeroAPI:
    """Client for interacting with the NetZero Insights API."""

    def __init__(
        self,
        username: str,
        password: str,
        use_sandbox: bool = False
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
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the API and store the session cookie."""
        logger.info("Authenticating with NetZero API")
        try:
            import ipdb; ipdb.set_trace()
            response = self.session.post(
                f"{self.base_url}/security/formLogin",
                data={
                    "username": self.username,
                    "password": self.password
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                verify=True
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
            response = self.session.get(f"{self.base_url}/security/logout")
            response.raise_for_status()
            logger.info("Successfully logged out from NetZero API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to logout from NetZero API: {str(e)}")
            raise

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
                response = self.session.post(
                    f"{self.base_url}/{endpoint}",
                    json=current_payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()
                
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

    def get_startups(
        self,
        filter: Optional[StartupFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of startups matching the specified criteria.
        
        Args:
            filter: Filter criteria for startups
            sorting: Sorting criteria
            limit: Maximum number of results to return (None for all results)
            offset: Number of results to skip
            page_size: Number of items per page when using pagination
            max_pages: Maximum number of pages to fetch (None for all pages)
            
        Returns:
            If limit is specified: Dict containing count and list of matching startups
            If limit is None: Generator yielding Dicts containing results for each page
        """
        logger.info(f"Fetching startups with offset={offset}")
        payload = {
            "include": create_filter_dict(filter) if filter else {},
            "exclude": {},
            "sorting": sorting.__dict__ if sorting else None
        }

        if limit is not None:
            # Single request with limit
            payload.update({
                "limit": limit,
                "offset": offset
            })
            try:
                response = self.session.post(
                    f"{self.base_url}/companies",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()
                logger.info(f"Successfully fetched {len(data.get('results', []))} startups")
                return data
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch startups: {str(e)}")
                raise
        else:
            # Paginated requests
            return self._paginate(
                endpoint="companies",
                payload=payload,
                page_size=page_size,
                max_pages=max_pages
            )

    def get_deals(
        self,
        filter: Optional[DealFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of deals matching the specified criteria.
        
        Args:
            filter: Filter criteria for deals
            sorting: Sorting criteria
            limit: Maximum number of results to return (None for all results)
            offset: Number of results to skip
            page_size: Number of items per page when using pagination
            max_pages: Maximum number of pages to fetch (None for all pages)
            
        Returns:
            If limit is specified: Dict containing count and list of matching deals
            If limit is None: Generator yielding Dicts containing results for each page
        """
        logger.info(f"Fetching deals with offset={offset}")
        payload = {
            "include": create_filter_dict(filter) if filter else {},
            "exclude": {},
            "sorting": sorting.__dict__ if sorting else None
        }

        if limit is not None:
            # Single request with limit
            payload.update({
                "limit": limit,
                "offset": offset
            })
            try:
                response = self.session.post(
                    f"{self.base_url}/fundingRounds",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()
                logger.info(f"Successfully fetched {len(data.get('results', []))} deals")
                return data
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch deals: {str(e)}")
                raise
        else:
            # Paginated requests
            return self._paginate(
                endpoint="fundingRounds",
                payload=payload,
                page_size=page_size,
                max_pages=max_pages
            )

    def get_startup_detail(self, startup_id: int) -> Dict:
        """Get detailed information about a specific startup."""
        logger.info(f"Fetching details for startup with ID {startup_id}")
        with get_session() as session:
            logger.info(f"Checking database for startup with ID {startup_id}")
            startup = session.query(Startup).filter(Startup.clientID == startup_id).first()
            if startup:
                logger.info(f"Found startup in database")
                return startup.to_dict()

        try:
            response = self.session.get(f"{self.base_url}/getStartup/{startup_id}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched details for startup {startup_id}")
            startup = Startup(**data)
            session.add(startup)
            session.commit()
            return startup.to_dict()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch startup details for ID {startup_id}: {str(e)}")
            raise

    def get_deal_detail(self, deal_id: int) -> Dict:
        """Get detailed information about a specific deal."""
        logger.info(f"Fetching details for deal with ID {deal_id}")
        try:
            response = self.session.get(f"{self.base_url}/fundingRounds/{deal_id}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched details for deal {deal_id}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch deal details for ID {deal_id}: {str(e)}")
            raise

    def get_investors(
        self,
        filter: Optional[InvestorFilter] = None,
        sorting: Optional[Sorting] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> Union[Dict, Generator[Dict, None, None]]:
        """Get a list of investors matching the specified criteria.
        
        Args:
            filter: Filter criteria for investors
            sorting: Sorting criteria
            limit: Maximum number of results to return (None for all results)
            offset: Number of results to skip
            page_size: Number of items per page when using pagination
            max_pages: Maximum number of pages to fetch (None for all pages)
            
        Returns:
            If limit is specified: Dict containing count and list of matching investors
            If limit is None: Generator yielding Dicts containing results for each page
        """
        logger.info(f"Fetching investors with offset={offset}")
        payload = {
            "include": create_filter_dict(filter) if filter else {},
            "exclude": {},
            "sorting": sorting.__dict__ if sorting else None
        }
        
        if limit is not None:
            # Single request with limit
            payload.update({
                "limit": limit,
                "offset": offset
            })
            try:
                response = self.session.post(
                    f"{self.base_url}/investors",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()
                logger.info(f"Successfully fetched {len(data.get('results', []))} investors")
                return data
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch investors: {str(e)}")
                raise
        else:
            # Paginated requests
            return self._paginate(
                endpoint="investors",
                payload=payload,
                page_size=page_size,
                max_pages=max_pages
            )

    def get_investor_detail(self, investor_id: int) -> Dict:
        """Get detailed information about a specific investor."""
        logger.info(f"Fetching details for investor with ID {investor_id}")
        try:
            response = self.session.get(f"{self.base_url}/investors/{investor_id}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched details for investor {investor_id}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch investor details for ID {investor_id}: {str(e)}")
            raise 