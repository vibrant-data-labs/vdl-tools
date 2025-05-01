from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
from datetime import datetime


@dataclass
class Sorting:
    """Sorting criteria for API requests."""
    field: str
    order: str = "asc"  # "asc" or "desc"


@dataclass
class MainFilter:
    """Main filter structure for API requests."""
    limit: Optional[int] = None
    offset: int = 0
    include: Dict = field(default_factory=dict)
    exclude: Dict = field(default_factory=dict)
    sorting: Optional[Sorting] = None


@dataclass
class StartupFilter:
    """Filter criteria for startups."""
    searchable_locations: Optional[List[int]] = None
    stages: Optional[List[int]] = None
    fundings: Optional[List[int]] = None
    employees_from: Optional[int] = None
    employees_to: Optional[int] = None
    fundings_from: Optional[int] = None
    fundings_to: Optional[int] = None
    tags: Optional[List[int]] = None
    tags_mode: Optional[str] = None  # "AND" or "OR"
    trls: Optional[List[int]] = None
    financial_stage_ids: Optional[List[int]] = None
    sustainabilities: Optional[List[int]] = None
    founded_dates: Optional[List[Dict[str, str]]] = None
    acquisition_date_from: Optional[str] = None
    acquisition_date_to: Optional[str] = None
    founded_dates_from: Optional[str] = None
    founded_dates_to: Optional[str] = None
    raised_date_from: Optional[str] = None
    raised_date_to: Optional[str] = None
    last_round_dates: Optional[List[Dict[str, str]]] = None
    last_round_dates_from: Optional[str] = None
    last_round_dates_to: Optional[str] = None
    number_of_round_from: Optional[int] = None
    number_of_round_to: Optional[int] = None
    funding_types: Optional[List[Dict[str, str]]] = None
    sdgs: Optional[List[int]] = None
    wildcards: Optional[List[str]] = None
    wildcardsFields: Optional[List[Dict[str, str]]] = None
    investors: Optional[List[int]] = None
    last_funding_types: Optional[List[Dict[str, str]]] = None
    last_fundings_from: Optional[List[int]] = None
    last_fundings_to: Optional[List[int]] = None
    patent_search: Optional[List[str]] = None
    patents_status: Optional[List[Dict[str, str]]] = None
    application_date_from: Optional[str] = None
    application_date_to: Optional[str] = None
    granted_date_from: Optional[str] = None
    granted_date_to: Optional[str] = None


@dataclass
class DealFilter:
    """Filter criteria for deals."""
    acquisition_date_from: Optional[str] = None
    acquisition_date_to: Optional[str] = None
    dates_from: Optional[str] = None
    dates_to: Optional[str] = None
    last_round_days: Optional[List[int]] = None
    amount_from: Optional[int] = None
    amount_to: Optional[int] = None
    types: Optional[List[int]] = None
    allow_null_amounts: Optional[bool] = None
    number_from: Optional[int] = None
    number_to: Optional[int] = None
    investors: Optional[List[int]] = None
    total_funding_from: Optional[int] = None
    total_funding_to: Optional[int] = None
    financing_instruments: Optional[List[str]] = None
    equity_stages: Optional[List[int]] = None
    exit_stages: Optional[List[int]] = None


@dataclass
class InvestorFilter:
    """Filter criteria for investors."""
    investor_type_ids: Optional[List[int]] = None
    include_other_investor_types: Optional[bool] = None
    investor_deals_from: Optional[int] = None
    investor_deals_to: Optional[int] = None
    investor_searchable_locations: Optional[List[int]] = None
    investor_regions: Optional[List[int]] = None
    co_investors: Optional[List[int]] = None
    investments: Optional[List[int]] = None
    investor_ids: Optional[List[int]] = None
    investor_founded_dates_from: Optional[str] = None
    investor_founded_dates_to: Optional[str] = None


@dataclass
class ContactFilter:
    """Filter criteria for contacts."""
    client_id: int
    decision_maker: Optional[bool] = None
    role_id: Optional[int] = None


@dataclass
class InvestorContactFilter:
    """Filter criteria for investor contacts."""
    investor_id: int
    decision_maker: Optional[bool] = None
    role_id: Optional[int] = None


@dataclass
class TagFilter:
    """Filter criteria for tags."""
    name: Optional[str] = None
    type: Optional[List[str]] = None  # ["Technology", "Market", etc.]


def create_filter_dict(filter_obj: Union[StartupFilter, DealFilter, InvestorFilter, ContactFilter, InvestorContactFilter, TagFilter]) -> Dict:
    """Convert a filter object to a dictionary for API requests."""
    filter_dict = {}
    for field, value in filter_obj.__dict__.items():
        if value is not None:
            filter_dict[field] = value
    return filter_dict 