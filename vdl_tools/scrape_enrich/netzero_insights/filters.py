from pydantic import BaseModel
from typing import Dict, List, Optional, Union
from datetime import datetime



class Sorting(BaseModel):
    """Sorting criteria for API requests."""
    field: str
    order: str = "asc"  # "asc" or "desc"



class StartupFilter(BaseModel):
    """Filter criteria for startups."""
    searchableLocations: Optional[List[int]] = None
    stages: Optional[List[int]] = None
    fundings: Optional[List[int]] = None
    employeesFrom: Optional[int] = None
    employeesTo: Optional[int] = None
    fundingsFrom: Optional[int] = None
    fundingsTo: Optional[int] = None
    tags: Optional[List[int]] = None
    tagsMode: Optional[str] = None  # "AND" or "OR"
    taxonomyItems: Optional[List[int]] = None
    taxonomyItemsMode: Optional[str] = None  # "AND" or "OR"
    trls: Optional[List[int]] = None
    financialStageIDs: Optional[List[int]] = None
    sustainabilities: Optional[List[int]] = None
    foundedDates: Optional[List[Dict[str, str]]] = None
    acquisitionDateFrom: Optional[str] = None
    acquisitionDateTo: Optional[str] = None
    foundedDatesFrom: Optional[str] = None
    foundedDatesTo: Optional[str] = None
    raisedDateFrom: Optional[str] = None
    raisedDateTo: Optional[str] = None
    lastRoundDates: Optional[List[Dict[str, str]]] = None
    lastRoundDatesFrom: Optional[str] = None
    lastRoundDatesTo: Optional[str] = None
    numberOfRoundFrom: Optional[int] = None
    numberOfRoundTo: Optional[int] = None
    fundingTypes: Optional[List[Dict[str, str]]] = None
    sdgs: Optional[List[int]] = None
    wildcards: Optional[List[str]] = None
    wildcardsFields: Optional[List[str]] = None
    investors: Optional[List[int]] = None
    lastFundingTypes: Optional[List[Dict[str, str]]] = None
    lastFundingsFrom: Optional[List[int]] = None
    lastFundingsTo: Optional[List[int]] = None
    patentSearch: Optional[List[str]] = None
    patentsStatus: Optional[List[Dict[str, str]]] = None
    applicationDateFrom: Optional[str] = None
    applicationDateTo: Optional[str] = None
    grantedDateFrom: Optional[str] = None
    grantedDateTo: Optional[str] = None


class DealFilter(BaseModel):
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


class InvestorFilter(BaseModel):
    """Filter criteria for investors."""
    investorTypeIDs: Optional[List[int]] = None
    includeOtherInvestorTypes: Optional[bool] = None
    investorDealsFrom: Optional[int] = None
    investorDealsTo: Optional[int] = None
    investorSearchableLocations: Optional[List[int]] = None
    investorRegions: Optional[List[int]] = None
    coInvestors: Optional[List[int]] = None
    investments: Optional[List[int]] = None
    investorIDs: Optional[List[int]] = None
    investorFoundedDatesFrom: Optional[str] = None
    investorFoundedDatesTo: Optional[str] = None


class ContactFilter(BaseModel):
    """Filter criteria for contacts."""
    client_id: int
    decision_maker: Optional[bool] = None
    role_id: Optional[int] = None


class InvestorContactFilter(BaseModel):
    """Filter criteria for investor contacts."""
    investor_id: int
    decision_maker: Optional[bool] = None
    role_id: Optional[int] = None


class TagFilter(BaseModel):
    """Filter criteria for tags."""
    name: Optional[str] = None
    type: Optional[List[str]] = None  # ["Technology", "Market", etc.]


class MainFilter(BaseModel):
    """Main filter structure for API requests."""
    limit: Optional[int] = None
    offset: int = 0
    include: Optional[StartupFilter | dict] = {}
    exclude: Optional[StartupFilter | dict] = {}
    investorInclude: Optional[InvestorFilter | dict] = {}
    investorExclude: Optional[InvestorFilter | dict] = {}
    fundingRoundInclude: Optional[DealFilter | dict] = {}
    fundingRoundExclude: Optional[DealFilter | dict] = {}
    sorting: Optional[Sorting] = None


def create_filter_dict(filter_obj: MainFilter) -> Dict:
    """Recursively convert a filter object to a dictionary for API requests removing None values"""
    filter_dict = filter_obj.model_dump()
    filter_dict = filter_none_values(filter_dict)
    return filter_dict


def filter_none_values(filter_dict: Dict, new_dict: Dict = {}) -> Dict:
    new_dict = new_dict or {}
    """Remove None values from a nested dictionary"""
    for key, value in filter_dict.items():
        if isinstance(value, dict):
            filter_none_values(value, new_dict)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    filter_none_values(item, new_dict)
        else:
            new_dict[key] = value
    return new_dict
