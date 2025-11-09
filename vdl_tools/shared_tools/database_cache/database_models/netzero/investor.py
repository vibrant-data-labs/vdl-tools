from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, BigInteger
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class Investor(BaseMixin):
    __tablename__ = 'investors'

    # Primary and unique identifiers
    id = Column(Integer, primary_key=True)
    investorID = Column(Integer, unique=True)
    name = Column(String)

    # Dates
    firstRoundDate = Column(String) # "Jul 2022",

    # Funding details
    roundTypes = Column(String) # "Late VC"
    primaryType = Column(String) # 'Venture Capital'
    funds = Column(String)

    