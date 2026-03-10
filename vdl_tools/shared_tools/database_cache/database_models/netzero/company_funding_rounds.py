from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class CompanyFundingRounds(BaseMixin):
    __tablename__ = 'company_funding_rounds_nzi'

    # Primary and unique identifiers
    clientID = Column(Integer, primary_key=True)
    fullData = Column(JSONB)
