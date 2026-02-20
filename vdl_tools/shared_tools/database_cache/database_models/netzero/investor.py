from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class Investor(BaseMixin):
    __tablename__ = 'investors_nzi'

    # Primary and unique identifiers
    investorID = Column(Integer, primary_key=True)
    name = Column(String)
    fullData = Column(JSONB)
