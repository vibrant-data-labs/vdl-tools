from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from sqlalchemy_utils import generic_repr


@generic_repr
class CommercialDeal(BaseMixin):
    __tablename__ = 'commercial_deals_nzi'

    # Primary and unique identifiers
    id = Column(Integer, primary_key=True)
    clientID = Column(Integer, unique=True)
    fullData = Column(JSONB)
