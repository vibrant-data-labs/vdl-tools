from sqlalchemy import (
    Column,
    Integer,
    String,
)
from sqlalchemy_utils import generic_repr

from sqlalchemy.dialects.postgresql import ARRAY

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class LinkedInOrganization(BaseMixin):
    """Table to hold scraped webpages"""
    __tablename__ = 'linkedin_organization'

    linkedin_id = Column(String, primary_key=True)
    original_id = Column(String)
    coresignal_id = Column(Integer)
    name = Column(String)
    image = Column(String)
    url = Column(String, index=True)
    summary = Column(String)
    about = Column(String)
    website = Column(String)
    industry = Column(String)
    specialties = Column(ARRAY(String))
    company_size = Column(String)
    hq_location = Column(String)
    company_type = Column(String)
    founded = Column(String)
    locations = Column(ARRAY(String))
    datasource = Column(String, index=True)
    raw_html = Column(String, nullable=True)
    num_errors = Column(Integer, nullable=True)
