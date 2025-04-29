from sqlalchemy import (
    Column,
    Integer,
    String,
)
from sqlalchemy_utils import generic_repr

from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class LinkedInPerson(BaseMixin):
    """Table to hold scraped LinkedIn profiles"""
    __tablename__ = 'linkedin_person'

    linkedin_id = Column(String, primary_key=True)
    original_id = Column(String)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    title = Column(String)
    url = Column(String, index=True)
    location = Column(String)
    industry = Column(String)
    summary = Column(String)
    connections = Column(String)
    logo_url = Column(String)
    country = Column(String)
    connections_count = Column(Integer)
    experience_count = Column(Integer)
    member_shorthand_name = Column(String)
    member_shorthand_name_hash = Column(String)
    canonical_url = Column(String)
    canonical_hash = Column(String)
    canonical_shorthand_name = Column(String)
    canonical_shorthand_name_hash = Column(String)
    member_also_viewed_collection = Column(JSONB)
    member_awards_collection = Column(JSONB)
    member_certifications_collection = Column(JSONB)
    member_education_collection = Column(JSONB)
    member_experience_collection = Column(JSONB)
    member_languages_collection = Column(JSONB)
    member_organizations_collection = Column(JSONB)
    member_patent_status_list = Column(JSONB)
    member_groups_collection = Column(JSONB)
    member_projects_collection = Column(JSONB)
    member_publications_collection = Column(JSONB)
    member_skills_collection = Column(JSONB)
    member_volunteering_positions_collection = Column(JSONB)
    member_websites_collection = Column(JSONB)
    datasource = Column(String, index=True)
    raw_json = Column(JSONB, nullable=True)
    num_errors = Column(Integer, nullable=True)
