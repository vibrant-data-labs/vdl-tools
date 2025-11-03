from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
)
from sqlalchemy_utils import generic_repr

from sqlalchemy.dialects.postgresql import JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class LinkedInBaseEmployee(BaseMixin):
    """Table to hold LinkedIn base employee data from Coresignal"""
    __tablename__ = 'linkedin_base_employee'

    id = Column(BigInteger, primary_key=True)
    parent_id = Column(BigInteger, nullable=False)
    is_parent = Column(Integer, nullable=True)
    full_name = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=True)
    headline = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    checked_at = Column(DateTime, nullable=False)
    public_profile_id = Column(String, nullable=True)
    profile_url = Column(String, nullable=False, index=True)
    location = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    summary = Column(String, nullable=True)
    services = Column(String, nullable=True)
    profile_photo_url = Column(String, nullable=True)
    deleted = Column(Integer, nullable=False, default=0)
    country = Column(String, nullable=True)
    country_iso_2 = Column(String, nullable=True)
    country_iso_3 = Column(String, nullable=True)
    recommendations_count = Column(Integer, nullable=True)
    connections_count = Column(Integer, nullable=True)
    follower_count = Column(Integer, nullable=True)
    experience_count = Column(Integer, nullable=True)
    shorthand_name = Column(String, nullable=False)
    canonical_shorthand_name = Column(String, nullable=False)
    received_at = Column(DateTime, nullable=False)
    is_latest = Column(Integer, nullable=False, default=1)
    
    # Array/nested fields stored as JSONB
    regions = Column(JSONB, nullable=True)
    shorthand_names = Column(JSONB, nullable=False)
    historical_ids = Column(JSONB, nullable=False)
    also_viewed = Column(JSONB, nullable=False)
    awards = Column(JSONB, nullable=False)
    certifications = Column(JSONB, nullable=False)
    courses = Column(JSONB, nullable=False)
    education = Column(JSONB, nullable=False)
    experience = Column(JSONB, nullable=False)
    groups = Column(JSONB, nullable=False)
    interests = Column(JSONB, nullable=False)
    languages = Column(JSONB, nullable=False)
    organizations = Column(JSONB, nullable=False)
    patents = Column(JSONB, nullable=False)
    posts_see_more_urls = Column(JSONB, nullable=False)
    projects = Column(JSONB, nullable=False)
    publications = Column(JSONB, nullable=False)
    recommendations = Column(JSONB, nullable=False)
    similar_profiles = Column(JSONB, nullable=False)
    others_named = Column(JSONB, nullable=False)
    skills = Column(JSONB, nullable=False)
    test_scores = Column(JSONB, nullable=False)
    volunteering_cares = Column(JSONB, nullable=False)
    volunteering_opportunities = Column(JSONB, nullable=False)
    volunteering_positions = Column(JSONB, nullable=False)
    volunteering_supports = Column(JSONB, nullable=False)
    websites = Column(JSONB, nullable=False)
    course_suggestions = Column(JSONB, nullable=False)
    activity = Column(JSONB, nullable=False)
    hidden_details = Column(JSONB, nullable=False)

