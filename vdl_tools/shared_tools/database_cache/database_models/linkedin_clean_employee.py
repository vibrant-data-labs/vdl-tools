from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Date,
    DateTime,
)
from sqlalchemy_utils import generic_repr

from sqlalchemy.dialects.postgresql import JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class LinkedInCleanEmployee(BaseMixin):
    """Table to hold cleaned LinkedIn employee data from Coresignal"""
    __tablename__ = 'linkedin_clean_employee'

    id = Column(BigInteger, primary_key=True)
    public_profile_id = Column(BigInteger, nullable=True)
    full_name = Column(String, nullable=True)
    name_first = Column(String, nullable=True)
    name_middle = Column(String, nullable=True)
    name_last = Column(String, nullable=True)
    headline = Column(String, nullable=True)
    generated_headline = Column(String, nullable=True)
    websites_linkedin = Column(String, nullable=True, index=True)
    shorthand_names = Column(JSONB, nullable=True)  # Array of strings
    last_updated = Column(DateTime, nullable=True)
    is_deleted = Column(Integer, nullable=True, default=0)
    is_hidden = Column(Integer, nullable=True, default=0)
    picture_url = Column(String, nullable=True)
    description = Column(String, nullable=True)
    location_raw_address = Column(String, nullable=True)
    location_country = Column(String, nullable=True)
    location_regions = Column(JSONB, nullable=True)  # Array of strings
    connections_count = Column(Integer, nullable=True)
    follower_count = Column(Integer, nullable=True)
    is_working = Column(Integer, nullable=True)
    company_id = Column(BigInteger, nullable=True, index=True)
    job_title = Column(String, nullable=True)
    management_level = Column(String, nullable=True)
    is_decision_maker = Column(Integer, nullable=True)
    department = Column(String, nullable=True)
    job_description = Column(String, nullable=True)
    total_experience_duration = Column(String, nullable=True)
    total_experience_duration_months = Column(Integer, nullable=True)
    
    # Array/nested fields stored as JSONB
    experience = Column(JSONB, nullable=True)
    education = Column(JSONB, nullable=True)
    skills = Column(JSONB, nullable=True)  # Array of strings
    recommendations_count = Column(Integer, nullable=True)
    recommendations = Column(JSONB, nullable=True)
    languages = Column(JSONB, nullable=True)
    courses = Column(JSONB, nullable=True)
    certifications = Column(JSONB, nullable=True)
    organizations = Column(JSONB, nullable=True)
    patents = Column(JSONB, nullable=True)
    publications = Column(JSONB, nullable=True)
    awards = Column(JSONB, nullable=True)
    activity = Column(JSONB, nullable=True)
    original_url = Column(String, nullable=True, index=True)  # URL used to query this profile
    full_result = Column(JSONB, nullable=False)
