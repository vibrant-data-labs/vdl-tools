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

    member_id = Column(BigInteger, primary_key=True)
    member_full_name = Column(String, nullable=True)
    member_name_first = Column(String, nullable=True)
    member_name_middle = Column(String, nullable=True)
    member_name_last = Column(String, nullable=True)
    member_websites_linkedin = Column(String, nullable=True, index=True)
    member_picture_url = Column(String, nullable=True)
    member_public_profile_id = Column(String, nullable=True)
    member_description = Column(String, nullable=True)
    member_job_title = Column(String, nullable=True)
    is_decision_maker = Column(Integer, nullable=True)
    member_job_description = Column(String, nullable=True)
    company_id = Column(BigInteger, nullable=True, index=True)
    member_recommendations_count = Column(Integer, nullable=True)
    member_connections_count = Column(Integer, nullable=True)
    member_location_raw_address = Column(String, nullable=True)
    member_location_country = Column(String, nullable=True)
    member_location_regions = Column(String, nullable=True)
    member_last_updated = Column(Date, nullable=True)
    member_is_deleted = Column(Integer, nullable=True)
    member_department = Column(String, nullable=True)
    member_management_level = Column(String, nullable=True)
    is_working = Column(Integer, nullable=True)
    member_sort_score = Column(Integer, nullable=True)
    member_quality = Column(Integer, nullable=True)
    is_hidden = Column(Integer, nullable=True)
    member_generated_headline = Column(String, nullable=True)
    member_headline = Column(String, nullable=True)
    member_follower_count = Column(Integer, nullable=True)
    total_experience_duration = Column(String, nullable=True)
    total_experience_duration_months = Column(BigInteger, nullable=True)
    op_created_at = Column(DateTime, nullable=True)
    op_updated_at = Column(DateTime, nullable=True)
    
    # Array/nested fields stored as JSONB
    member_experience = Column(JSONB, nullable=True)
    member_education = Column(JSONB, nullable=True)
    member_languages = Column(JSONB, nullable=True)
    member_certifications = Column(JSONB, nullable=True)
    member_courses = Column(JSONB, nullable=True)
    member_awards = Column(JSONB, nullable=True)
    member_activity = Column(JSONB, nullable=True)
    member_organizations = Column(JSONB, nullable=True)
    member_patents = Column(JSONB, nullable=True)
    member_publications = Column(JSONB, nullable=True)
    member_recommendations = Column(JSONB, nullable=True)
    member_skills = Column(JSONB, nullable=True)  # Array of strings
    member_shorthand_names = Column(JSONB, nullable=True)  # Array of strings
