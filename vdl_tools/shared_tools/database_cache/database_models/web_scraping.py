from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
)
from sqlalchemy_utils import generic_repr

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin

@generic_repr
class WebPagesParsed(BaseMixin):
    """Table to hold scraped webpages"""
    __tablename__ = 'web_pages_parsed'

    cleaned_home_key = Column(String, nullable=False, primary_key=True, index=True)
    home_url = Column(String, nullable=False, index=True)
    num_errors = Column(Integer, nullable=True)
    combined_text = Column(String, nullable=True)

@generic_repr
class WebPagesScraped(BaseMixin):
    """Table to hold scraped webpages"""
    __tablename__ = 'web_pages_scraped'

    cleaned_key = Column(String, nullable=False, primary_key=True, index=True)
    # parent_key = Column(String, ForeignKey('web_pages_parsed.cleaned_home_key'), nullable=False)
    full_path = Column(String, index=True)
    home_url = Column(String, nullable=False, index=True)
    subpath = Column(String, nullable=False)
    raw_html = Column(String, nullable=True)
    parsed_html = Column(String, nullable=True)
    page_type = Column(String, nullable=False, index=True)
    response_status_code = Column(Integer, nullable=True)
    num_errors = Column(Integer, nullable=True)


