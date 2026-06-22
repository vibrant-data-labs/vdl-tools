from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
)
from sqlalchemy_utils import generic_repr
from sqlalchemy.dialects.postgresql import JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5


@generic_repr
class Geocode(BaseMixin):
    """Table to hold geocoded address results.

    Keyed by (provider, address_id) where address_id is a deterministic md5 of the
    *normalized* address string (parallels Embedding's (model_name, text_id)).
    Failed lookups are stored as rows with num_errors set.
    """
    __tablename__ = 'geocode'

    provider = Column(String, primary_key=True)
    address_id = Column(String, primary_key=True)
    address = Column(String, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    response_full = Column(JSONB, nullable=True)
    num_errors = Column(Integer, nullable=True)

    def __init__(self, **kwargs):
        if 'address_id' not in kwargs:
            kwargs['address_id'] = self.create_address_id(kwargs["address"])
        else:
            assert kwargs['address_id'] == self.create_address_id(kwargs["address"])
        super().__init__(**kwargs)

    @classmethod
    def create_address_id(cls, address):
        return create_deterministic_md5(address)
