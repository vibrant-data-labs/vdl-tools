from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy_utils import generic_repr
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5


@generic_repr
class Embedding(BaseMixin):
    """Table to hold the embeddings of input texts"""
    __tablename__ = 'embedding'

    model_name = Column(String, primary_key=True)
    text_id = Column(String, primary_key=True)
    given_id = Column(String)
    input_text = Column(String, nullable=False)
    response_full = Column(JSONB, nullable=False)
    embedding = Column(ARRAY(Float), nullable=True)
    num_errors = Column(Integer, nullable=True)

    def __init__(self, **kwargs):
        if 'text_id' not in kwargs:
            text_id = self.create_text_id(
                text=kwargs["input_text"],
            )
            kwargs['text_id'] = text_id
        else:
            assert kwargs['text_id'] == self.create_text_id(
                text=kwargs["input_text"],
            )

        if 'given_id' not in kwargs:
            given_id = self.create_text_id(
                text=kwargs["input_text"],
            )
            kwargs['given_id'] = given_id

        super().__init__(**kwargs)

    @classmethod
    def create_text_id(cls, text):
        return create_deterministic_md5(text)