import json
from uuid import uuid5, NAMESPACE_URL

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
)
from sqlalchemy_utils import generic_repr
from sqlalchemy.dialects.postgresql import JSONB

from vdl_tools.shared_tools.database_cache.database_models.base import BaseMixin


PROMPT_RESPONSE_NAMESPACE_TEXT = "PROMPT_RESPONSE_NAMESPACE_TEXT"


def make_uuid(name, namespace_text=PROMPT_RESPONSE_NAMESPACE_TEXT):
    namespace = uuid5(NAMESPACE_URL, namespace_text)
    return str(uuid5(namespace, name=name))


@generic_repr
class Prompt(BaseMixin):
    """Table to hold scraped prompts"""
    __tablename__ = 'prompt'

    id = Column(String, primary_key=True, index=True)
    name = Column(String, primary_key=False, nullable=True)
    description = Column(String, nullable=True)
    prompt_str = Column(String, nullable=False)

    def __init__(self, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = self.create_text_id(
                kwargs["prompt_str"]
            )
        else:
            assert kwargs['id'] == self.create_text_id(
                kwargs["prompt_str"],
            )
        super().__init__(**kwargs)

    @classmethod
    def create_text_id(cls, text):
        return make_uuid(
            text,
            namespace_text=PROMPT_RESPONSE_NAMESPACE_TEXT
        )

class PromptResponse(BaseMixin):
    """Table to hold the responses for prompts"""
    __tablename__ = 'prompt_response'

    prompt_id = Column(String, ForeignKey('prompt.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True, index=True)
    given_id = Column(String, primary_key=True, index=True)
    text_id = Column(String, index=True)
    input_text = Column(String, nullable=False)
    response_full = Column(JSONB, nullable=False)
    response_text = Column(String, nullable=True)
    num_errors = Column(Integer, nullable=True)

    def __init__(self, **kwargs):
        if 'text_id' not in kwargs:
            text_id = self.create_text_id(
                kwargs["input_text"],
            )
            kwargs['text_id'] = text_id
        else:
            assert kwargs['text_id'] == self.create_text_id(
                kwargs["input_text"]
            )
        super().__init__(**kwargs)

    @classmethod
    def create_text_id(cls, text):
        if not isinstance(text, (str, int)):
            text = json.dumps(text)
        return make_uuid(
            text,
            namespace_text=PROMPT_RESPONSE_NAMESPACE_TEXT
        )
