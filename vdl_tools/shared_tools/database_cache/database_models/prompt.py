import json
from uuid import uuid5, NAMESPACE_URL

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    PrimaryKeyConstraint,
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

    __table_args__ = (
        PrimaryKeyConstraint('prompt_id', 'given_id', 'model_name', 'request_hash', name='unique_prompt_given_id_model_name'),
    )

    prompt_id = Column(String, ForeignKey('prompt.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True, index=True)
    given_id = Column(String, primary_key=True, index=True)
    model_name = Column(String, primary_key=True, index=True)
    # Hash of the allowlisted API kwargs that change model output (reasoning,
    # tools, temperature, ...). '' means "no output-affecting kwargs" and is
    # also the backfill value for pre-hash rows. See
    # prompt_response_cache_sql.KWARG_KEYS_THAT_AFFECT_OUTPUT.
    request_hash = Column(String, primary_key=True, index=True, default='', server_default='')
    # The normalized allowlisted kwargs the hash was computed from — kept for
    # auditability (a hash alone can't tell you which effort/temperature
    # produced a row) and to make future re-keying possible.
    request_kwargs = Column(JSONB, nullable=True)
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
