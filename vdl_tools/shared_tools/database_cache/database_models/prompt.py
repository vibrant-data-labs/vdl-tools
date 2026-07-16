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
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5


PROMPT_RESPONSE_NAMESPACE_TEXT = "PROMPT_RESPONSE_NAMESPACE_TEXT"

# API kwargs that change what the model produces, and therefore belong in the
# cache key. Anything NOT on this list is treated as cosmetic and ignored when
# hashing — notably `text_format`: `response_full` is stored raw and parsed at
# retrieval time, so the same row can be re-parsed against a different Pydantic
# model without a cache miss. New output-affecting kwargs must be added here
# explicitly (via PR) — opt-in keeps cache invalidation predictable.
KWARG_KEYS_THAT_AFFECT_OUTPUT = frozenset({
    "reasoning",
    "tools",
    "tool_choice",
    "temperature",
    "top_p",
    "max_output_tokens",
    "seed",
    "service_tier",
})


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
        # Named to match the constraint the alembic lineage manages
        # (223e827c20cd created it as prompt_response_pkey), so a
        # create_all-bootstrapped DB stays migratable.
        PrimaryKeyConstraint('prompt_id', 'given_id', 'model_name', 'request_hash', name='prompt_response_pkey'),
    )

    prompt_id = Column(String, ForeignKey('prompt.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True, index=True)
    given_id = Column(String, primary_key=True, index=True)
    model_name = Column(String, primary_key=True, index=True)
    # Hash of the allowlisted API kwargs that change model output (reasoning,
    # tools, temperature, ...). '' means "no output-affecting kwargs" and is
    # also the backfill value for pre-hash rows. Derived from request_kwargs
    # (see KWARG_KEYS_THAT_AFFECT_OUTPUT above), never passed independently —
    # __init__ enforces the pairing the way it does for text_id/input_text.
    # No standalone index: reads always pair request_hash with indexed
    # columns, and it is '' for virtually all rows (near-zero cardinality).
    request_hash = Column(String, primary_key=True, default='', server_default='')
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
        # request_hash mirrors text_id: derived from request_kwargs, never
        # trusted. A supplied hash must match the derived one.
        norm_kwargs = self.normalize_request_kwargs(kwargs.get('request_kwargs'))
        derived_hash = self.create_request_hash(norm_kwargs)
        if 'request_hash' in kwargs:
            assert kwargs['request_hash'] == derived_hash, (
                f"request_hash {kwargs['request_hash']!r} does not match the "
                f"hash derived from request_kwargs ({derived_hash!r})"
            )
        kwargs['request_hash'] = derived_hash
        kwargs['request_kwargs'] = norm_kwargs or None
        super().__init__(**kwargs)

    @classmethod
    def create_text_id(cls, text):
        if not isinstance(text, (str, int)):
            text = json.dumps(text)
        return make_uuid(
            text,
            namespace_text=PROMPT_RESPONSE_NAMESPACE_TEXT
        )

    @classmethod
    def normalize_request_kwargs(cls, request_kwargs: dict | None) -> dict:
        """Filter kwargs to the output-affecting allowlist and normalize values.

        Values are round-tripped through JSON with sorted keys so structural
        equality maps to the same normalized form (e.g. dict key order or
        tool list contents don't produce spurious cache misses). Idempotent,
        so raw API kwargs and already-normalized dicts are both fine.

        Allowlisted values must be JSON-serializable: a non-serializable
        value (e.g. a callable inside ``tools``) raises TypeError here rather
        than being coerced through ``str()`` — repr-based coercion would
        embed memory addresses, making the hash differ every process run
        (permanent cache misses) and storing junk in ``request_kwargs``.
        """
        norm = {}
        for key in sorted(request_kwargs or {}):
            if key not in KWARG_KEYS_THAT_AFFECT_OUTPUT:
                continue
            norm[key] = json.loads(json.dumps(request_kwargs[key], sort_keys=True))
        return norm

    @classmethod
    def create_request_hash(cls, request_kwargs: dict | None) -> str:
        """Derive the cache-key hash from (raw or normalized) API kwargs.

        Empty/no allowlisted kwargs hash to '' so those calls keep matching
        pre-hash rows (which were backfilled with '').
        """
        norm = cls.normalize_request_kwargs(request_kwargs)
        if not norm:
            return ""
        return create_deterministic_md5(json.dumps(norm, sort_keys=True))
