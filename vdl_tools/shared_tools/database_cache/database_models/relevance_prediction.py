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
class RelevancePrediction(BaseMixin):
    """Classifier predictions over a piece of text, cached by what was scored.

    Keyed by (model_name, model_version, text_id) where text_id is a
    deterministic md5 of the *scored* text — the same convention as
    Embedding's (model_name, text_id) and Geocode's (provider, address_id).

    Two properties fall out of keying this way, both of which the previous
    JSONL cache lacked:

    1. Source-agnostic. Candid orgs and OECD projects can share one table
       without colliding, because the key is the text that was scored rather
       than which script happened to run. The old cache keyed on a caller-
       supplied `id`, which is why OECD predictions were being appended into
       the Candid cache file.

    2. Re-running a retrained model ADDS rows rather than overwriting. Keeping
       model_version in the key means old labels survive, so "did the new model
       change who is relevant?" is a query rather than an archaeology project.

    `given_id` is the caller's own identifier (e.g. an OECD id_unique_project)
    carried along for joins. It is deliberately NOT part of the key: the same
    text scored under two different upstream ids is one prediction, not two.
    """

    __tablename__ = 'relevance_prediction'

    model_name = Column(String, primary_key=True, index=True)
    # Checkpoint / fine-tune identifier. Distinct from model_name so that a
    # retrained ClimateBERT does not silently overwrite the labels that
    # produced an already-published map.
    model_version = Column(String, primary_key=True, default='', server_default='')
    text_id = Column(String, primary_key=True, index=True)

    given_id = Column(String, nullable=True, index=True)
    input_text = Column(String, nullable=False)

    label = Column(String, nullable=False)
    # Softmax confidence for the predicted class. The current pipeline throws
    # this away (argmax only), which makes it impossible to review borderline
    # cases or move the decision threshold without a full re-run.
    score = Column(Float, nullable=True)
    # Full per-class distribution, for threshold changes after the fact.
    response_full = Column(JSONB, nullable=True)
    num_errors = Column(Integer, nullable=True)

    def __init__(self, **kwargs):
        if 'text_id' not in kwargs:
            kwargs['text_id'] = self.create_text_id(kwargs["input_text"])
        else:
            assert kwargs['text_id'] == self.create_text_id(kwargs["input_text"])
        super().__init__(**kwargs)

    @classmethod
    def create_text_id(cls, text):
        return create_deterministic_md5(text)
