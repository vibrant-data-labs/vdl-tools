import numpy as np
from sqlalchemy.orm import Session

from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.embedding_cache import EmbeddingCache
from vdl_tools.shared_tools.openai.openai_api_utils import get_embedding_response_nomic


EMBEDDING_MODEL = 'nomic-1.5'

class EmbeddingCacheNomic(EmbeddingCache):

    def __init__(
        self,
        session: Session,
        model_name: str = EMBEDDING_MODEL,
        truss_api_key: str = None,
    ):
        self.session = session
        self.model_name = model_name
        if not truss_api_key:
            truss_api_key = get_configuration()['baseten']['api_key']
        self.truss_api_key = truss_api_key


    def get_embedding(self, texts, **kwargs):
        return get_embedding_response_nomic(
            texts=texts,
            truss_api_key=self.truss_api_key,
            **kwargs
        )
