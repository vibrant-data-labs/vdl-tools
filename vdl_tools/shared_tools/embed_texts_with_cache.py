import numpy as np

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.embedding_cache import EmbeddingCache
from vdl_tools.shared_tools.openai.embedding_cache_nomic import EmbeddingCacheNomic


EMBEDDING_PROVIDER = {
    "openai": EmbeddingCache,
    "nomic": EmbeddingCacheNomic,
}


def embed_texts_with_cache(
    ids_texts: list[tuple[str, str]],
    use_cached_result: bool = True,
    n_per_commit: int = 1500,
    max_workers=3,
    return_flat: bool = True,
    embedding_provider="openai",
    embedding_model="text-embedding-3-large",
):
    if embedding_provider not in EMBEDDING_PROVIDER:
        raise ValueError(f"Invalid embedding provider: {embedding_provider}")
    with get_session() as session:
        cache = EMBEDDING_PROVIDER[embedding_provider](
            session=session,
            model_name=embedding_model,
        )

        res = cache.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_result,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
        )

    entity_embeddings = {
        k: v['embedding']
        for k, v in res.items() if 'embedding' in v
    }
    if not return_flat:
        return entity_embeddings
    flat_embeddings = []
    for id_, _ in ids_texts:
        if id_ in entity_embeddings:
            flat_embeddings.append(entity_embeddings[id_])
    return np.stack(flat_embeddings)
