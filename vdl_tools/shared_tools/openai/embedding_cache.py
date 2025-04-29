from collections import defaultdict
from multiprocessing.pool import ThreadPool

from more_itertools import chunked
import numpy as np
from sqlalchemy.orm import Session

from vdl_tools.shared_tools.database_cache.database_models.embedding import Embedding
from vdl_tools.shared_tools.openai.openai_api_utils import get_embedding_response
from vdl_tools.shared_tools.tools.logger import logger


EMBEDDING_MODEL = 'text-embedding-3-large'


class EmbeddingCache():

    def __init__(
        self,
        session: Session,
        model_name: str = EMBEDDING_MODEL,
    ):
        self.session = session
        self.model_name = model_name

    def get_embedding(self, texts, **kwargs):
        response = get_embedding_response(
            texts=texts,
            model_name=self.model_name,
        )
        return [x.embedding for x in response.data]

    def get_embedding_obj(self, text: str):
        text_id = Embedding.create_text_id(text)

        embedding = (
            self.session
            .query(
                Embedding
            )
            .filter(
                Embedding.model_name == self.model_name,
                Embedding.text_id == text_id,
            )
            .first()
        )
        return embedding
    
    def get_embedding_obj_bulk(self, texts: str):
        logger.info(
            "Starting to pull %s previous ids for model_name: %s",
            len(texts),
            self.model_name,
        )

        text_ids = [Embedding.create_text_id(text) for text in texts]
        found_rows_ids = (
            self.session
            .query(
                Embedding.model_name,
                Embedding.text_id,
                Embedding.num_errors,
            )
            .filter(
                Embedding.model_name == self.model_name,
                Embedding.text_id.in_(text_ids)
            )
            .all()
        )

        found_rows_to_errors = {x.text_id: x.num_errors for x in found_rows_ids}
        found_rows_ids = [x.text_id for x in found_rows_ids if not found_rows_to_errors.get(x.text_id)]

        logger.info(
            "Starting to pull %s previous results for model_name: %s",
            len(texts),
            self.model_name,
        )
        found_rows = (
            self.session
            .query(
                Embedding.text_id,
                Embedding.given_id,
                Embedding.embedding,
            )
            .filter(
                Embedding.model_name == self.model_name,
                Embedding.text_id.in_(found_rows_ids)
            )
            .all()
        )

        found_rows_keys = found_rows_to_errors.keys()
        unfound_ids_or_errors = {
            x: found_rows_to_errors.get(x, 0) for x in text_ids
            if x not in found_rows_keys or found_rows_to_errors.get(x)
        }
        logger.info("%s previous found, %s unfound", len(found_rows), len(unfound_ids_or_errors))
        return found_rows, unfound_ids_or_errors

    def store_error(
        self,
        text: str,
        response_full: dict,
        given_id: str,
    ):
        text_id = Embedding.create_text_id(text)
        logger.info("Storing error for %s, %s", self.model_name, given_id)
        previous_response = (
            self.session.query(Embedding)
            .filter(
                Embedding.model_name == self.model_name,
                Embedding.text_id == text_id,
            )
            .first()
        )
        if previous_response:
            previous_response.response_full = response_full
            if previous_response.num_errors:
                previous_response.num_errors += 1
            else:
                previous_response.num_errors = 1
            self.session.merge(previous_response)
            return previous_response
        else:
            embedding_obj = Embedding(
                model_name=self.model_name,
                given_id=given_id,
                input_text=text,
                response_full=response_full,
                num_errors=1,
            )
            self.session.merge(embedding_obj)
        return embedding_obj

    def store_item(
        self,
        given_id: str,
        text: str,
        response,
    ):
        embedding_obj = Embedding(
            model_name=self.model_name,
            given_id=given_id,
            input_text=text,
            response_full={"data": response},
            embedding=np.array(response),
        )

        self.session.merge(embedding_obj)
        return embedding_obj

    def get_cache_or_run(
        self,
        text: str,
        given_id: str = None,
        use_cached_result: bool = True,
        **kwargs
    ) -> str:

        given_id = given_id or Embedding.create_text_id(text)

        if use_cached_result:
            data = self.get_embedding_obj(text=text)
            if data:
                logger.info("Found cached response for %s", given_id)
                return data.to_dict()

        try:
            response = self.get_embedding(
                texts=[text],
                **kwargs,
            )
            if response is not None:
                data = self.store_item(
                    given_id=given_id,
                    text=text,
                    response=response[0],
                )
            else:
                logger.warning("No response text for %s", given_id)
                return None

        except Exception as ex:
            logger.error("Error getting completion: %s", ex)
            response = {
                "message": str(ex),
            }
            data = self.store_error(
                text=text,
                response_full=response,
                given_id=given_id,
            )
        return data.to_dict()

    def bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        use_cached_result: bool = True,
        n_per_commit: int = 1500,
        max_workers=3,
        max_errors=3,
        **kwargs
    ) -> str:

        _, texts = zip(*given_ids_texts)
        text_id_to_given_ids = defaultdict(list)
        text_id_to_text = {}
        for given_id, text in given_ids_texts:
            text_id = Embedding.create_text_id(text)
            given_id = given_id or text_id
            text_id_to_given_ids[text_id].append(given_id)
            text_id_to_text[text_id] = text

        if use_cached_result:
            found_rows, unfound_ids_errors = self.get_embedding_obj_bulk(texts=texts)

            # Some texts could be duplicated
            unique_unfound_rows = set()
            unfound_rows = []
            for _, text in given_ids_texts:
                text_id = Embedding.create_text_id(text)
                errors_for_id = unfound_ids_errors.get(text_id, 0)
                if (
                    text_id in unfound_ids_errors and
                    (errors_for_id == 0 or errors_for_id < max_errors)
                ):
                    unfound_rows.append((text_id, text))
                    unique_unfound_rows.add((text_id, text))
        else:
            unfound_rows = given_ids_texts
            unique_unfound_rows = {(Embedding.create_text_id(x[1]), x[1]) for x in given_ids_texts}
            found_rows = []

        res = {}
        for x in found_rows:
            text_id = x.text_id
            given_ids = text_id_to_given_ids[text_id]
            for given_id in given_ids:
                res[given_id] = {
                    "given_id": x.given_id,
                    "text_id": x.text_id,
                    "embedding": x.embedding,
                }
        text_id_to_embeddings = {x.text_id: x.embedding for x in found_rows}

        len_unfound = len(unfound_rows)
        logger.info("Found %s cached responses", len(found_rows))
        logger.info("Need to run %s responses", len_unfound)

        def _run_chunk(i_chunk):
            i, chunk = i_chunk
            logger.info("Starting mini chunk %s of len %s", i, len(chunk))
            chunk_texts = [x[1] for x in chunk]
            text_id_to_embedding = {}
            embeddings = self.get_embedding(
                texts=chunk_texts,
                **kwargs,
            )
            for idx, (_, text) in enumerate(chunk):
                text_id = Embedding.create_text_id(text)
                text_id_to_embedding[text_id] = embeddings[idx]
            return text_id_to_embedding

        commit_chunks = chunked(list(unique_unfound_rows), n_per_commit)

        # In unfound rows

        for i, commit_chunk in enumerate(commit_chunks):
            chunks = enumerate(chunked(commit_chunk, n_per_commit//max_workers))
            i_chunks = [(i+1 * y+1, x) for y, x in chunks]
            with ThreadPool(processes=max_workers) as executor:
                chunk_results = list(executor.map(_run_chunk, i_chunks))

            added_to_commit = 0
            for result_chunk in chunk_results:
                text_id_to_embeddings.update(result_chunk)
                added_to_commit += len(result_chunk)

                for text_id, embedding in result_chunk.items():
                    text = text_id_to_text[text_id]
                    if embedding is not None:
                        data = self.store_item(
                            given_id=None,
                            text=text,
                            response=embedding,
                        )
                        given_ids = text_id_to_given_ids[text_id]
                        for given_id in given_ids:
                            res[given_id] = {
                                "given_id": data.given_id,
                                "text_id": data.text_id,
                                "embedding": data.embedding,
                            }
                    else:
                        logger.error("No response for %s", text_id)
                        data = self.store_error(
                            text=text,
                            response_full={"message": "No response"},
                            given_id=None,
                        )
                        given_ids = text_id_to_given_ids[text_id]
                        for given_id in given_ids:
                            res[given_id] = {
                                "given_id": data.given_id,
                                "text_id": data.text_id,
                                "embedding": data.embedding,
                            }
            logger.info("Committing chunk %s of len %s", i, added_to_commit)
            logger.info("Total committed %s", len(res))
            self.session.commit()

        return res
