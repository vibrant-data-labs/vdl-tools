from collections import defaultdict
from multiprocessing.pool import ThreadPool
from typing import Any
import datetime as dt

from more_itertools import chunked
import numpy as np
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from vdl_tools.shared_tools.database_cache.database_models.embedding import Embedding
from vdl_tools.shared_tools.openai.openai_api_utils import get_embedding_response
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    _UNSET,
    _resolve_cache_flags,
)
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

    def _build_success_row(
        self,
        given_id: str,
        text: str,
        embedding,
    ) -> dict[str, Any]:
        """Build a row dict for a successful embedding."""
        text_id = Embedding.create_text_id(text)
        return {
            "model_name": self.model_name,
            "text_id": text_id,
            "given_id": given_id if given_id is not None else text_id,
            "input_text": text,
            "response_full": {"data": embedding},
            "embedding": embedding,
            "num_errors": None,
        }

    def _build_error_row(
        self,
        given_id: str,
        text: str,
        response_full: dict,
    ) -> dict[str, Any]:
        """Build a row dict for a failed embedding call."""
        text_id = Embedding.create_text_id(text)
        return {
            "model_name": self.model_name,
            "text_id": text_id,
            "given_id": given_id if given_id is not None else text_id,
            "input_text": text,
            "response_full": response_full,
            "num_errors": 1,
        }

    def _upsert_success_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert successful embedding rows via PG ON CONFLICT.

        Composite PK is (model_name, text_id). On conflict, overwrites the
        embedding fields, clears num_errors, and bumps date_updated (Core
        doesn't fire the ORM `onupdate` hook through `ON CONFLICT DO UPDATE`).
        """
        if not rows:
            return
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            key = (row["model_name"], row["text_id"])
            deduped[key] = row
        rows = list(deduped.values())

        now = dt.datetime.utcnow()
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(Embedding).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["model_name", "text_id"],
            set_={
                "given_id": stmt.excluded.given_id,
                "input_text": stmt.excluded.input_text,
                "response_full": stmt.excluded.response_full,
                "embedding": stmt.excluded.embedding,
                "num_errors": None,
                # date_added intentionally NOT in the SET clause — preserve
                # the original "first cached at" timestamp on refresh.
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    def _upsert_error_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert error rows, incrementing num_errors on conflict."""
        if not rows:
            return
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            key = (row["model_name"], row["text_id"])
            deduped[key] = row
        rows = list(deduped.values())

        now = dt.datetime.utcnow()
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(Embedding).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["model_name", "text_id"],
            set_={
                "response_full": stmt.excluded.response_full,
                "num_errors": func.coalesce(Embedding.num_errors, 0) + 1,
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    def store_error(
        self,
        text: str,
        response_full: dict,
        given_id: str,
    ):
        """Single-row error store (used by `get_cache_or_run`)."""
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
            row = self._build_error_row(given_id, text, response_full)
            embedding_obj = Embedding(**row)
            self.session.merge(embedding_obj)
        return embedding_obj

    def store_item(
        self,
        given_id: str,
        text: str,
        response,
    ):
        """Single-row success store (used by `get_cache_or_run`)."""
        row = self._build_success_row(given_id, text, response)
        embedding_obj = Embedding(**row)
        self.session.merge(embedding_obj)
        return embedding_obj

    def get_cache_or_run(
        self,
        text: str,
        given_id: str = None,
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        use_cached_result=_UNSET,
        **kwargs
    ) -> str:
        """Return a cached embedding for ``text`` or call the API and (optionally) cache it.

        Parameters
        ----------
        text : str
            Input text to embed (also used to compute the cache key).
        given_id : str, optional
            Caller-supplied identifier. Defaults to the text hash.
        read_from_cache : bool, optional
            If True, return a cached embedding when available. If False,
            always call the API (force-refresh). Default is True.
        write_to_cache : bool, optional
            If True, persist new embeddings (and errors) to the cache. If
            False, the cache is left untouched. Default is True.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``.
            When supplied, overrides ``read_from_cache``.
        **kwargs
            Forwarded to ``get_embedding_response``.
        """
        read_from_cache, write_to_cache = _resolve_cache_flags(
            read_from_cache, write_to_cache, use_cached_result,
        )

        given_id = given_id or Embedding.create_text_id(text)

        if read_from_cache:
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
                if write_to_cache:
                    data = self.store_item(
                        given_id=given_id,
                        text=text,
                        response=response[0],
                    )
                    return data.to_dict()
                # Read-only / passthrough: build the dict without persisting.
                row = self._build_success_row(
                    given_id=given_id,
                    text=text,
                    embedding=response[0],
                )
                return {
                    "model_name": row["model_name"],
                    "text_id": row["text_id"],
                    "given_id": row["given_id"],
                    "input_text": row["input_text"],
                    "embedding": row["embedding"],
                }
            else:
                logger.warning("No response text for %s", given_id)
                return None

        except Exception as ex:
            logger.error("Error getting completion: %s", ex)
            response = {
                "message": str(ex),
            }
            if write_to_cache:
                data = self.store_error(
                    text=text,
                    response_full=response,
                    given_id=given_id,
                )
                return data.to_dict()
            return None

    def bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        n_per_commit: int = 1500,
        max_workers=10,
        max_errors=3,
        use_cached_result=_UNSET,
        **kwargs
    ) -> str:
        """Bulk get cached or fresh embeddings for many (given_id, text) pairs.

        Parameters
        ----------
        given_ids_texts : list[tuple[str, str]]
            (given_id, text) pairs to embed.
        read_from_cache : bool, optional
            If True, consult the cache before calling the API. Default is True.
        write_to_cache : bool, optional
            If True, persist results to the cache. Default is True.
        n_per_commit : int, optional
            Items per DB commit. Default is 1500.
        max_workers : int, optional
            Parallel API workers. Default is 10.
        max_errors : int, optional
            Max errors per text before skipping. Default is 3.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``.
            When supplied, overrides ``read_from_cache``.
        **kwargs
            Forwarded to ``get_embedding_response``.
        """
        read_from_cache, write_to_cache = _resolve_cache_flags(
            read_from_cache, write_to_cache, use_cached_result,
        )

        _, texts = zip(*given_ids_texts)
        text_id_to_given_ids = defaultdict(list)
        text_id_to_text = {}
        for given_id, text in given_ids_texts:
            text_id = Embedding.create_text_id(text)
            given_id = given_id or text_id
            text_id_to_given_ids[text_id].append(given_id)
            text_id_to_text[text_id] = text

        if read_from_cache:
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

            success_rows: list[dict[str, Any]] = []
            error_rows: list[dict[str, Any]] = []
            added_to_commit = 0
            for result_chunk in chunk_results:
                text_id_to_embeddings.update(result_chunk)
                added_to_commit += len(result_chunk)

                for text_id, embedding in result_chunk.items():
                    text = text_id_to_text[text_id]
                    given_ids = text_id_to_given_ids[text_id]
                    # Use the first associated given_id as the row's given_id;
                    # downstream `res` is keyed by every associated given_id.
                    row_given_id = given_ids[0] if given_ids else text_id

                    if embedding is not None:
                        row = self._build_success_row(
                            given_id=row_given_id,
                            text=text,
                            embedding=embedding,
                        )
                        success_rows.append(row)
                        for given_id in given_ids:
                            res[given_id] = {
                                "given_id": row["given_id"],
                                "text_id": row["text_id"],
                                "embedding": row["embedding"],
                            }
                    else:
                        logger.error("No response for %s", text_id)
                        row = self._build_error_row(
                            given_id=row_given_id,
                            text=text,
                            response_full={"message": "No response"},
                        )
                        error_rows.append(row)
                        for given_id in given_ids:
                            res[given_id] = {
                                "given_id": row["given_id"],
                                "text_id": row["text_id"],
                                "embedding": None,
                            }

            # Bulk upsert (one statement per kind) instead of per-item merge.
            # Skipped entirely when `write_to_cache` is False — results are
            # still returned in `res`, the cache is just left untouched.
            if write_to_cache:
                self._upsert_success_rows(success_rows)
                self._upsert_error_rows(error_rows)
                logger.info("Committing chunk %s of len %s", i, added_to_commit)
                logger.info("Total committed %s", len(res))
                self.session.commit()

        return res
