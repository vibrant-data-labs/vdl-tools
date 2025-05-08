from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor as ThreadPool

from sqlalchemy import select, tuple_
from more_itertools import chunked

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion
from vdl_tools.shared_tools.tools.logger import logger

import logging
logger.setLevel(logging.DEBUG)


def get_prompt_by_id(prompt_id: str, session):
    prompt_by_id_stmnt = select(Prompt).where(Prompt.id == prompt_id)
    return session.scalars(prompt_by_id_stmnt).first()


def load_prompts(session):
    return {x.id: x for x in session.query(Prompt).all()}


def register_prompt(prompt: Prompt, session):
    # Add the session to the prompt object
    prompt_obj = get_prompt_by_id(prompt.id, session)
    print(prompt_obj)
    if not prompt_obj:
        session.add(prompt)
        prompt_obj = get_prompt_by_id(prompt.id, session)
    return prompt_obj


class PromptResponseCacheSQL():

    def __init__(
        self,
        session,
        prompt: Prompt = None,
        prompt_str: str = None,
        prompt_id: str = None,
        prompt_name: str = "",
        prompt_description: str = "",
    ):
        if not any([prompt is not None, prompt_str, prompt_id]):
            raise Exception("Need to give at least one of prompt, prompt_str, prompt_id")

        self.session = session or get_session()
        self.prompt = self._set_prompt_obj(
            prompt=prompt,
            prompt_str=prompt_str,
            prompt_id=prompt_id,
            prompt_name=prompt_name,
            prompt_description=prompt_description,
        )

    def _set_prompt_obj(
        self, prompt, prompt_str, prompt_id, prompt_name, prompt_description):
        """Sets the `prompt` for the class through the prompt, prompt_str, or prompt_id.

        If more than one are given, then biases to:
        prompt -> `prompt_text` -> `id` (via prompt_manager)
        """

        if prompt and isinstance(prompt, dict):
            prompt = Prompt(**prompt)

        all_prompts = load_prompts(self.session)
        if prompt and isinstance(prompt, Prompt):
            logger.info("Prompt object passed, using that")
            logger.info(str(prompt))
            temp_prompt = prompt

            if prompt_str and prompt_str != prompt.prompt_str:
                logger.warning("Given prompt_str and prompt.prompt_str are different")

            if prompt_id and prompt_id != prompt.id:
                logger.warning("Given prompt_id and prompt.prompt_id are different")
        elif prompt_id:
            logger.info("Retrieving by prompt_id")
            temp_prompt = all_prompts.get(prompt_id)
            if not temp_prompt:
                raise Exception(f"No prompt for {prompt_id} found in registry")

        elif prompt_str:
            logger.warning("prompt_str passed, using that to create a prompt")
            temp_prompt = Prompt(
                name=prompt_name,
                prompt_str=prompt_str,
                description=prompt_description,
            )

        else:
            logger.info("Retrieving by prompt_id")
            temp_prompt = all_prompts.get(prompt_id)
            if not temp_prompt:
                raise Exception(f"No prompt for {prompt_id} found in registry")

        if temp_prompt.id not in all_prompts:
            logger.info("Registering prompt %s", temp_prompt.id)
            register_prompt(temp_prompt, self.session)
        else:
            temp_prompt = all_prompts[temp_prompt.id]
        return temp_prompt

    def get_prompt_response_obj(self, given_id: str, text: str):
        text_id = PromptResponse.create_text_id(text)

        prompt_response_obj = (
            self.session
            .query(PromptResponse)
            .filter(
                PromptResponse.prompt_id == self.prompt.id,
                PromptResponse.text_id == text_id,
                PromptResponse.given_id == given_id,
            )
        )
        return prompt_response_obj.first()

    def get_prompt_response_obj_bulk(
        self,
        given_ids_texts: list[tuple[str, str]]
    ):
        logger.info(
            "Starting to pull %s previous results for prompt: %s",
            len(given_ids_texts),
            self.prompt.name,
        )
        given_ids, _ = zip(*given_ids_texts)
        text_ids = [PromptResponse.create_text_id(x[1]) for x in given_ids_texts]
        given_ids_text_ids = list(zip(given_ids, text_ids))

        found_rows = []
        for i, chunk in enumerate(chunked(given_ids_text_ids, 4000)):
            logger.info("Pulling chunk %s of %s", i, len(given_ids_text_ids) // 4000)
            chunk_found_rows = (
                self.session
                .query(PromptResponse)
                .filter(
                    PromptResponse.prompt_id == self.prompt.id,
                    tuple_(PromptResponse.given_id, PromptResponse.text_id).in_(chunk),
                )
            ).all()
            found_rows.extend(chunk_found_rows)
            logger.info("Found %s total rows", len(found_rows))

        found_rows_to_errors = {(x.given_id, x.text_id): x.num_errors for x in found_rows}
        found_rows = [x for x in found_rows if not found_rows_to_errors.get((x.given_id, x.text_id))]

        found_rows_keys = found_rows_to_errors.keys()
        unfound_ids_or_errors = {
            x: found_rows_to_errors.get(x, 0) for x in given_ids_text_ids
            if x not in found_rows_keys or found_rows_to_errors.get(x)
        }
        logger.info("%s previous found, %s unfound", len(found_rows), len(unfound_ids_or_errors))
        return found_rows, unfound_ids_or_errors

    def store_error(
        self,
        given_id: str,
        text,
        response_full,
    ):
        logger.info("Storing error for %s, %s", self.prompt.name, given_id)
        text_id = PromptResponse.create_text_id(text)
        previous_response = (
            self.session
            .query(PromptResponse)
            .filter(
                PromptResponse.prompt_id == self.prompt.id,
                PromptResponse.text_id == text_id,
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
            prompt_response_obj = previous_response
        else:
            prompt_response_obj = PromptResponse(
                prompt_id=self.prompt.id,
                given_id=given_id,
                input_text=text,
                response_full=response_full,
                num_errors=1,
            )
            self.session.merge(prompt_response_obj)
        return prompt_response_obj

    def store_item(
        self,
        given_id: str,
        text,
        response,
    ):
        prompt_response_obj = PromptResponse(
            prompt_id=self.prompt.id,
            given_id=given_id,
            input_text=text,
            response_full=response.model_dump_json(),
            response_text=response.choices[0].message.content,
            num_errors=None,
        )

        logger.debug("Storing response for %s, %s", self.prompt.name, given_id)
        self.session.merge(prompt_response_obj)
        return prompt_response_obj

    def get_completion_catch_error(self, prompt_str, text, model="gpt-4.1-mini", **kwargs):
        try:
            completion = self.get_completion(prompt_str, text, model=model, **kwargs)
            return completion, False
        except Exception as ex:
            logger.error("Error getting completion: %s", ex)
            response = {
                "message": str(ex),
            }
            return response, True

    def get_completion(self, prompt_str, text, model="gpt-4.1-mini", **kwargs):
        """Adding this method here in case a subclass wants to override it.

        Returns the completion and a boolean indicating if there was an error.
        """
        return get_completion(
            prompt=prompt_str,
            text=text,
            model=model,
            **kwargs,
        )

    def _get_cache_or_run(
        self,
        given_id: str,
        text,
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        **kwargs
    ) -> str:

        if use_cached_result:
            data = self.get_prompt_response_obj(
                given_id=given_id,
                text=text,
            )
            if data:
                logger.info("Found cached response for %s", given_id)
                return data.to_dict()

        response, error = self.get_completion_catch_error(
            prompt_str=self.prompt.prompt_str,
            text=text,
            model=model,
            return_all=True,
            **kwargs
        )
        if not error:
            data = self.store_item(
                given_id=given_id,
                text=text,
                response=response,
            )
        else:
            logger.warning("No response text for %s", given_id)
            self.store_error(
                given_id=given_id,
                text=text,
                response_full=response,
            )
            return None
        return data.to_dict()

    def get_cache_or_run(
        self,
        given_id: str,
        text,
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        **kwargs,
    ):
        return self._get_cache_or_run(
            given_id=given_id,
            text=text,
            model=model,
            use_cached_result=use_cached_result,
            **kwargs,
        )

    def _bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        n_per_commit: int = 50,
        max_workers=3,
        max_errors=1,
        **kwargs
    ) -> dict[str, dict]:
        """
        This will look up previous results for a `(prompt_id, given_id)` combinations based on the
        `(prompt_id, given_id, text_id)` combinations, where `text_id` is the hash of the input text.
        This way if the text changes, the result is re-run with the new text and the new result
        for the new text is stored for the `(prompt_id, given_id)` combination it will overwrite the old version.

        Parameters
        ----------
        given_ids_texts : list[tuple[str, str]]
            A list of `(given_id, text)` tuples to run the completion on.
            given_id can be a string identifier that helps a human look up what the completion was for.
            An example would be a URL if this is a website summarization task.
        model : str, optional
            OpenAI model to use, by default "gpt-4.1-mini"
        use_cached_result : bool, optional
            Whether to use the cached result, by default True
        n_per_commit : int, optional
            Number of records to run per commit, by default 50
        max_workers : int, optional
            Number of workers to use simultaneously, by default 3
        max_errors : int, optional
            Maximum number of errors to allow for a given (given_id, text), by default 1

        Returns
        -------
        dict
            A dictionary of the results for each given_id
        """
        if not given_ids_texts:
            logger.warning("No given_ids_texts passed")
            return {}

        # Remove duplicates when the text is a string
        # Sometimes it's a dict like with taxonmy mapping
        if isinstance(given_ids_texts[0][1], str):
            given_ids_texts = list(set([(x[0], x[1]) for x in given_ids_texts]))
        else:
            given_ids_texts = given_ids_texts

        if use_cached_result:
            found_rows, unfound_ids_errors = self.get_prompt_response_obj_bulk(given_ids_texts)
            unfound_rows = []
            for given_id, text in given_ids_texts:
                text_id = PromptResponse.create_text_id(text)
                errors_for_id = unfound_ids_errors.get((given_id, text_id), 0)
                if (
                    (given_id, text_id) in unfound_ids_errors and
                    (errors_for_id == 0 or errors_for_id < max_errors)
                ):
                    unfound_rows.append((given_id, text))

        else:
            unfound_rows = given_ids_texts
            found_rows = []

        res = {x.given_id: x.to_dict() for x in found_rows}

        len_unfound = len(unfound_rows)
        logger.info("Found %s cached responses", len(res))
        logger.info("Need to run %s responses", len_unfound)

        def _get_completion_store(given_id_text):
            given_id, text = given_id_text
            response, error = self.get_completion_catch_error(
                prompt_str=self.prompt.prompt_str,
                text=text,
                model=model,
                return_all=True,
                **kwargs
            )
            if error:
                self.store_error(
                    given_id=given_id,
                    text=text,
                    response_full=response,
                )
                return None

            data = self.store_item(
                given_id=given_id,
                text=text,
                response=response,
            )
            return data.to_dict()

        n_run = 0
        with ThreadPool(max_workers=max_workers) as executor:
            for chunk in chunked(unfound_rows, n_per_commit):
                given_ids, _ = zip(*chunk)
                try:
                    logger.info("Running GPT %s on chunk of %s", self.prompt.name, len(chunk))
                    results = list(executor.map(_get_completion_store, chunk))
                    for given_id, response in zip(given_ids, results):
                        if response:
                            res[given_id] = response
                    self.session.commit()
                    n_run += len(results)
                except KeyboardInterrupt:
                    logger.warning("Received KeyboardInterrupt, returning the currently scraped data...")
                    break

                if len(res) % 500 == 0:
                    logger.info("Completed %s of %s", len(res), len_unfound)
        return res

    def bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        model="gpt-4.1-mini",
        use_cached_result: bool = True,
        n_per_commit: int = 50,
        max_workers=3,
        max_errors=1,
        **kwargs
    ):
        return self._bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            model=model,
            use_cached_result=use_cached_result,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
            **kwargs,
        )
