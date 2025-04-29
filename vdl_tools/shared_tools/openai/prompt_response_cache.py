from configparser import ConfigParser

from vdl_tools.shared_tools.cache import Cache
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion
from vdl_tools.shared_tools.prompt_management.prompt_manager import PromptManager, Prompt
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools import log_utils as log
from vdl_tools.shared_tools.tools.logger import logger

S3_DEFAULT_BUCKET_NAME = 'vdl-prompt-response-cache'
S3_DEFAULT_BUCKET_REGION = 'us-east-1'
DEFAULT_FILE_CACHE_DIRECTORY = '.cache/prompt-response'


PROMPT_RESPONSE_NAMESPACE_TEXT = "PROMPT_RESPONSE_NAMESPACE_TEXT"


class GeneralPromptResponseCache(Cache):
    _cached_keys = []
    _local_cache_keys = []

    def __init__(
        self,
        prompt_manager: PromptManager = None,
        prompt: Prompt = None,
        prompt_str: str = None,
        prompt_id: str = None,
        prompt_name: str = "",
        bucket_name: str = S3_DEFAULT_BUCKET_NAME,
        config: ConfigParser = None,
        aws_region: str = S3_DEFAULT_BUCKET_REGION,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        file_cache_directory: str = DEFAULT_FILE_CACHE_DIRECTORY,
        namespace_name = PROMPT_RESPONSE_NAMESPACE_TEXT,
    ):
        config = config or get_configuration()
        if not any([prompt is not None, prompt_str, prompt_id]):
            raise Exception("Need to give at least one of prompt, prompt_str, prompt_id")

        super().__init__(
            bucket_name=bucket_name,
            aws_region=aws_region or config["aws"]["region"],
            aws_access_key_id=aws_access_key_id or config["aws"]["access_key_id"],
            aws_secret_access_key=aws_secret_access_key or config["aws"]["secret_access_key"],
            file_cache_directory=file_cache_directory or config["prompt_response_cache"]["file_cache_directory"],
        )

        self.prompt_manager = prompt_manager or PromptManager(
            config=config,
        )
        self.namespace_name = namespace_name
        self.prompt = self._set_prompt_obj(prompt, prompt_str, prompt_id, prompt_name)

    def _set_prompt_obj(self, prompt, prompt_str, prompt_id, prompt_name):
        """Sets the `prompt` for the class through the prompt, prompt_str, or prompt_id.

        If more than one are given, then biases to:
        prompt -> `prompt_text` -> `id` (via prompt_manager)
        """

        if prompt and isinstance(prompt, Prompt):
            log.info("Prompt object passed, using that")
            log.info(str(prompt))
            self.prompt = prompt

            if prompt_str and prompt_str != prompt.prompt_str:
                log.warn("Given prompt_str and prompt.prompt_str are different")

            if prompt_id and prompt_id != prompt.id:
                log.warn("Given prompt_id and prompt.prompt_id are different")

        elif prompt_str:
            log.info("prompt_str passed, using that to create a prompt")
            self.prompt = Prompt(
                name=prompt_name,
                prompt_str=prompt_str,
                namespace_name=self.namespace_name
            )

        else:
            log.info("Retrieving by prompt_id")
            prompt = self.prompt_manager.prompt_registry.get(prompt_id)
            if not prompt:
                raise Exception(f"No prompt for {prompt_id} found in registry")
            self.prompt = prompt

        if self.prompt.id not in self.prompt_manager.prompt_registry:
            log.info(f"Registering prompt {self.prompt.id}")
            self.prompt_manager.register_prompt(self.prompt)
        return self.prompt

    def create_search_key(self, id: str) -> str:
        return f"{self.prompt.id}/{id}"

    def list_cached_results(self):
        return [x["Key"] for x in
            self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prompt.id,
            ).get('Contents', [])
        ]

    def get_cache_or_run(
        self,
        id: str,
        text,
        model="gpt-4.1-mini",
        return_all=False,
        **kwargs
    ) -> str:

        data = self.get_cache_item(id)

        if not data:
            response = get_completion(
                prompt=self.prompt.prompt_str,
                text=text,
                model=model,
                return_all=return_all,
                **kwargs
            )
            if return_all:
                # Don't cache if it's returning the ChatCompletion object
                return response
            if response:
                self.store_item(id, response)
            else:
                logger.warn(f"No response text for {id}")
                return None
            data = response
        return data
