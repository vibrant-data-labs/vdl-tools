from configparser import ConfigParser
import json
from uuid import uuid5, NAMESPACE_URL

import boto3
from botocore.exceptions import ClientError

from vdl_tools.shared_tools.tools.config_utils import get_configuration
import vdl_tools.shared_tools.tools.log_utils as log


S3_DEFAULT_BUCKET_NAME = 'vdl-prompt-management'
S3_DEFAULT_BUCKET_REGION = 'us-east-1'
S3_PROMPT_FILENAME = 'vdl_prompts.json'
DEFAULT_LOCAL_FILE = '/tmp/vdl_prompts.json'


class Prompt():
    def __init__(
        self,
        name: str,
        prompt_str: str,
        namespace_name: str,
        id: str = None,
    ):
        self.name = name
        self.prompt_str = prompt_str
        self.namespace_name = namespace_name
        self.namespace = uuid5(NAMESPACE_URL, namespace_name)

        if id:
            self._validate_id(id)
            self.id = id
        else:
            self.id = self.make_uuid()

    def make_uuid(self):
        uuid_text = uuid5(self.namespace, name=self.prompt_str).hex
        return uuid_text

    def _validate_id(self, _id):
        inferred_id = self.make_uuid()
        assert inferred_id == _id

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "prompt_str": self.prompt_str,
            "namespace_name": self.namespace_name,
        }

    def __repr__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class PromptManager:
    def __init__(
        self,
        config: ConfigParser = None
    ) -> None:

        self.bucket_name = S3_DEFAULT_BUCKET_NAME
        self.config = config or get_configuration()
        self.s3_client = self._get_boto_client()
        self._check_create_bucket(self.bucket_name)
        self.prompt_registry = self.load_prompts()

    def _check_create_bucket(self, bucket):
        try:
            self.client.head_bucket(Bucket=bucket)
        except self.client.exceptions.ClientError:
            log.warn(f'Bucket "{bucket}" does not exist, creating...')
            self.client.create_bucket(Bucket=bucket)

    def _get_boto_client(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.config["aws"]["access_key_id"],
            aws_secret_access_key=self.config["aws"]["secret_access_key"],
            region_name=self.config["aws"]["region"],
        )
        return self.client

    def load_prompts(self):
        temp_prompt_registry = {}
        try:
            prompts_str = self.client.get_object(
                Bucket=self.bucket_name, Key=S3_PROMPT_FILENAME)['Body'].read().decode('utf-8')
            prompts = json.loads(prompts_str)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                log.info('No object found - returning empty')
                prompts = []
            else:
                raise

        for prompt_dict in prompts:
            prompt = Prompt(**prompt_dict)
            temp_prompt_registry[prompt.id] = prompt
        return temp_prompt_registry

    def save_prompts(self, prompt_registry: dict[str, Prompt] | None = None):
        current_saved_prompts = self.load_prompts()
        prompt_registry = prompt_registry or self.prompt_registry
        # Make sure you are additive to whatever is currently there so you don't overwrite / delete
        for prompt_id, prompt in prompt_registry.items():
            if prompt_id not in current_saved_prompts:
                current_saved_prompts[prompt_id] = prompt
        all_prompts_dicts = [
            x.to_dict()
            for x in current_saved_prompts.values()
        ]
        all_prompts_binary = json.dumps(all_prompts_dicts).encode()
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=S3_PROMPT_FILENAME,
            Body=all_prompts_binary,
        )

    def register_prompt(self, prompt: Prompt):
        # Re-load the prompts in case they have been updated by another process
        self.prompt_registry = self.load_prompts()
        self.prompt_registry[prompt.id] = prompt
        self.save_prompts()

    def register_prompts(self, prompts):
        for prompt in prompts:
            self.register_prompt(prompt)

    def to_dict(self):
        return {k: v.to_dict() for k, v in self.prompt_registry.items()}
