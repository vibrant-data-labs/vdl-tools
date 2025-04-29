# Prompt Management
The PromptManager is a tool we can utilize for generating unique ids for prompts of text and storing the values in S3 (or another persistent, shared storage in the future).

## Quick Start
```python
from shared_tools.prompt_management.prompt_manager import Prompt, PromptManager
from shared_tools.tools.config_utils import get_configuration

# Assumes you have a config in the root directory or have set the
# environment variable VDL_GLOBAL_CONFIG_PATH that points to a config.ini
config = get_configuration()
prompt_manager = PromptManager(config=config)

# List all previously created prompts
prompt_manager.prompt_registry
# {'29661b2cc13e5acf9d7997d431b0c1a7': {
#    "id": "29661b2cc13e5acf9d7997d431b0c1a7",
#    "name": "personal_website_summary",
#    "prompt_str": "You are an analyst researching people who work in the field of Systems Change in order to write a summary of their work. Your data science team has scraped their personal websites and it is your job to summarize the text to give a good description the person and their work.\n\nThe text is scraped from websites, so please ignore junk or repetitive text.\nIf possible please focus on the person's work in the field of Systems Change.\nPlease take your time and ensure the information is accurate and well written.\nPlease do not include any references to the website or suggest visiting the website for more information.\nPlease only include the summary and nothing but the summary.\nPlease only return a single summary.\nPlease do not include copyright, legal text, or other citations such as address.\n\nYou will receive a set of webpage urls and the web text for a single person's website. Each set will be delineated by a line break and <code>---</code> characters.\n\n{INPUT TEXT}\n{SUMMARY}",
#    "namespace_name": "WEB_SUMMARIZER"
#  },
#  '56b0eff7c09f5bf4b148645ca8cebc54': {
#    "id": "56b0eff7c09f5bf4b148645ca8cebc54",
#    "name": "org_website_summary",
#    "prompt_str": "\nYou are an analyst researching organizations in order to write a summary of their work. Your data science team has scraped the websites of the organizations and it is your job to summarize the text to give a good description the organization.\n\nThe text is scraped from websites, so please ignore junk or repetitive text.\nPlease do not mention anything regarding donations or how to fund the organization.\nPlease take your time and ensure the information is accurate and well written.\nPlease do not include any references to the website or suggest visiting the website for more information.\nPlease only include the summary and nothing but the summary.\nPlease only return a single summary.\nPlease do not include copyright, legal text, or other citations such as address.\n\nYou will receive a set of webpage urls and the web text for a single organization. Each set will be delineated by a line break and <code>---</code> characters.\n\n{INPUT TEXT}\n{SUMMARY}\n",
#    "namespace_name": "WEB_SUMMARIZER"
#  }
# }


# Create and register a new prompt
prompt = Prompt(
    # Some text to describe the larger goal
    namespace_name="Adaptation vs Mitigation",
    # The name that describes what the prompt is trying to accomplish.
    name="Adaptation/Mitigation 4 Categories",
    prompt_str="You are a climate scientist who is an expert in all things climate. For the given block of text about a technology or project, please determine if this adaptation, mitigation, both, or neither.",
)

# See how a unique identifier was created
print(prompt.id)
# e3161295e5c05f12a0ccebf0edfba1ed

# Register and store the prompt - this will update the master file in S3
prompt_manager.register_prompt(prompt)

# Check that it's in the prompt registry
print(prompt_manager.prompt_registry[prompt.id])
# {
#   "id": "e3161295e5c05f12a0ccebf0edfba1ed",
#   "name": "Adaptation/Mitigation 4 Categories",
#   "prompt_str": "You are a climate scientist who is an expert in all things climate. For the given block of text about a technology or project, please determine if this adaptation, mitigation, both, or neither.",
#   "namespace_name": "Adaptation vs Mitigation"
# }
```

## Key Concepts
### Prompt
The Prompt class is a simple class that takes 3 initialization arguments:
* `name` - That is a way you want to describe what this prompt is doing that would discern it from other prompts.
* `namespace_name` - A more general way to describe the over-arching goal of the prompt. Think of this like a category for what the prompt is trying to accomplish. The `namespace_name` is also used for a hashing function when generating a unique id.
* `prompt_str` - The actual string that will be sent for the system prompt.

On instantiation, a deterministic unique id will be generated based on the `namespace_name` and `prompt_str`. 

**Note**
When creating a new prompt, all 3 of these fields are required. However, you can almost ignore creating and registering prompts if you use the [`GeneralPromptResponseCache`](../openai/prompt_response_cache.py) directly (or subclass it). Once you set the `prompt_str` when instantiating the `GeneralPromptResponseCache` object, the process of registering or loading a previously registering a `Prompt` is handled. See [README](../openai/README.md) for more details.

### PromptManager
The `PromptManager` is quite simple. It takes care of reading a file in S3 that maps all `ids` to their `Prompt` representation and writing that file back when a new prompt is registered.

As mentioned above, you rarely will need to interact with the `PromptManager` directly if you use the `GeneralPromptResponseCache`. However, it could be useful for understanding the text that was used when looking at previously cached results. As the `GeneralPromptResponseCache` will cache items with `<prompt.id>/<some_unique_id_for_text>`, you can look up the first part of that `id` in the `prompt_registry` to determine the exact prompt used.
