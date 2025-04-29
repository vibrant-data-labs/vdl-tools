# OpenAI
File directory for tools and utilities we built off of the OpenAI API

## Prompt Response Cache
The [GeneralPromptResponseCache](./prompt_response_cache.py) is a class that allows us to easily make calls to the OpenAI Chats API for a given prompt and will take car of the caching new results or loading pre-cached results. It is built off of the general [VDL Cache](../cache.py).

### Quick Start
```python
from shared_tools.openai.prompt_response_cache import GeneralPromptResponseCache

prompt_response_story = GeneralPromptResponseCache(
    prompt_str="You are a children's book author who specializes in telling stories about American history in age appropriate rap lyrics.",
    prompt_name="Lin Manuel Miranda Fake",
)

jefferson = prompt_response_story.get_cache_or_run(
    id="Thomas Jefferson",
    text="Tell me a story about Thomas Jefferson."
)

print(jefferson)
```
```
In [5]: print(_)
(Verse 1)
Listen up, kids, gather 'round, I'll tell you a tale
'Bout a man named Thomas Jefferson, he never did fail
He was the third president of the USA
And he had a lot of wisdom to share every day

(Chorus)
Thomas Jefferson, a founding father so wise
Helped shape our nation, reached for the skies
With his pen and his words, he fought for our rights
A true American hero, shining so bright
....
```

The next time you run this for the same ID, we will ignore the `text` and retrieve the result because it will have been cached.
```python
import time

s = time.time()
jefferson_cached = prompt_response_story.get_cache_or_run(
    id="Thomas Jefferson",
    text="Tell me a story about Hamilton."
)
e = time.time()

print((e - s))
print(jefferson_cached[:200])
```
```
2024-02-28 14:24:57,880 - INFO - {/Users/zeintawil/dev/vdl/vdl-tools/shared-tools/shared_tools/tools/log_utils.py:10} - Getting item Thomas Jefferson from file system
2024-02-28 14:24:57,881 - WARNING - {/Users/zeintawil/dev/vdl/vdl-tools/shared-tools/shared_tools/tools/log_utils.py:13} - Item d5142af77da652dcb33e1a3db09b36ba/Thomas Jefferson is taken from local cache
0.000881195068359375
(Verse 1)
Listen up, kids, gather 'round, I'll tell you a tale
'Bout a man named Thomas Jefferson, he never did fail
He was the third president of the USA
And he had a lot of wisdom to share every day
```

### Deeper Dive
We expose a number of parameters for instantiating the class, but the most import ones to know are the `prompt`, `prompt_str`, or `prompt_id`. These determine which `prompt_str` will be used for sending to the API as well as how we will be caching the results.

See [here](../prompt_management/README.md) for more details on `Prompt`s and `PromptManager`
You only need to send one of the three.
* `prompt` is an instantiation of the [`Prompt`](../prompt_management/prompt_manager.py) class.
* `prompt_str` is the exact string we will use for the system prompt. If this is used, then a `Prompt` object will be created using the `prompt_str`.
* `prompt_id` is the `id` from a previously created `Prompt`. If it is passed, the `Prompt` will be loaded via the `PromptManager`.

If the prompt used in the first two options hasn't been registered in the global `PromptRegistry`, we will store it so that we can have the original `prompt` that matches the `id` that will be cached with the result.

#### Caching
By default, the bucket that will be used for storing the cached results will be `'vdl-prompt-response-cache'`.
Should you wish, this can be changed with the init argument `bucket_name`, but if it is, you should document this in code as a subclassed Class (see the [WebsiteSummarizationCache](../web_summarization/website_summarization_cache.py )). This is because we want to know the bucket to look into for previous results the next time this is loaded.

The cache id will be `prompt.id/<id>` where `id` is the `id` value that is passed when calling `GeneralPromptResponseCache.get_cache_or_run`.


#### Sending Requests
The `get_cache_or_run` method first checks the cache for the result and if it's not in there, it will run a completions request to the OpenAI Chat Completions endpoint, using the [shared_tools.openai.openai_utils.get_completion](./openai_api_utils.py) function. You can pass various kwargs for controlling the hyperparameters of the completions API call.

**Warning** Results are only cached at the `hash(prompt.prompt_str)/id` granularity. Changing hyperparameters or input text will not recalculate results if the id is the same and previous results were cached with first being deleted.

## Assorted Tools

#### OpenAI API Utils
[shared_tools.openai.openai_api_utils.py](./openai_api_utils.py) contains a few useful utility functions.

`get_num_tokens` will return the number of tokens in a given piece of text using a specific model name.

```python
from shared_tools.openai.openai_api_utils import get_num_tokens

In [1]: get_num_tokens('the cat sat with the bat', 'gpt-3.5-turbo-0125')
Out[1]: 6
```

`get_completion` runs a call to the Chat Completions endpoint and handles formatting the messages with the prompts.

The argument `prompt` is the text prompt and will be the `system_prompt` for the chats.
Model should be the simplified model name (`gpt-3.5` or `gpt-4`). The function will look up the exact model name from `shared_tools.openai.openai_constants.MODEL_DATA`.

`get_completion` sets sane defaults for the hyperparameters and errs on the side of caution in terms of model creativity (`temperature=.2`). 

`return_all` returns the `ChatCompletion` object from OpenAI. Setting it to `False` will return the text.

```python
from shared_tools.openai.openai_api_utils import get_completion

rap = get_completion(
    prompt="You are a children's book author who specializes in telling stories about American history in age appropriate rap lyrics.",
    model="gpt-3.5",
    text="Tell me a story about Thomas Jefferson in the style of Lil Wayne",
    return_all=False,
)

print(rap)
```

```
In [24]: print(rap)
(Verse 1)
Yo, listen up kids, I'm here to drop some knowledge,
'Bout a man named Jefferson, he went to college.
He wrote the Declaration, with a pen in hand,
Fought for freedom, made a stand in this land.

(Chorus)
Thomas Jefferson, a founding father so wise,
Helped build this nation, reached for the skies.
His words still ring true, in the land of the free,
So let's give a shout out to T-Jeff, can't you see?

....
```

#### OpenAI API Constants
[shared_tools.openai.openai_constants.py](./openai_constants.py) contains common constants used when interacting with the OpenAI API. Most notably, we have `MODEL_DATA`. `MODEL_DATA` is a mapping of simple model names (`gpt-3.5`, `gpt-4`) to features of those models that are useful when using them like `max_context_window` for ensuring we don't send too much text and hit an error and `max_output_tokens` for ensuring we set an appropriate `max_tokens` value so we don't hit an error. It also includes pricing information if we want to track / estimate potential costs of running a model at scale.