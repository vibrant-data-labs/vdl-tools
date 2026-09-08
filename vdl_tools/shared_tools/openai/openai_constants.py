MODEL_DATA = {
    # GPT-5.6 family. Pricing note: prompts over 272K input tokens are billed
    # at 2x input / 1.5x output for the WHOLE request, and cache writes at
    # 1.25x the uncached input rate — neither is expressible in the flat
    # per-token fields below, so cost estimates for very long prompts will
    # read low. See https://developers.openai.com/api/docs/models/gpt-5.6-luna
    "gpt-5.6-sol": {
        "model_name": "gpt-5.6-sol",
        "max_context_window": 1_050_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 5.00 / 1_000_000,
        "output_cost_per_token": 30.00 / 1_000_000,
    },
    "gpt-5.6-terra": {
        "model_name": "gpt-5.6-terra",
        "max_context_window": 1_050_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 2.00 / 1_000_000,
        "output_cost_per_token": 12.00 / 1_000_000,
    },
    "gpt-5.6-luna": {
        "model_name": "gpt-5.6-luna",
        "max_context_window": 1_050_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 0.20 / 1_000_000,
        "output_cost_per_token": 1.20 / 1_000_000,
    },
    "gpt-5-mini": {
        "model_name": "gpt-5-mini",
        "max_context_window": 400_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 0.25 / 1_000_000,
        "output_cost_per_token": 2.0 / 1_000_000,
    },
    "gpt-5.5": {
        "model_name": "gpt-5.5",
        "max_context_window": 1_000_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 5.0 / 1_000_000,
        "output_cost_per_token": 30.0 / 1_000_000,
    },
    "gpt-5.4": {
        "model_name": "gpt-5.4",
        "max_context_window": 1_000_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 2.5 / 1_000_000,
        "output_cost_per_token": 15.0 / 1_000_000,
    },
    "gpt-5.4-mini": {
        "model_name": "gpt-5.4-mini",
        "max_context_window": 400_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 0.75 / 1_000_000,
        "output_cost_per_token": 4.5 / 1_000_000,
    },
    "gpt-5.4-nano": {
        "model_name": "gpt-5.4-nano",
        "max_context_window": 400_000,
        "max_output_tokens": 128_000,
        "input_cost_per_token": 0.20 / 1_000_000,
        "output_cost_per_token": 1.25 / 1_000_000,
    },
    "o3-mini":{
        "model_name": "o3-mini",
        "max_context_window": 1_047_576,
        "max_output_tokens": 32_768,
        "input_cost_per_token": 1.1 / 1_000_000,
        "output_cost_per_token": 4.4 / 1_000_000,
    },
    "gpt-4.1-mini":{
        "model_name": "gpt-4.1-mini",
        "max_context_window": 1_047_576,
        "max_output_tokens": 32_768,
        "input_cost_per_token": 0.4 / 1_000_000,
        "output_cost_per_token": 1.6 / 1_000_000,
    },

    "gpt-4.1":{
        "model_name": "gpt-4.1",
        "max_context_window": 1_047_576,
        "max_output_tokens": 32_768,
        "input_cost_per_token": 2.0 / 1_000_000,
        "output_cost_per_token": 8.0 / 1_000_000,
    },
    "gpt-4o":{
        "model_name": "gpt-4o-2024-08-06",
        "max_context_window": 128000,
        "max_output_tokens": 16384,
        "input_cost_per_token": 2.5 / 1_000_000,
        "output_cost_per_token": 10.0 / 1_000_000,
    },
    "gpt-4o-mini":{
        "model_name": "gpt-4o-mini-2024-07-18",
        "max_context_window": 128000,
        "max_output_tokens": 16384,
        "input_cost_per_token": 0.00015 / 1000,
        "output_cost_per_token": 0.0006 / 1000,
    },
    "gpt-4o-mini-products-ft":{
        "model_name": "ft:gpt-4o-mini-2024-07-18:vibrant-data-labs:financial-product-extraction-95:9yTaC31L",
        "max_context_window": 128000,
        "max_output_tokens": 8192,
        "input_cost_per_token": 0.00015 / 1000,
        "output_cost_per_token": 0.0006 / 1000,
    },
    "gpt-4": {
        "model_name": "gpt-4-0125-preview",
        "max_context_window": 128000,
        "max_output_tokens": 8192,
        "input_cost_per_token": 0.01 / 1000,
        "output_cost_per_token": 0.03 / 1000,
    },
    "gpt-3.5": {
        "model_name": "gpt-3.5-turbo-0125",
        "max_context_window": 16385,
        "max_output_tokens": 4096,
        "input_cost_per_token": 0.0005 / 1000,
        "output_cost_per_token": 0.0015 / 1000, },
}


SEED = 7118
