"""
Example usage of the ClimateRelevanceCache class.

This demonstrates how to use the new climate relevance classification
system with PostgreSQL caching.
"""

import pandas as pd
from vdl_tools.shared_tools.model_caches import generate_climate_relevance_predictions, generate_predictions


CLIMATE_EXAMPLES = {
    'uuid': ['org_1', 'org_2', 'org_3', 'org_4', 'org_5'],
    'short_description': [
        "Tesla designs and manufactures electric vehicles and clean energy solutions.",
        "McDonald's operates fast food restaurants worldwide serving burgers and fries.",
        "Sunrun provides residential solar panel installations and financing.",
        "Netflix streams movies and TV shows online to subscribers.",
        "Vestas manufactures wind turbines for renewable energy generation.",
    ]
}

# def example_generate_health_predictions():

#     # Create sample DataFrame similar to original script
#     data = {
#         'uuid': ['org_1', 'org_2', 'org_3', 'org_4', 'org_5'],
#         'short_description': [
#             "Tesla designs and manufactures electric vehicles and clean energy solutions.",
#             "McDonald's operates fast food restaurants worldwide serving burgers and fries.",
#             "Sunrun provides residential solar panel installations and financing.",
#             "Netflix streams movies and TV shows online to subscribers.",
#             "Vestas manufactures wind turbines for renewable energy generation.",
#         ]
#     }
#     df = pd.DataFrame(data)

#     predictions = generate_predictions(
#         df=df,

#     )


def example_generate_climate_predictions(use_cached_results: bool = True):
    """Example using the new generate_predictions function - similar to the original script."""
    
    # Create sample DataFrame similar to original script
    df = pd.DataFrame(CLIMATE_EXAMPLES)

    print("\nGenerate Predictions Function Example:")
    print("Original DataFrame:")
    print(df[['uuid', 'short_description']])

    # Method 1: Standalone function (like original script)
    predictions = generate_climate_relevance_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        max_workers=3,
        use_cached_results=use_cached_results,
    )

    print("\nPredictions Dictionary (Standalone Function):")
    for _, row in predictions.iterrows():
        relevance = "Relevant" if row['prediction'] == 1 else "Not Relevant" if row['prediction'] == 0 else "Error"
        print(f"{row['uuid']}: {relevance} (probability: {row['probability']})")


def example_generate_climate_predictions_with_override_dict():
    """Example using the new generate_predictions function - similar to the original script."""

    # Create sample DataFrame similar to original script
    df = pd.DataFrame(CLIMATE_EXAMPLES)

    print("\nGenerate Predictions Function Example:")
    print("Original DataFrame:")
    print(df[['uuid', 'short_description']])

    # Method 1: Standalone function (like original script)
    predictions = generate_climate_relevance_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        max_workers=3,
        use_cached_results=True,
        label_override_dict={
            'org_1': 1,
            'org_2': 1,
            'org_3': 1,
            'org_4': 1,
            'org_5': 0,
        }
    )

    print("\nPredictions Dictionary (Standalone Function):")
    for _, row in predictions.iterrows():
        relevance = "Relevant" if row['prediction'] == 1 else "Not Relevant" if row['prediction'] == 0 else "Error"
        print(f"{row['uuid']}: {relevance} (probability: {row['probability']})")



if __name__ == "__main__":
    print("=== Climate Relevance Cache Example ===\n")

    example_generate_climate_predictions(use_cached_results=True)

    print("\n=== Climate Relevance Cache Example with no cached results ===\n")

    example_generate_climate_predictions(use_cached_results=False)

    print("\n=== Climate Relevance Cache Example with Override Dict ===\n")

    example_generate_climate_predictions_with_override_dict()
