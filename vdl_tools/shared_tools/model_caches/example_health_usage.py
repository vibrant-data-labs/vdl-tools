"""
Example usage of the health relevance classification system.

This demonstrates how to use the new health relevance classification
system with PostgreSQL caching for ARPA-H related projects.
"""

import pandas as pd
from vdl_tools.shared_tools.model_caches import generate_health_relevance_predictions


HEALTH_EXAMPLES = {
    'uuid': ['org_1', 'org_2', 'org_3', 'org_4', 'org_5'],
    'short_description': [
        "Biotech company developing novel cancer immunotherapies using CRISPR technology.",
        "McDonald's operates fast food restaurants worldwide serving burgers and fries.",
        "Medical device manufacturer creating next-generation surgical robots.",
        "Netflix streams movies and TV shows online to subscribers.",
        "Pharmaceutical company researching treatments for rare genetic diseases.",
    ]
}


def example_generate_health_predictions(use_cached_results: bool = True):
    """Example using the generate_health_relevance_predictions function."""
    
    # Create sample DataFrame
    df = pd.DataFrame(HEALTH_EXAMPLES)

    print("\nGenerate Health Relevance Predictions Example:")
    print("Original DataFrame:")
    print(df[['uuid', 'short_description']])

    # Generate predictions
    df_with_predictions = generate_health_relevance_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        max_workers=3,
        use_cached_results=use_cached_results,
    )

    print("\nPredictions:")
    for _, row in df_with_predictions.iterrows():
        relevance = "Health Relevant" if row['prediction'] == 1 else "Not Health Relevant" if row['prediction'] == 0 else "Error"
        print(f"{row['uuid']}: {relevance} (probability: {row['probability']})")


def example_generate_health_predictions_with_override_dict():
    """Example using label overrides for manual classification."""

    # Create sample DataFrame
    df = pd.DataFrame(HEALTH_EXAMPLES)

    print("\nGenerate Health Predictions with Override Dict Example:")
    print("Original DataFrame:")
    print(df[['uuid', 'short_description']])

    # Generate predictions with manual overrides
    df_with_predictions = generate_health_relevance_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        max_workers=3,
        use_cached_results=True,
        label_override_dict={
            'org_1': 1,  # Force biotech to be relevant
            'org_2': 0,  # Force McDonald's to be not relevant
            'org_3': 1,  # Force medical device to be relevant
            'org_4': 0,  # Force Netflix to be not relevant
            'org_5': 1,  # Force pharma to be relevant
        }
    )

    print("\nPredictions with Overrides:")
    for _, row in df_with_predictions.iterrows():
        relevance = "Health Relevant" if row['prediction'] == 1 else "Not Health Relevant" if row['prediction'] == 0 else "Error"
        print(f"{row['uuid']}: {relevance} (probability: {row['probability']})")


def example_with_custom_model():
    """Example using a specific ARPA-H model."""
    from vdl_tools.shared_tools.model_caches.health_relevance_cache import ARPAH_MODEL
    
    df = pd.DataFrame(HEALTH_EXAMPLES)

    print("\nExample with ARPAH_MODEL (less conservative):")
    
    df_with_predictions = generate_health_relevance_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        model=ARPAH_MODEL,  # Using the less conservative model
        max_workers=3,
        use_cached_results=True,
    )

    print("\nPredictions:")
    for _, row in df_with_predictions.iterrows():
        relevance = "Health Relevant" if row['prediction'] == 1 else "Not Health Relevant" if row['prediction'] == 0 else "Error"
        print(f"{row['uuid']}: {relevance} (probability: {row['probability']})")


if __name__ == "__main__":
    print("=== Health Relevance Cache Examples ===\n")

    print("Example 1: Basic Usage with Caching")
    example_generate_health_predictions(use_cached_results=True)

    print("\n" + "="*50)
    print("\nExample 2: Without Using Cache (Force Re-run)")
    example_generate_health_predictions(use_cached_results=False)

    print("\n" + "="*50)
    print("\nExample 3: With Manual Override Dict")
    example_generate_health_predictions_with_override_dict()

    print("\n" + "="*50)
    print("\nExample 4: Using Different Model")
    example_with_custom_model()

