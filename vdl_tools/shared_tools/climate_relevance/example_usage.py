"""
Example usage of the ClimateRelevanceCache class.

This demonstrates how to use the new climate relevance classification
system with PostgreSQL caching.
"""

import pandas as pd
from vdl_tools.shared_tools.climate_relevance import ClimateRelevanceCache, generate_predictions
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.climate_relevance.climate_relevance_cache_psql import CB_CD_MODEL_4OMINI

def example_single_prediction(use_cached_result: bool = True):
    """Example of getting a single climate relevance prediction.
    
    Args:
        use_cached_result: Whether to use cached results.
    """

    # Sample organization description
    text = "Tesla designs and manufactures electric vehicles, energy storage systems, and solar panels."
    given_id = "tesla_example"

    with get_session() as session:
        # Initialize the cache with default settings
        cache = ClimateRelevanceCache(session=session)

        # Get prediction
        prediction, probability = cache.get_climate_relevance(
            given_id=given_id,
            text=text,
            use_cached_result=use_cached_result
        )

    print(f"Text: {text}")
    print(f"Prediction: {prediction} (1=climate relevant, 0=not relevant)")
    print(f"Probability: {probability}")


def example_bulk_predictions(use_cached_result: bool = True):
    """Example of getting bulk climate relevance predictions.

    Args:
        use_cached_result: Whether to use cached results.
    """

    # Sample data
    sample_data = [
        ("org_1", "Tesla designs and manufactures electric vehicles and clean energy solutions."),
        ("org_2", "McDonald's operates fast food restaurants worldwide."),
        ("org_3", "Sunrun provides residential solar panel installations and financing."),
        ("org_4", "Netflix streams movies and TV shows online."),
        ("org_5", "Vestas manufactures wind turbines for renewable energy generation."),
    ]

    with get_session() as session:
        # Initialize the cache with specific model
        cache = ClimateRelevanceCache(
            session=session,
            model=CB_CD_MODEL_4OMINI,
        )

        # Get bulk predictions
        results = cache.bulk_get_climate_relevance(
            given_ids_texts=sample_data,
            use_cached_result=use_cached_result,
            max_workers=3,
            n_per_commit=10
        )

    # Display results
    print("\nBulk Prediction Results:")
    print("-" * 60)
    for given_id, text in sample_data:
        prediction, probability = results.get(given_id, (None, None))
        relevance = "Climate Relevant" if prediction == 1 else "Not Climate Relevant" if prediction == 0 else "Error"
        print(f"ID: {given_id}")
        print(f"Text: {text[:50]}...")
        print(f"Result: {relevance} (confidence: {probability})")
        print("-" * 60)


def example_with_dataframe(use_cached_result: bool = True):
    """Example of processing a pandas DataFrame.
    
    Args:
        use_cached_result: Whether to use cached results.
    """

    # Create sample DataFrame (similar to original script)
    data = {
        'uuid': ['org_1', 'org_2', 'org_3', 'org_4', 'org_5'],
        'short_description': [
            "Tesla designs and manufactures electric vehicles and clean energy solutions.",
            "McDonald's operates fast food restaurants worldwide.",
            "Sunrun provides residential solar panel installations and financing.",
            "Netflix streams movies and TV shows online.",
            "Vestas manufactures wind turbines for renewable energy generation.",
        ]
    }
    df = pd.DataFrame(data)

    # Prepare data for bulk processing
    given_ids_texts = list(zip(df['uuid'], df['short_description']))    

    with get_session() as session:
        # Initialize cache
        cache = ClimateRelevanceCache(session=session)
        # Get predictions
        results = cache.bulk_get_climate_relevance(
            given_ids_texts,
            use_cached_result=use_cached_result,
            max_workers=3,
            n_per_commit=10
        )

    # Add results back to DataFrame
    df['prediction'] = df['uuid'].map(lambda x: results.get(x, (None, None))[0])
    df['probability'] = df['uuid'].map(lambda x: results.get(x, (None, None))[1])

    # Filter for climate relevant organizations
    climate_relevant = df[df['prediction'] == 1].copy()

    print("\nDataFrame Processing Results:")
    print("Climate Relevant Organizations:")
    print(climate_relevant[['uuid', 'short_description', 'probability']])

def example_generate_predictions():
    """Example using the new generate_predictions function - similar to the original script."""
    
    # Create sample DataFrame similar to original script
    data = {
        'uuid': ['org_1', 'org_2', 'org_3', 'org_4', 'org_5'],
        'short_description': [
            "Tesla designs and manufactures electric vehicles and clean energy solutions.",
            "McDonald's operates fast food restaurants worldwide serving burgers and fries.",
            "Sunrun provides residential solar panel installations and financing.",
            "Netflix streams movies and TV shows online to subscribers.",
            "Vestas manufactures wind turbines for renewable energy generation.",
        ]
    }
    df = pd.DataFrame(data)

    print("\nGenerate Predictions Function Example:")
    print("Original DataFrame:")
    print(df[['uuid', 'short_description']])
    
    # Method 1: Standalone function (like original script)
    predictions = generate_predictions(
        df=df,
        column_text='short_description',
        idn='uuid',
        model=CB_CD_MODEL_4OMINI,
        max_workers=3,
        use_cached_results=True
    )
    
    print("\nPredictions Dictionary (Standalone Function):")
    for uuid, (prediction, probability) in predictions.items():
        relevance = "Climate Relevant" if prediction == 1 else "Not Climate Relevant" if prediction == 0 else "Error"
        print(f"{uuid}: {relevance} (confidence: {probability})")


if __name__ == "__main__":
    print("=== Climate Relevance Cache Example ===\n")

    print("1. Single Prediction Example:")
    example_single_prediction()
    print("\n2. Bulk Predictions Example:")

    example_bulk_predictions()
    print("\n3. DataFrame Processing Example:")
    example_with_dataframe()
    
    print("\n4. Generate Predictions Example:")
    example_generate_predictions()
