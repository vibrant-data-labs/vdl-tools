import jsonlines
import pandas as pd
from vdl_tools.shared_tools.model_caches.climate_relevance_cache import (
    generate_climate_relevance_predictions,
    CB_CD_MODEL_4OMINI, 
)


def backfill_climate_relevance_predictions(
    df,
    num_records_per_source: int = 100,
    max_workers: int = 10,
    n_per_commit: int = 100,
):
    df = df[df['text_for_relevance_model'].notna()]

    if num_records_per_source:
        df = pd.concat([
            df[df['Data Source'] == "Crunchbase"].iloc[:num_records_per_source],
            df[df['Data Source'] == "Candid"].iloc[:num_records_per_source],
        ])
    predictions = generate_climate_relevance_predictions(
        df=df,
        column_text='text_for_relevance_model',
        idn='id',
        model=CB_CD_MODEL_4OMINI,
        max_workers=max_workers,
        n_per_commit=n_per_commit,
    )
    return predictions





