import jsonlines
import pandas as pd
from vdl_tools.shared_tools.model_caches.climate_relevance_cache import (
    generate_climate_relevance_predictions,
    CB_CD_MODEL_4OMINI, 
)


def backfill_climate_relevance_predictions(
    test_num_records: int = 100,
):
    df = pd.read_json('../climate-landscape/data/results/relevance_model_results.json')
    df = df.loc[:test_num_records]
    predictions = generate_climate_relevance_predictions(
        df=df,
        column_text='text_for_relevance_model',
        idn='id',
        model=CB_CD_MODEL_4OMINI,
    )
    return predictions





