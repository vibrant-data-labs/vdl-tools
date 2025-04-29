import json
import joblib
import warnings
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.ensemble import AdaBoostClassifier
from sklearn.base import BaseEstimator, TransformerMixin

from vdl_tools.shared_tools import s3_model
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.cb_funding_calculations import raised_from_venture_rounds

# Suppress only SettingWithCopyWarning
warnings.filterwarnings('ignore')

MODEL_VERSION = '2025_03_28.0'
MODEL_NAME = 'org_type_classifier'
FULL_MODEL_FILENAME = f'{MODEL_NAME}_{MODEL_VERSION}.joblib'

FULL_MODEL_PATH = s3_model.wd / 'models' / MODEL_NAME / FULL_MODEL_FILENAME
MODEL_KEY = f'{MODEL_NAME}/{FULL_MODEL_FILENAME}'


LABEL_MAP = {
    0: 'For Profit',
    1: 'Non Profit',
}

INVERSE_LABEL_MAP = {v: k for k, v in LABEL_MAP.items()}


def load_model():
    if not FULL_MODEL_PATH.exists():
        FULL_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        s3_model.s3_file(
            key=MODEL_KEY,
            filename=FULL_MODEL_PATH
        )
    return joblib.load(FULL_MODEL_PATH)


class CategoryEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, text_pipeline):
        self.text_pipeline = text_pipeline

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        model_predictions = self.text_pipeline.predict(X['text'])
        X.loc[:, 'OrgType_Text_Prediction'] = model_predictions

        X.loc[:, 'industry_li_parsed'] = X["industry_li"].apply(lambda x: x[0] if len(x) > 0 else None)
        X.loc[:, 'Is LinkedIn NP'] = X["industry_li"].apply(lambda x: x == "Non-profit Organizations")

        X.loc[:, 'Non-Profit in CB Sectors'] = X["sectors_cb_cd"].apply(lambda x: 1 if x and  "Non Profit" in x else 0)

        X.loc[:, 'Is Non-Profit Org Type'] = X['Org Type'].apply(lambda x: x == 'Non Profit')

        categorical_features = [
            'Is Non-Profit Org Type',
            'Is LinkedIn NP',
            'Non-Profit in CB Sectors',
            'OrgType_Text_Prediction',
        ]

        return X[categorical_features]


def train_model():
    df = pd.read_json('../climate-landscape/data/results/cb_cd_li_meta.json')
    labels = json.load(open('../shared-data/data/training_labels/2025_03_26_org_type_labels.json'))
    df['Label'] = df['id'].apply(lambda x: INVERSE_LABEL_MAP.get(labels.get(x)))

    df = df[df['Label'].notnull()]

    df_cb = df[(df['Data Source'] == 'Crunchbase')].copy()

    non_profit_cb = df_cb[df_cb['Label'] == INVERSE_LABEL_MAP.get('Non Profit')].copy()

    # Downn sample the for profit to more closely match the number of non profit
    # But don't over downsample so that we don't have too many false false positives
    for_profit_cb = df_cb[df_cb['Label'] == INVERSE_LABEL_MAP.get('For Profit')].sample(frac=.8)

    # Take some candid but not too many so that we don't have too many false positives
    df_candid = df[(df['Data Source'] == 'Candid')].sample(n=round(len(non_profit_cb) * .1))

    training_data = pd.concat(
        [non_profit_cb, for_profit_cb, df_candid],
        axis=0,
        ignore_index=True
    )

    vectorizer = TfidfVectorizer(ngram_range=(1, 3), stop_words='english')

    train, test = train_test_split(
        training_data,
        test_size=0.2,
        random_state=42,
        stratify=training_data['Label']
    )

    X_train = vectorizer.fit_transform(train['text'])
    y_train = train['Label']

    X_test = vectorizer.transform(test['text'])
    y_test = test['Label']

    adaboost_text_model = AdaBoostClassifier(n_estimators=15, random_state=42,)
    adaboost_text_model.fit(X_train, y_train)
    y_pred_adaboost = adaboost_text_model.predict(X_test)
    logger.info(
        "\nText Model:\nPrecision: %s, Recall: %s, F1: %s, Accuracy: %s",
        precision_score(y_test, y_pred_adaboost, average='macro'),
        recall_score(y_test, y_pred_adaboost, average='macro'),
        f1_score(y_test, y_pred_adaboost, average='macro'),
        accuracy_score(y_test, y_pred_adaboost)
    )

    text_pipeline = Pipeline([
        ('vectorizer', vectorizer),
        ('adaboost', adaboost_text_model)
    ])

    category_encoder = CategoryEncoder(text_pipeline)
    X_category = category_encoder.transform(training_data)
    y_category = training_data['Label']

    X_category_train, X_category_test, y_category_train, y_category_test = train_test_split(
        X_category,
        y_category,
        test_size=0.2,
        random_state=42,
        stratify=y_category
    )

    adaboost_category_model = AdaBoostClassifier(n_estimators=5, random_state=42)
    adaboost_category_model.fit(X_category_train, y_category_train)
    y_pred_adaboost_category = adaboost_category_model.predict(X_category_test)

    logger.info(
        "\nCategory Model:\nPrecision: %s, Recall: %s, F1: %s, Accuracy: %s",
        precision_score(y_category_test, y_pred_adaboost_category, average='macro'),
        recall_score(y_category_test, y_pred_adaboost_category, average='macro'),
        f1_score(y_category_test, y_pred_adaboost_category, average='macro'),
        accuracy_score(y_category_test, y_pred_adaboost_category)
    )

    full_pipeline = Pipeline([
        ('category_encoder', CategoryEncoder(text_pipeline)),
        ('adaboost_category', adaboost_category_model),
    ])

    # Save the model to the local directory
    FULL_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        full_pipeline,
        FULL_MODEL_PATH
    )

    # Save the model to S3
    s3_model.put_file(
        key=MODEL_KEY,
        filename=FULL_MODEL_PATH
    )
    return full_pipeline


def predict(
    df,
    text_field='text',
    id_field='id',
    org_type_field='Org Type',
    data_source_field='Data Source',
    org_type_prediction_field='OrgType Prediction',
    linkedin_industry_field='industry_li',
    sectors_cb_cd_field='sectors_cb_cd',
    funding_stage_field='Funding Stage',
    funding_types_field='Funding Types',
):
    model = load_model()

    df_cb = df[(df['Data Source'] == 'Crunchbase')]
    df_cd = df[(df['Data Source'] == 'Candid')]

    # Only predict for Crunchbase for now
    prediction_df = df_cb.copy()

    # Rename the columns to match the model's expected input
    prediction_df.rename(
        columns={
            text_field: 'text',
            id_field: 'id',
            org_type_field: 'Org Type',
            data_source_field: 'Data Source',
            linkedin_industry_field: 'industry_li',
            sectors_cb_cd_field: 'sectors_cb_cd',
        },
        inplace=True
    )

    prediction_df[org_type_prediction_field] = model.predict(prediction_df)

    # If the text prediction is non profit, use that,
    # otherwise use the category prediction
    prediction_df[org_type_prediction_field] = prediction_df.apply(
        lambda x: INVERSE_LABEL_MAP['Non Profit']
          if x['OrgType_Text_Prediction'] == INVERSE_LABEL_MAP['Non Profit']
          else x[org_type_prediction_field],
        axis=1
    )

    # If the company raised from a venture round, assume it's a for profit
    prediction_df[org_type_prediction_field] = prediction_df.apply(
        lambda x: INVERSE_LABEL_MAP['For Profit']
          if raised_from_venture_rounds(
              x,
              funding_types_field=funding_types_field,
              funding_stage_field=funding_stage_field
            )
          else x[org_type_prediction_field],
        axis=1
    )

    # Map the prediction to the label
    prediction_df[org_type_prediction_field] = prediction_df[org_type_prediction_field].map(LABEL_MAP)
    id_to_prediction = dict(zip(prediction_df[id_field], prediction_df[org_type_prediction_field]))
    df_cb[org_type_prediction_field] = df_cb[id_field].map(id_to_prediction)

    df_cd[org_type_prediction_field] = df_cd[org_type_field]
    df = pd.concat([df_cb, df_cd])
    return df


if __name__ == "__main__":
    df = pd.read_json('../climate-landscape/data/results/cb_cd_li_meta.json')
    df = predict(df)
