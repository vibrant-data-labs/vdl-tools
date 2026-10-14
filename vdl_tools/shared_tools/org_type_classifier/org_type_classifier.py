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
from vdl_tools.shared_tools.cb_funding_calculations import (
    FOR_PROFIT_ROUND_TYPES,
    _raw_stage,
    _raw_types,
    raised_from_venture_rounds,
)

# Suppress only SettingWithCopyWarning
warnings.filterwarnings('ignore')

MODEL_VERSION = '2025_03_28.0'
MODEL_NAME = 'org_type_classifier'

TRAINING_DATA_PATH = '../climate-landscape/data/results/cb_cd_li_meta.json'
TRAINING_LABELS_PATH = '../shared-data-clean/data/training_labels/2025_03_26_org_type_labels.json'


def model_paths(model_version=MODEL_VERSION):
    """Local path and S3 key of one saved version of the model."""
    filename = f'{MODEL_NAME}_{model_version}.joblib'
    return s3_model.wd / 'models' / MODEL_NAME / filename, f'{MODEL_NAME}/{filename}'


LABEL_MAP = {
    0: 'For Profit',
    1: 'Non Profit',
}

INVERSE_LABEL_MAP = {v: k for k, v in LABEL_MAP.items()}

# LinkedIn renamed this industry in its 2022 taxonomy; profiles carry either name.
LINKEDIN_NONPROFIT_INDUSTRIES = {'Non-profit Organizations', 'Non-profit Organization Management'}


def load_model(model_version=MODEL_VERSION):
    full_model_path, model_key = model_paths(model_version)
    if not full_model_path.exists():
        full_model_path.parent.mkdir(parents=True, exist_ok=True)
        s3_model.s3_file(
            key=model_key,
            filename=full_model_path
        )
    return joblib.load(full_model_path)


class CategoryEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, text_pipeline):
        self.text_pipeline = text_pipeline

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        model_predictions = self.text_pipeline.predict(X['text'])
        X.loc[:, 'OrgType_Text_Prediction'] = model_predictions

        X.loc[:, 'industry_li_parsed'] = X["industry_li"].apply(lambda x: x[0] if len(x) > 0 else None)
        X.loc[:, 'Is LinkedIn NP'] = X["industry_li_parsed"].apply(lambda x: x in LINKEDIN_NONPROFIT_INDUSTRIES)

        X.loc[:, 'Non-Profit in CB Sectors'] = X["sectors_cb_cd"].apply(lambda x: 1 if x and  "Non Profit" in x else 0)

        X.loc[:, 'Is Non-Profit Org Type'] = X['Org Type'].apply(lambda x: x == 'Non Profit')

        categorical_features = [
            'Is Non-Profit Org Type',
            'Is LinkedIn NP',
            'Non-Profit in CB Sectors',
            'OrgType_Text_Prediction',
        ]

        return X[categorical_features]


def train_model(
    model_version,
    training_data_path=TRAINING_DATA_PATH,
    training_labels_path=TRAINING_LABELS_PATH,
    extra_training_data_path=None,
):
    # No default version, so a retrain can't overwrite the model other projects load
    df = pd.read_json(training_data_path)
    labels = json.load(open(training_labels_path))
    df['Label'] = df['id'].apply(lambda x: INVERSE_LABEL_MAP.get(labels.get(x)))

    # Hand-verified rows that carry their own Label, e.g. nonprofits that Crunchbase
    # calls For Profit; their label replaces any for the same id in the main data
    extra = pd.DataFrame()
    if extra_training_data_path:
        extra = pd.read_json(extra_training_data_path)
        unknown = extra.loc[~extra['Label'].isin(INVERSE_LABEL_MAP), 'Label']
        if len(unknown):
            raise ValueError(
                f"{extra_training_data_path}: {len(unknown)} row(s) with a Label other than "
                f"{list(INVERSE_LABEL_MAP)}: {sorted(unknown.astype(str).unique())}"
            )
        extra['Label'] = extra['Label'].map(INVERSE_LABEL_MAP)
        df = df[~df['id'].isin(extra['id'])]

    df = df[df['Label'].notnull()]

    df_cb = df[(df['Data Source'] == 'Crunchbase')].copy()

    non_profit_cb = df_cb[df_cb['Label'] == INVERSE_LABEL_MAP.get('Non Profit')].copy()

    # Downn sample the for profit to more closely match the number of non profit
    # But don't over downsample so that we don't have too many false false positives
    for_profit_cb = df_cb[df_cb['Label'] == INVERSE_LABEL_MAP.get('For Profit')].sample(frac=.8, random_state=42)

    # Take some candid but not too many so that we don't have too many false positives
    df_candid = df[(df['Data Source'] == 'Candid')].sample(n=round(len(non_profit_cb) * .1), random_state=42)

    training_data = pd.concat(
        [non_profit_cb, for_profit_cb, df_candid],
        axis=0,
        ignore_index=True
    )

    # The extra rows train the text model only. They are all orgs whose upstream
    # Org Type is wrong, so in the category model they would teach it to distrust
    # Org Type for every org, not just for these.
    text_training_data = pd.concat([training_data, extra], axis=0, ignore_index=True)

    vectorizer = TfidfVectorizer(ngram_range=(1, 3), stop_words='english')

    train, test = train_test_split(
        text_training_data,
        test_size=0.2,
        random_state=42,
        stratify=text_training_data['Label']
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
    full_model_path, model_key = model_paths(model_version)
    full_model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        full_pipeline,
        full_model_path
    )

    # Save the model to S3
    s3_model.put_file(
        key=model_key,
        filename=full_model_path
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
    venture_override_skips_series_unknown=False,
    model_version=MODEL_VERSION,
):
    model = load_model(model_version)

    df_cb = df[(df['Data Source'] == 'Crunchbase')]
    df_cd = df[(df['Data Source'] == 'Candid')]
    df_gt = df[(df['Data Source'] == 'Giving Tuesday')]

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

    # If the company raised from a venture round, assume it's a for profit.
    # By this point 'Funding Types' / 'Funding Stage' hold DISPLAY names;
    # raised_from_venture_rounds normalizes them via cb_funding_types.as_raw, so
    # this override works on either vocabulary (it used to silently never fire).
    # With venture_override_skips_series_unknown, an org whose only for-profit-style
    # round is series_unknown keeps the model's prediction: Crunchbase also files
    # nonprofit grants under that type, and often retypes them to grant later.
    def venture_override(x):
        if venture_override_skips_series_unknown and (
            _raw_types(x, funding_types_field) & FOR_PROFIT_ROUND_TYPES == {'series_unknown'}
            and _raw_stage(x, funding_stage_field) != 'ipo'
        ):
            return False
        return raised_from_venture_rounds(
            x,
            funding_types_field=funding_types_field,
            funding_stage_field=funding_stage_field
        )

    prediction_df[org_type_prediction_field] = prediction_df.apply(
        lambda x: INVERSE_LABEL_MAP['For Profit']
          if venture_override(x)
          else x[org_type_prediction_field],
        axis=1
    )

    # Map the prediction to the label
    prediction_df[org_type_prediction_field] = prediction_df[org_type_prediction_field].map(LABEL_MAP)
    id_to_prediction = dict(zip(prediction_df[id_field], prediction_df[org_type_prediction_field]))
    df_cb[org_type_prediction_field] = df_cb[id_field].map(id_to_prediction)

    df_cd[org_type_prediction_field] = df_cd[org_type_field]
    df_gt[org_type_prediction_field] = df_gt[org_type_field]
    df = pd.concat([df_cb, df_cd, df_gt])
    return df


if __name__ == "__main__":
    df = pd.read_json(TRAINING_DATA_PATH)
    df = predict(df)
