import os
import numpy as np
import pandas as pd

import socket; socket.setdefaulttimeout(30)

# from common: commonly used functions
from vdl_tools.linkedin import org_loader as li
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id
from vdl_tools.scrape_enrich.combine_crunchbase_candid_linkedin import combine_cb_cd_li
from vdl_tools.scrape_enrich.scraper.scrape_websites import extract_website_name, scrape_websites_psql
from vdl_tools.shared_tools import climatebert_adaptation as adp
from vdl_tools.shared_tools.all_source_organization_summarization import generate_summary_of_summaries, BASE_SUMMARY_OF_SUMMARIES_PROMPT
from vdl_tools.shared_tools.climate_landscape.diversity_keywords import DIVERSITY_BIPOC_DICT
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.geotagging_prompting import geotag_texts_bulk
from vdl_tools.shared_tools.org_type_classifier import org_type_classifier
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.web_summarization.website_summarization_cache_psql import (
    GENERIC_ORG_WEBSITE_PROMPT_TEXT,
)
from vdl_tools.shared_tools.web_summarization.website_summarization_psql import summarize_scraped_df

from vdl_tools.shared_tools.model_caches.climate_relevance_cache import generate_climate_relevance_predictions
import vdl_tools.scrape_enrich.geocode_v2 as geocode
import vdl_tools.scrape_enrich.process_images as images
import vdl_tools.scrape_enrich.tags_from_text as tft
import vdl_tools.shared_tools.common_functions as cf
from vdl_tools.shared_tools.model_caches.climate_relevance_cache import (
    CB_CD_MODEL_4OMINI,
    DEFAULT_CLIMATE_SYSTEM_PROMPT,
    DEFAULT_CLIMATE_PROMPT_FORMAT,
)


GLOBAL_CONFIG = get_configuration()


MAX_WORKERS = 200
N_PER_COMMIT = 200 # number of records to commit to the database at a time
MAX_SCRAPE_WEBSITES_WORKERS = int(os.environ.get("MAX_SCRAPE_WEBSITES_WORKERS", 5))
MIN_DESCRIPTION_LENGTH = 100
TEXT_FIELDS = ["Description", "Description_990", "Website Summary", "About LinkedIn"]


def get_website_summaries(
    df,
    website_column_scrape='Website_cb_cd',
    canonical_website_column='Website',
    linkedin_url_column='LinkedIn',
    skip_existing=True,
    summary_prompt=GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    use_combined=True,
    max_workers=MAX_WORKERS,
    n_per_commit=N_PER_COMMIT,
    max_errors=1,
    max_scrape_websites_workers=MAX_SCRAPE_WEBSITES_WORKERS,
):
    df_web, df = get_scraped_df(
        df,
        website_column_scrape=website_column_scrape,
        canonical_website_column=canonical_website_column,
        linkedin_url_column=linkedin_url_column,
        skip_existing=skip_existing,
        summary_prompt=summary_prompt,
        return_combined_res=use_combined,
        max_workers=max_scrape_websites_workers,
        max_errors=max_errors,
    )

    # Filter out short texts
    summaries = summarize_scraped_df(
        df_web,
        prompt_str=summary_prompt,
        is_combined=use_combined,
        skip_existing=skip_existing,
        max_workers=max_workers,
        max_errors=max_errors,
        n_per_commit=n_per_commit,
    )

    summaries = {k.rstrip("/"): v for k, v in summaries.items() if v}
    df[website_column_scrape] = df[website_column_scrape].apply(lambda x: x.rstrip("/") if coerced_bool(x) else x)
    df[canonical_website_column] = df[canonical_website_column].apply(lambda x: x.rstrip("/") if coerced_bool(x) else x)

    df['extracted_website_key'] = df[canonical_website_column].apply(
        lambda x: extract_website_name(x)
        if coerced_bool(x) else None
    )
    summaries = {extract_website_name(k): v for k, v in summaries.items()}
    df['Website Summary'] = df['extracted_website_key'].map(summaries, None)

    return summaries, df


def choose_longer_text(
    row,
    summary_column='Summary',
    website_summary_column='Website Summary',
):
    website_summary = row[website_summary_column]
    if pd.isnull(website_summary):
        website_summary = ""
    general_summary = row[summary_column]
    if pd.isnull(general_summary):
        general_summary = ""
    if len(general_summary) >= len(website_summary):
        return general_summary
    return website_summary


def get_scraped_df(
    df,
    website_column_scrape='Website_cb_cd',
    canonical_website_column='Website',
    linkedin_url_column='LinkedIn',
    skip_existing=True,
    summary_prompt=GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    return_combined_res=True,
    max_workers=MAX_WORKERS,
    n_per_commit=N_PER_COMMIT,
    max_errors=1,
    subpage_type='about',
):

    if linkedin_url_column not in df.columns:
        df[linkedin_url_column] = None

    if canonical_website_column not in df.columns:
        df[canonical_website_column] = df[website_column_scrape]

    has_url_mask = df[website_column_scrape].notnull()

    with get_session() as session:
        df_web = scrape_websites_psql(
            # filter out null websites,
            urls=df[has_url_mask][website_column_scrape],
            session=session,
            skip_existing=skip_existing,
            subpage_type=subpage_type,
            n_per_commit=n_per_commit,
            max_errors=max_errors,
            summary_prompt=summary_prompt,
            return_combined_res=return_combined_res,
            max_workers=max_workers,
            verify_ssl=True,
        )


    if return_combined_res:
        web_success_mask = df_web['combined_text'].notnull()
        df_web_success = df_web[web_success_mask].copy()
        failed_urls = df_web[~web_success_mask]['home_url'].unique()

    else:
        web_success_mask = df_web['response_status_code'] == 200
        df_web_success = df_web[web_success_mask].copy()

        failed_urls = df_web[~web_success_mask & df_web['type'] == 'PageType.INDEX']['source'].unique()

    has_linkedin_mask = df[linkedin_url_column].notnull()
    failed_urls_linkedin_urls = df[df[website_column_scrape].isin(failed_urls) & has_linkedin_mask][linkedin_url_column].tolist()

    no_web_url_has_linkedin = df[~has_url_mask & has_linkedin_mask][linkedin_url_column].tolist()
    linkedin_urls = no_web_url_has_linkedin + failed_urls_linkedin_urls

    # Scrape LinkedIn for those that didn't have a url or their LinkedIn url didn't work
    if linkedin_urls:
        with get_session(GLOBAL_CONFIG) as session:
            df_linkedin = li.scrape_organizations_psql(
                linkedin_urls,
                session=session,
                api_key=GLOBAL_CONFIG['linkedin']['coresignal_api_key'],
                max_errors=max_errors,
            )

        original_li_id_to_website_url = {
            extract_linkedin_id(x['original_id']): x['website']
            for _, x in df_linkedin.iterrows()
            if x['website']
        }

        # Guard against the case where the LinkedIn url from original and from coresignal differ by
        # http vs https or or trailing slash
        df['extracted_linkedin_id'] = df[linkedin_url_column].apply(
            lambda x: extract_linkedin_id(x) if coerced_bool(x) else None
        )
        df[canonical_website_column] = df.apply(
            lambda x: original_li_id_to_website_url.get(
                x['extracted_linkedin_id'], x[canonical_website_column]
            ),
            axis=1,
        )

        # Now scrape them based on the websites we just got
        df_web_for_scraping = df[df[linkedin_url_column].isin(linkedin_urls) & df[canonical_website_column].notnull()]
        with get_session() as session:
            df_web_linkedin = scrape_websites_psql(
                # filter out null websites,
                urls=df_web_for_scraping[canonical_website_column],
                session=session,
                skip_existing=skip_existing,
                subpage_type="about",
                n_per_commit=n_per_commit,
                max_errors=max_errors,
                summary_prompt=summary_prompt,
                return_combined_res=return_combined_res,
                max_workers=max_workers,
            )

        if df_web_for_scraping.shape[0] > 0:
            if return_combined_res:
                web_success_mask = df_web_linkedin['combined_text'].notnull()
            else:
                web_success_mask = df_web_linkedin['response_status_code'] == 200
            df_web_linkedin_success = df_web_linkedin[web_success_mask]

        df_web_success = pd.concat([df_web_success, df_web_linkedin_success])

    return df_web_success, df


def _missing_description_mask(df, min_description_length=MIN_DESCRIPTION_LENGTH, description_col='Description'):
    """returns a filtering mask when the description is missing or too short"""
    return (
            (df[description_col].isnull()) |
            (df[description_col].str.len() < min_description_length)
    )


def prepare_for_relevance_model(
    df,
    max_workers=MAX_WORKERS,
    n_per_commit=N_PER_COMMIT,
    description_col='Description'
):
    """The relevance models require descriptive text.
    CB and Candid descriptions have shown to be enough text for the models to work.
    However, sometimes we are missing the descriptions from the original source.

    In this case, we will use summaries from their scraped websites.

    If the website urls are are missing, we will scrape LinkedIn for their website urls and descriptions.
    Then we
    """
    # Determine which organizations have missing descriptions or their descriptions are too short
    # Note: Assuming _missing_description_mask handles the column selection or defaults to 'Description' internally.
    # If it is hardcoded to 'Description', you might want to update it to use description_col as well.
    missing_descriptions_mask = _missing_description_mask(
        df,
        min_description_length=MIN_DESCRIPTION_LENGTH,
        description_col=description_col
    )

    # Scrape websites for those missing descriptions
    df_missing_descriptions = df[missing_descriptions_mask].copy()

    if df_missing_descriptions.shape[0] > 0:
        # Add the website summaries but only for those missing descriptions
        summaries_for_missing_desc, df_missing_descriptions = get_website_summaries(
            df_missing_descriptions,
            skip_existing=True,
            max_workers=max_workers,
            n_per_commit=n_per_commit,
        )
    else:
        summaries_for_missing_desc = {}
        df_missing_descriptions = pd.DataFrame()

    # UPDATED: Uses the custom column if provided, defaults to 'Description'
    df['text_for_relevance_model'] = df[description_col]

    if df_missing_descriptions.shape[0] > 0:
        df['Website Summary'] = df_missing_descriptions['Website Summary']
        # For orgs with a short/missing description, prefer the website summary when available
        has_website_summary = df['Website Summary'].notnull()
        df.loc[has_website_summary, 'text_for_relevance_model'] = df.loc[has_website_summary, 'Website Summary']
    else:
        df['Website Summary'] = None

    # TO DO: Fix what happens when there is no website text
    # LinkedIn text? 990?

    logger.info(f"Dropping organizations with missing descriptions: {df['text_for_relevance_model'].isnull().sum()}")
    df = df[df['text_for_relevance_model'].notnull()].copy()
    return df


def run_relevance_model(
    df,
    model_name,
    idn,
    column_text='text_for_relevance_model',
    label_override_filepath=None,
    use_cached_results=True,
    system_prompt=None,
    prompt_format=None,
    prompt_name="climate_relevance_classification",
    n_per_commit=N_PER_COMMIT,
    max_workers=MAX_WORKERS,
):
    model_name_safe = model_name.replace("-", "_").replace(":", "_")

    if label_override_filepath:
        label_override_filepath = str(label_override_filepath).replace("MODEL_NAME_HOLDER", model_name_safe)

    df_with_predictions = generate_climate_relevance_predictions(
        df=df,
        column_text=column_text,
        idn=idn,
        model=model_name,
        use_cached_results=use_cached_results,
        system_prompt=system_prompt,
        prompt_format=prompt_format,
        label_override_filepath=label_override_filepath,
        prompt_name=prompt_name,
        n_per_commit=n_per_commit,
        max_workers=max_workers,
    )

    df_with_predictions.rename(columns={"prediction": "prediction_relevant", "probability": "probability_relevant"}, inplace=True)

    good_predictions_mask = df_with_predictions["prediction_relevant"].isin([0, 1])
    errors = df_with_predictions[~good_predictions_mask].copy()
    df_with_predictions = df_with_predictions[good_predictions_mask].copy()
    return df_with_predictions, errors


def add_geotags(
    df,
    text_fields=TEXT_FIELDS,
    max_workers=MAX_WORKERS,
    id_col='id',
    max_errors=2,
):
    ids_texts = []
    for _, row in df[[id_col] + text_fields].iterrows():
        text = "\n".join([row[col] for col in text_fields if row[col] and isinstance(row[col], str)])
        ids_texts.append((row[id_col], text))

    with get_session(GLOBAL_CONFIG) as session:
        ids_to_geotags = geotag_texts_bulk(
            ids_texts=ids_texts,
            session=session,
            use_cached_result=True,
            max_workers=max_workers,
            max_errors=max_errors,
        )

    df['Geo_Tags_Dicts'] = df[id_col].map(ids_to_geotags)
    df['Geo_Tags'] = df['Geo_Tags_Dicts'].apply(
        lambda geo_dicts_list: [v for geo_dict in geo_dicts_list for v in geo_dict.values() if v]
        if isinstance(geo_dicts_list, list) else []
    )
    return df


def add_climate_keywords(
    df,
    keyword_path,
    id_col='id',
    df_climate_kwds=None,
    text_fields=TEXT_FIELDS,
):
    # load kwd mapping table (here it is master_term: [list of search terms])
    # terms used in database search
    if df_climate_kwds is None:
        df_climate_kwds = pd.read_excel(keyword_path)
        cf.string2list(
            df_climate_kwds,
            ['broad_tags', 'bigram', 'unigram', 'search_terms']  # format as lists
        )

    df = tft.add_kwd_tags(
        df,
        df_climate_kwds,  # kwd mapping corpus grouped by kwd tag
        kwds='climate_kwds',  # column to store tags
        idCol=id_col,  # unique id column
        textcols=text_fields,  # text columns to search
        format_tagmap=True,  # explode search terms
        master_term='tag',  # name col with master term
        search_terms='search_terms',  # name of col with list of search terms
        add_related='broad_tags',  # name of col with manual list of add_related
        add_unigrams=True,  # add unigrams within 2 or more grams.
        add_bigrams=True,  # add bigrams within 3 or more grams
    )
    return df


def add_summary_of_summaries(
    df,
    text_fields=TEXT_FIELDS,
    id_col='id',
    use_cached_results=True,
    max_workers=MAX_WORKERS,
    summary_prompt=BASE_SUMMARY_OF_SUMMARIES_PROMPT,
    summary_column='Summary',
    n_per_commit=N_PER_COMMIT,
):

    # Remove all the rows that are missing all the text fields
    contains_at_least_one_text = (
        np.logical_or
        .reduce(
            [df[field].apply(coerced_bool) for field in text_fields]
        )
    )
    if not contains_at_least_one_text.all():
        logger.info(
            "Dropping %s rows that are missing all text fields for summarization",
            (~contains_at_least_one_text).sum()
        )
    df_for_summary = df[contains_at_least_one_text].copy()

    df_for_summary.fillna({col: "" for col in text_fields}, inplace=True)

    ids_text_lists = zip(
        df_for_summary[id_col].values,
        df_for_summary[text_fields].values.tolist()
    )

    ids_to_summaries = generate_summary_of_summaries(
        ids_text_lists,
        use_cached_results=use_cached_results,
        max_workers=max_workers,
        prompt_string=summary_prompt,
        n_per_commit=n_per_commit,
    )

    df[summary_column] = df[id_col].map(ids_to_summaries, None)
    return df


def log_major_step(text):
    full_text = "\n" + "*" * 100 + f"\n{text}\n" + "*" * 100
    logger.info(full_text)


def run_pipeline(
    paths,
    enrich_input_uri=None,
    meta_output_uri=None,
    relevance_model_name=CB_CD_MODEL_4OMINI,
    relevance_model_system_prompt=DEFAULT_CLIMATE_SYSTEM_PROMPT,
    relevance_model_prompt_format=DEFAULT_CLIMATE_PROMPT_FORMAT,
    adaptation_model_id=adp.CPI_ADAPTATION_MODEL_2024_ID,
    num_records=None,
    run_process_images=False,
    id_col='id',
    text_fields=TEXT_FIELDS,
    label_override_filepath=None,
    max_workers=MAX_WORKERS,
    n_per_commit=N_PER_COMMIT,
):
    """Pure enrichment: scrape, summarize, classify, geocode, and write the meta artifact.

    Taxonomy mapping (One Earth, Drawdown) is a follow-on stage that reads the
    meta artifact — see climate-landscape's ``process_map_taxonomies``.
    """
    log_major_step("loading pre-processed combined crunchbase + candid data")
    if enrich_input_uri:
        df_cb_cd_full = read_dataframe(enrich_input_uri)
    else:
        # TODO: remove local-json fallback once callers pass enrich_input_uri
        df_cb_cd_full = pd.read_json(paths['enrich_input_file'])
    logger.info("Loaded %s organizations", len(df_cb_cd_full))

    if num_records:
        log_major_step(f"Filtering down to {num_records} records")
        n_per_source = round(num_records // 2)
        df_cb_cd = pd.concat([
            df_cb_cd_full[df_cb_cd_full['Data Source'] == "Crunchbase"].iloc[:n_per_source],
            df_cb_cd_full[df_cb_cd_full['Data Source'] == "Candid"].iloc[:n_per_source],
        ])
    else:
        df_cb_cd = df_cb_cd_full

    log_major_step("Preparing DF for relevance model")
    df_cb_cd = prepare_for_relevance_model(df_cb_cd, max_workers=max_workers)
    # Run Model for CB
    log_major_step("Running relevance model")
    df_cb_cd, df_cb_cd_errors = run_relevance_model(
        df=df_cb_cd,
        model_name=relevance_model_name,
        system_prompt=relevance_model_system_prompt,
        prompt_format=relevance_model_prompt_format,
        column_text='text_for_relevance_model',
        idn=id_col,
        label_override_filepath=label_override_filepath,
        n_per_commit=n_per_commit,
        max_workers=max_workers,
    )
    df_cb_cd.to_json(paths['relevance_model_results'], orient='records')

    df_relevant = df_cb_cd[df_cb_cd['prediction_relevant'] == 1].copy()
    logger.info(
        "Filtered to relevant orgs. Went from %s to %s orgs",
        df_cb_cd.shape[0],
        df_relevant.shape[0],
    )

    log_major_step("Generating website summaries")

    # Reset the canonical website column and let `get_website_summaries` fill it in
    if 'Website' in df_relevant.columns:
        df_relevant.pop('Website')

    summaries, df_relevant = get_website_summaries(
        df_relevant,
        skip_existing=True,
        max_workers=max_workers,
        n_per_commit=n_per_commit,
    )

    df_relevant['extracted_website_key'] = df_relevant['Website'].apply(
        lambda x: extract_website_name(x)
        if coerced_bool(x) else None
    )
    summaries = {extract_website_name(k): v for k, v in summaries.items()}
    df_relevant['Website Summary'] = df_relevant['extracted_website_key'].map(summaries, None)

    # LinkedIn
    log_major_step("Scraping LinkedIn")
    with get_session() as session:
        df_linkedin = li.scrape_organizations_psql(
            urls=df_relevant[df_relevant['LinkedIn'].notnull()]["LinkedIn"],
            session=session,
            api_key=GLOBAL_CONFIG["linkedin"]["coresignal_api_key"],
            skip_existing=True,
            max_errors=1,
            n_per_commit=10,
        )

    df_linkedin.drop(columns=['summary'], inplace=True)
    df_linkedin.rename(
        columns={"about": "About LinkedIn", "name": "profile_name"},
        inplace=True,
    )
    # Combine the LinkedIn data
    df_relevant = combine_cb_cd_li(df_relevant, df_linkedin)
    # Now if the LinkedIn data from Coresignal is missing, assume it's a dead link
    # `datasource` is a column that is only present in the LinkedIn data

    # FIXME (ztawil): Better column naming patterns to know what is coming from where
    # https://airtable.com/appyWHEF7oCMjw6zR/tblvnllSFsnMOQrA1/viw8A93szuWwwCSjo/recf7V9RBN3CB0Sw6?blocks=hide
    df_relevant['Original LinkedIn'] = df_relevant['LinkedIn']
    df_relevant['LinkedIn'] = df_relevant.apply(
        lambda x: x['Original LinkedIn'] if coerced_bool(x['Original LinkedIn']) and coerced_bool(x['datasource']) else None,
        axis=1,
    )

    log_major_step("Cleaning Text Fields")
    # Clean Text of Line Breaks, Tabs, Double Spaces
    for col in text_fields:
        condition = ~df_relevant[col].isna()
        if len(df_relevant[condition]) == 0:
            continue
        cf.clean_spaces_linebreaks_col(df_relevant, col)

    log_major_step("Running Adaptation/Mitigation Model")
    # Adaptation/Mitigation/Dual
    df_relevant['text'] = cf.join_strings_no_missing(df_relevant, text_fields)

    preds_adapt = adp.generate_predictions_adapt_mit_remote_sql(
        df_relevant,
        50,
        'id',
        'text',
        api_key=GLOBAL_CONFIG['baseten']['api_key'],
        model_id=adaptation_model_id,
    )
    df_relevant["predictions_adapt"] = df_relevant["id"].map(preds_adapt)
    df_relevant = df_relevant[df_relevant["predictions_adapt"].isin(["adaptation", "mitigation", "both"])]


    df_relevant = df_relevant[
        df_relevant['Description'].notnull() |
        df_relevant['Website Summary'].notnull() |
        df_relevant['About LinkedIn'].notnull()
    ].copy()

    df_relevant['missing_all_texts'] = df_relevant.apply(
        lambda x: all([not(coerced_bool(x[col])) for col in text_fields],),
        axis=1,
    )
    log_major_step(f"Removing any organizations without Description, Website Summary, or LinkedIn About: {df_relevant['missing_all_texts'].sum()}")
    df_relevant = df_relevant[~df_relevant['missing_all_texts']].copy()

    # ADD SUMMARY OF SUMMARIES
    log_major_step("Adding summary of summaries")
    df_relevant = add_summary_of_summaries(
        df_relevant,
        text_fields=text_fields,
        id_col='id',
        use_cached_results=True,
        max_workers=max_workers,
        n_per_commit=n_per_commit,
    )


    # PROCESS DIVERSITY TAGS
    log_major_step("Adding Diversity Tags")
    cf.string2list(df_relevant, ['diversity'])  # format as lists

    # Run Geotagging
    log_major_step("Geotagging")
    # fill missing values in TEXT_FIELDS with empty strings
    df_relevant.fillna({col: "" for col in text_fields}, inplace=True)
    df_relevant = add_geotags(df_relevant, text_fields=text_fields, max_workers=max_workers)

    # Geocode the HQ
    log_major_step("Geocoding")
    df_relevant = geocode.add_geo_lat_long(
        df_relevant,  # use file trimmed of <min search terms
        idCol="id",  # unique id column
        address="Location",  # address column
    )
    df_relevant = geocode.clean_geo(df_relevant, summarize_new_geo=False)

    # ADD CLIMATE KEYWORDS
    log_major_step("Adding climate keywords using simple n-gram match")
    # load kwd mapping table (here it is master_term: [list of search terms])
    # terms used in database search
    df_climate_kwds = pd.read_excel(
        paths['common_kwds'] / "climate_kwd_map_byTag.xlsx", engine='openpyxl'
    )
    cf.string2list(
        df_climate_kwds,
        ['broad_tags', 'bigram', 'unigram', 'search_terms']  # format as lists
    )
    df_relevant = add_climate_keywords(
        df=df_relevant,
        keyword_path=paths['common_kwds'] / "climate_kwd_map_byTag.xlsx",
        df_climate_kwds=df_climate_kwds,
        # Add the Sector_String column as a column to search
        text_fields=text_fields + ["Sector_String"],
    )

    # SEPARATE EQUITY AND APPROACH TAGS
    log_major_step("Separating 'climate equity' and 'approach' tags.")
    tag_attr = 'climate_kwds'
    equity = "Equity-Justice Mentions"

    equity_list = df_climate_kwds[df_climate_kwds.equity == 1].tag.tolist()
    approach_list = df_climate_kwds[df_climate_kwds.strategy == 1].tag.tolist()

    tags = df_relevant[tag_attr].apply(lambda x: [val[0] for val in x])

    # Underserved Community Words
    # Need to do this before we filter the tags
    underserved_kwds = ['justice', 'indigenous', 'low income', 'poverty', 'underserved', 'marginalized']
    df_relevant["Underserved Community Keywords"] = tags.apply(lambda x: [tag for tag in x if tag in underserved_kwds])

    df_relevant[equity] = tags.apply(lambda x: [tag for tag in x if tag in equity_list])
    df_relevant["Approach Tags"] = tags.apply(lambda x: [tag for tag in x if tag in approach_list])

    # strip equity and approach from main tags (minus tags flagged to keep with main tags)
    keep_list = df_climate_kwds[df_climate_kwds['eq_strat_keep'] == 1].tag.tolist()
    # set of tags to remove from main tags
    strip_tags = set(equity_list+approach_list) - set(keep_list)
    # remove weighted tags and renormalize weights
    tft.blacklist_wtd_tags(df_relevant, tag_attr, strip_tags)
    df_relevant[tag_attr] = df_relevant[tag_attr].apply(lambda x: [tag for tag in x if tag[0] not in strip_tags])

    # add 'climate equity' yes/no
    df_relevant[equity].fillna("", inplace=True)
    df_relevant["Any Equity-Justice Mention"] = df_relevant[equity].apply(
        lambda x: "no equity-justice mention" if len(x) == 0 else "equity-justice mention"
    )

    df_relevant["Any Underserved Community Mention"] = df_relevant["Underserved Community Keywords"].apply(
        lambda x: "no underserved community mention" if len(x) == 0 else "underserved community mention"
    )

    # Run Prediction for Organization Type
    log_major_step("Running Prediction for Organization Type")
    df_relevant = org_type_classifier.predict(df_relevant)

    if run_process_images:
        # Add Logos
        log_major_step("Adding Logos")
        df_relevant = images.add_profile_images(
            df_relevant,
            # name of file to store image urls
            paths['images_name'],
            paths['images_errors_name'],
            id_col='id',  # col for merging
            # local folder to hold image metadata files
            images_meta_path=paths['images_path'],
            name_col='Organization',  # use for image filename
            image_url='image_url',  # image source url
            # local directory to store image files
            image_directory=str(paths['images_path']) + "/image_files",
            # s3 bucket to store images
            bucket=paths['images_bucket'].stem,
            grayscale=False,  # convert to BW image
            load_existing=True,
        )
    else:
        df_relevant['Image_URL'] = ""

    df_relevant['uid'] = df_relevant.id

    logger.info(
        "\nWriting final file of %s US-based organizations enriched with metadata",
        len(df_relevant),
    )
    logger.info('\n df_relevant final\n%s', df_relevant['Data Source'].value_counts())
    # TODO: remove local xlsx/json writes once all consumers read meta_output_uri parquet
    cf.write_excel_no_hyper(df_relevant, paths['results_path']/'cb_cd_li_meta.xlsx')
    df_relevant.to_json(paths['results_path'] / "cb_cd_li_meta.json", orient='records')
    if meta_output_uri:
        write_dataframe(
            df_relevant,
            meta_output_uri,
            lineage={
                "source": "enrichment_pipeline.run_pipeline",
                "enrich_input_uri": enrich_input_uri,
                "row_count": len(df_relevant),
            },
        )

    return df_relevant
