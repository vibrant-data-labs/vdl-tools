"""
LinkedIn Employee Profile Loader for PostgreSQL

This module loads LinkedIn employee profiles from Coresignal API
and stores them in PostgreSQL database using either Base or Clean models.
"""

from typing import Literal

from more_itertools import chunked
import pandas as pd

from vdl_tools.shared_tools.database_cache.database_models.linkedin_base_employee import LinkedInBaseEmployee
from vdl_tools.shared_tools.database_cache.database_models.linkedin_clean_employee import LinkedInCleanEmployee
from vdl_tools.shared_tools.tools.logger import logger

import vdl_tools.linkedin.handlers.coresignal_query as cs_query
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id


MAX_ERRORS = 5
CORESIGNAL_DATASOURCE = 'coresignal'


ModelType = Literal['base', 'clean']


def _get_model_config(model_type: ModelType):
    """Get the model class and configuration based on model type."""
    if model_type == 'base':
        return {
            'model': LinkedInBaseEmployee,
            'api_function': cs_query.get_base_person,
            'id_field': 'id',
            'url_field': 'profile_url',
        }
    elif model_type == 'clean':
        return {
            'model': LinkedInCleanEmployee,
            'api_function': cs_query.get_clean_person,
            'id_field': 'id',
            'url_field': 'websites_linkedin',
        }
    else:
        raise ValueError(f"Invalid model_type: {model_type}. Must be 'base' or 'clean'")


def get_employee_profiles(
    urls: list[str],
    session,
    api_key: str,
    model_type: ModelType = 'base',
    skip_existing: bool = True,
    n_per_commit: int = 10,
    max_errors: int = MAX_ERRORS,
) -> pd.DataFrame:
    """
    Retrieve LinkedIn employee profiles from Coresignal API and store in database.
    
    Parameters
    ----------
    urls : list[str]
        List of LinkedIn profile URLs
    session : SQLAlchemy session
        Database session for querying and storing data
    api_key : str
        Coresignal API key
    model_type : {'base', 'clean'}
        Type of employee model to use ('base' for LinkedInBaseEmployee, 
        'clean' for LinkedInCleanEmployee)
    skip_existing : bool, default=True
        Whether to skip profiles that already exist in the database
    n_per_commit : int, default=10
        Number of profiles to commit at once
    max_errors : int, default=MAX_ERRORS
        Maximum number of errors to retry for a profile
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing the retrieved profiles with original URLs
    """

    logger.info("Received %s LinkedIn URLs for querying", len(urls))

    # Get model configuration
    config = _get_model_config(model_type)
    Model = config['model']
    api_function = config['api_function']

    # Extract LinkedIn IDs from URLs
    urls_ids = [(url, extract_linkedin_id(url)) for url in urls if extract_linkedin_id(url)]
    if not urls_ids:
        logger.warning("No valid LinkedIn IDs found in provided URLs")
        return pd.DataFrame()

    # Determine which profiles to query
    if skip_existing:
        # Query existing profiles from database
        found_rows = (
            session
            .query(Model)
            .filter(
                getattr(Model, "original_url").in_([x[1] for x in urls_ids]),
            )
            .all()
        )

        found_ids = {str(getattr(row, "original_url")) for row in found_rows}

        # Filter out profiles that already exist
        unfound_rows = [
            x for x in urls_ids
            if x[1] not in found_ids
        ]
    else:
        found_rows = []
        unfound_rows = urls_ids

    # Convert existing rows to dictionaries
    results = [row.to_dict() for row in found_rows]

    if not unfound_rows:
        logger.info("All profiles already exist in database")
        return pd.DataFrame(results)

    logger.info("Found %s previously queried results in cache", len(found_rows))
    logger.info("Need to query %s new profiles from Coresignal API", len(unfound_rows))

    # Query new profiles from Coresignal API
    newly_found = []
    for chunk in chunked(unfound_rows, n_per_commit):
        for url, linkedin_id in chunk:
            logger.info("Querying profile: %s", linkedin_id)
            
            try:
                result = api_function(linkedin_id, api_key)
            except Exception as e:
                logger.error("Error querying profile %s: %s", linkedin_id, str(e))
                result = None

            if not result:
                logger.warning("No result for %s", url)
                # For failed queries, we don't store them in the database
                # to allow retry on next run
                continue

            # Add original URL to result
            result['original_url'] = url

            # Store full result as JSONB (make a copy to avoid circular reference)
            result['full_result'] = {k: v for k, v in result.items()}

            # Filter result to only include columns that are in the model
            db_result = {k: v for k, v in result.items() if k in Model.__table__.columns.keys()}

            # Store in database
            try:
                sql_obj = Model(**db_result)
                session.merge(sql_obj)
                newly_found.append(linkedin_id)
            except Exception as e:
                logger.error("Error storing profile %s: %s", linkedin_id, str(e))
                continue

            # Add to results for output DataFrame
            results.append(result.copy())

        # Commit batch
        try:
            logger.info("Committing %s profiles", len(chunk))
            session.commit()
            logger.info("Successfully committed. Total found: %s of %s", len(newly_found), len(unfound_rows))
        except Exception as e:
            logger.error("Error committing batch: %s", str(e))
            session.rollback()

    logger.info("Completed. Total profiles retrieved: %s", len(results))
    return pd.DataFrame(results)


if __name__ == '__main__':
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.tools.config_utils import get_configuration
    
    config = get_configuration()
    
    # Example URLs
    test_urls = [
        'https://www.linkedin.com/in/zeintawil/',
        'https://www.linkedin.com/in/eric-berlow/',
        'https://www.linkedin.com/in/jasonjhirsch/'
    ]

    with get_session() as session:
        # Test with base model
        print("\n=== Testing Base Employee Model ===")
        df_base = get_employee_profiles(
            test_urls,
            session,
            config['linkedin']['coresignal_api_key'],
            model_type='base',
            skip_existing=True,
        )
        print(f"Retrieved {len(df_base)} base profiles")
        if not df_base.empty:
            # Show available columns
            available_cols = [col for col in ['id', 'full_name', 'original_url'] if col in df_base.columns]
            if available_cols:
                print(df_base[available_cols].head())
            print(f"Columns: {list(df_base.columns)[:10]}...")  # Show first 10 columns
        
        # Test with clean model
        print("\n=== Testing Clean Employee Model ===")
        df_clean = get_employee_profiles(
            test_urls,
            session,
            config['linkedin']['coresignal_api_key'],
            model_type='clean',
            skip_existing=True,
        )
        print(f"Retrieved {len(df_clean)} clean profiles")
        if not df_clean.empty:
            # Show available columns
            available_cols = [col for col in ['id', 'full_name', 'original_url'] if col in df_clean.columns]
            if available_cols:
                print(df_clean[available_cols].head())
            print(f"Columns: {list(df_clean.columns)[:10]}...")  # Show first 10 columns
