"""Tools used for further cleaning profiles to get most recent information from the stored full profiles"""

import pandas as pd
import vdl_tools.shared_tools.common_functions as cf
import regex as re
from bs4 import BeautifulSoup

from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id


def cleanhtml(raw_html):
    CLEANR = re.compile('<.*?>')
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext


def html_to_plain_text(html_text: str):
    """
    Convert HTML text to plain text.
    Returns:
    - str, the plain text extracted from the HTML
    """
    # Parse the HTML text using BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')

    return soup.get_text()


def extract_items(x, key):
    # Extracts items from a list of dictionaries
    return [item[key] for item in x if item[key] is not None] if len(x) > 0 else []


def add_extracted_column(df, new_column, old_column, key):
    # Extracts items from a list of dictionaries and adds them as a new column
    df[new_column] = df[old_column].apply(lambda x: extract_items(x, key))
    return df


def clean_li_profiles(df_li_profiles: pd.DataFrame):
    """Clean LinkedIn profiles data

    Parameters
    ----------
    df_li_profiles : pd.DataFrame
        LinkedIn profiles data

    Returns
    -------
    pd.DataFrame
        Cleaned LinkedIn profiles data
    """
    # Skills
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'skills',
        'member_skills_collection',
        'skill',
    )

    # Education
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'education',
        'member_education_collection',
        'title',
    )

    # Experience descriptions list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'experiences',
        'member_experience_collection',
        'description',
    )

    # Project descriptions list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'projects',
        'member_projects_collection',
        'description',
    )

    # Languages list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'languages',
        'member_languages_collection',
        'language',
    )

    # Org affiliations list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'affiliations',
        'member_experience_collection',
        'company_name',
    )

    # Job titles list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'titles',
        'member_experience_collection',
        'title',
    )

    # Websites list
    df_li_profiles = add_extracted_column(
        df_li_profiles,
        'websites',
        'member_websites_collection',
        'website',
    )

    # fill empty canonical shorthand names with member shorthand names
    df_li_profiles['canonical_shorthand_name'].fillna(
        df_li_profiles['member_shorthand_name'],
        inplace=True,
    )

    # linkedin id
    df_li_profiles['li_id'] = df_li_profiles['canonical_shorthand_name']
    # linkedin id alt
    df_li_profiles['li_id_alt'] = df_li_profiles['member_shorthand_name']

    # clean html from each element of the lists
    df_li_profiles['experiences'] = df_li_profiles['experiences'].apply(
        lambda x: [html_to_plain_text(i) for i in x]
    )

    df_li_profiles['projects'] = df_li_profiles['projects'].apply(
        lambda x: [html_to_plain_text(i) for i in x]
    )

    delete_columns = [
        'member_awards_collection',
        'member_certifications_collection',
        'member_education_collection',
        'member_experience_collection',
        'member_groups_collection',
        'member_projects_collection',
        'member_languages_collection',
        'member_recommendations_collection',
        'member_skills_collection',
        'member_volunteering_positions_collection',
        'member_websites_collection',
        'member_shorthand_name_hash',
        'canonical_hash',
        'experience_count',
        'country',
        'industry',
        'first_name',
        'last_name',
        'connections',
        'last_updated',
    ]
    delete_columns = [col for col in delete_columns if col in df_li_profiles.columns]
    # drop unnecessary columns
    df_li_profiles_cleaned = df_li_profiles.drop(columns=delete_columns)

    return df_li_profiles_cleaned


def clean_li_orgs(df_li_orgs: pd.DataFrame):
    df_li_orgs['location'] = df_li_orgs['locations'].apply(lambda x: x[0] if len(x) > 0 else None)
    df_li_orgs['li_id'] = df_li_orgs['url'].apply(extract_linkedin_id)
    df_li_orgs['li_id_alt'] = df_li_orgs['li_id']

    # convert specialities into list all lower case
    df_li_orgs['sectors'] = df_li_orgs['specialties'].apply(
        lambda x: x.lower().split(", ") if x is not None else []
    )

    # combine summary and about
    df_li_orgs['about'] = cf.join_strings_no_missing(
        df_li_orgs,
        ['summary', 'about'],
        delim=" ",
    )
    # drop unnecessary columns
    df_li_orgs_cleaned = df_li_orgs.drop(
        columns=[
            'locations',
            'summary',
            'specialties',
            'company_size',
            'company_type',
            'industry',
            'founded',
        ]
    )
    return df_li_orgs_cleaned
