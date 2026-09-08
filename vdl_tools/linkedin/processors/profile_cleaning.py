"""Tools used for further cleaning profiles to get most recent information from the stored full profiles"""

from urllib.parse import urlparse, parse_qs

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


def clean_employee_profiles(df_employee_profiles: pd.DataFrame):
    """Clean LinkedIn profiles from the new Coresignal employee ("base person") API.

    Takes the DataFrame returned by
    `vdl_tools.linkedin.employee_loader_psql.get_employee_profiles(model_type='base')`
    and returns the same cleaned schema that `clean_li_profiles` produced from the
    old member API, so downstream code keeps working:

    - original_id : the LinkedIn URL used to query (merge key back to source data)
    - name, title, url, location, summary, logo_url, connections_count
    - li_id, li_id_alt : canonical / raw shorthand names
    - list columns: skills, education, experiences, projects, languages,
      affiliations, titles, websites
    """
    df = df_employee_profiles.copy()

    # The loader can return a mix of cached DB rows and fresh API rows, whose
    # column sets differ slightly. Make sure every column we need exists.
    needed_cols = [
        'original_url', 'full_name', 'headline', 'profile_url', 'location',
        'summary', 'profile_photo_url', 'connections_count',
        'canonical_shorthand_name', 'shorthand_name',
        'education', 'experience', 'projects', 'languages', 'websites',
    ]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None

    # Nested JSONB columns can be None — replace with empty list so extraction works
    for col in ['education', 'experience', 'projects', 'languages', 'websites']:
        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

    def extract(col, key):
        # pull one key out of each dict in a list-of-dicts column, in profile order.
        # The new API includes historical entries flagged deleted=1 (old versions of
        # the same jobs/schools) — skip those or lists are bloated with stale duplicates.
        def _extract_items(items):
            live = [i for i in items if i.get('deleted', 0) != 1]
            live.sort(key=lambda i: i.get('order_in_profile') or 0)
            return [i.get(key) for i in live if i.get(key) is not None]
        return df[col].apply(_extract_items)

    # Skills: the new API replaced the 'skills' collection with 'inferred_skills'
    # (a flat list of strings). Fresh API rows have it at the top level;
    # cached DB rows only have it inside full_result.
    if 'inferred_skills' not in df.columns:
        df['inferred_skills'] = None
    if 'full_result' not in df.columns:
        df['full_result'] = None
    df['skills'] = [
        inferred if isinstance(inferred, list)
        else (full.get('inferred_skills') or []) if isinstance(full, dict)
        else []
        for inferred, full in zip(df['inferred_skills'], df['full_result'])
    ]

    df['education'] = extract('education', 'institution')    # school names (old schema's 'title')
    df['experiences'] = extract('experience', 'description') # job descriptions
    df['projects'] = extract('projects', 'description')      # project descriptions
    df['languages'] = extract('languages', 'language')       # language names
    df['affiliations'] = extract('experience', 'company_name')  # org names
    df['titles'] = extract('experience', 'title')            # job titles
    df['websites'] = extract('websites', 'personal_website') # personal websites

    # unwrap linkedin redirect links (linkedin.com/redir/redirect?url=<real url>)
    def unwrap_redirect(url):
        if 'linkedin.com/redir/redirect' in url:
            query = parse_qs(urlparse(url).query)
            return query.get('url', [url])[0]
        return url
    df['websites'] = df['websites'].apply(lambda x: [unwrap_redirect(u) for u in x])

    # clean any html markup from the free-text descriptions
    df['experiences'] = df['experiences'].apply(lambda x: [html_to_plain_text(i) for i in x])
    df['projects'] = df['projects'].apply(lambda x: [html_to_plain_text(i) for i in x])

    # linkedin shorthand ids (canonical preferred, raw as fallback)
    df['canonical_shorthand_name'] = df['canonical_shorthand_name'].fillna(df['shorthand_name'])
    df['li_id'] = df['canonical_shorthand_name']
    df['li_id_alt'] = df['shorthand_name']

    # rename new API fields to the old cleaned-schema names
    df = df.rename(columns={
        'original_url': 'original_id',
        'full_name': 'name',
        'headline': 'title',
        'profile_url': 'url',
        'profile_photo_url': 'logo_url',
    })

    # keep only the cleaned columns (drops raw JSONB, full_result, id, etc.)
    keep_cols = [
        'original_id', 'name', 'title', 'url', 'location', 'summary',
        'logo_url', 'connections_count', 'li_id', 'li_id_alt',
        'skills', 'education', 'experiences', 'projects', 'languages',
        'affiliations', 'titles', 'websites',
    ]
    return df[keep_cols]


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
