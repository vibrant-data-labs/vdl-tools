import re
from ast import literal_eval
from collections import defaultdict
import json

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


from vdl_tools.linkedin.profile_loader import scrape_profiles
from vdl_tools.shared_tools.project_config import get_paths
from vdl_tools.shared_tools.funding_calcations import get_df_location


RUN_LINKEDIN_SCRAPER = True
PATHS = get_paths()


YEAR_REGEX = r"\b(19|20)\d{2}\b"
def get_min_experience_date(experience_collection):
    min_date = None
    for experience in experience_collection:
        start_date = experience.get("date_from")
        if not start_date:
            continue
        start_year_match = re.search(YEAR_REGEX, start_date)
        if not start_year_match:
            continue
        start_year = int(start_year_match.group())
        if not min_date or start_year < min_date:
            min_date = start_year
        experience["start_year"] = start_year
    return min_date, experience_collection


def get_org_uid_for_linkedin_row(linkedin_row, linkedin_id_to_org_uid):

    org_uid = linkedin_id_to_org_uid.get(linkedin_row['li_id'])
    if not org_uid:
        org_uid = linkedin_id_to_org_uid.get(linkedin_row['li_id_alt'])
    return org_uid


def extract_items(x, key):
    # Extracts items from a list of dictionaries
    return [item[key] for item in x if item[key] is not None] if len(x) > 0 else []


def load_venture_df(raw_df):
    # cft_df["Keywords"] = cft_df["Keywords"].apply(lambda x: x.split("|") 'if 'isinstance(x, str) else [])
    # venture_df = cft_df[cft_df['Funding Category'].isin(["Seed", "Early Venture", "Late Venture"])].copy()
    venture_df = get_df_location(raw_df, 'United States')

    crunchbase_df = pd.read_excel(PATHS["expanded_orgs_data"])
    crunchbase_df["founded_on"].replace(np.nan, "{}", inplace=True)
    crunchbase_df["founded_on"] = crunchbase_df["founded_on"].apply(literal_eval)
    crunchbase_df["founded_year"] = crunchbase_df["founded_on"].apply(lambda x: x.get("value", "")[:4])
    crunchbase_df["founded_year"] = crunchbase_df["founded_year"].apply(lambda x: int(x) if x else np.nan)


    venture_df = venture_df.merge(crunchbase_df[["uuid", "founded_year"]], left_on="uuid", right_on="uuid")
    venture_df.drop_duplicates(subset="uuid", inplace=True)
    return venture_df


def clean_li_profiles(df_li_profiles: pd.DataFrame):
    df_li_profiles['skills'] = df_li_profiles['member_skills_collection'].apply(
        lambda x: extract_items(x, 'skill'))  # skills list
    df_li_profiles['education'] = df_li_profiles['member_education_collection'].apply(
        lambda x: extract_items(x, 'title'))  # educational affiliations list
    df_li_profiles['experiences'] = df_li_profiles['member_experience_collection'].apply(
        lambda x: extract_items(x, 'description'))  # experience descriptions list
    df_li_profiles['projects'] = df_li_profiles['member_projects_collection'].apply(
        lambda x: extract_items(x, 'description'))  # project descriptions list
    df_li_profiles['languages'] = df_li_profiles['member_languages_collection'].apply(
        lambda x: extract_items(x, 'language'))  # languages list
    df_li_profiles['affiliations'] = df_li_profiles['member_experience_collection'].apply(
        lambda x: extract_items(x, 'company_name'))  # org affiliations list
    df_li_profiles['titles'] = df_li_profiles['member_experience_collection'].apply(
        lambda x: extract_items(x, 'title'))  # job titles list
    df_li_profiles['websites'] = df_li_profiles['member_websites_collection'].apply(
        lambda x: extract_items(x, 'website'))  # websites list
    # fill empty canonical shorthand names with member shorthand names
    df_li_profiles['canonical_shorthand_name'].fillna(df_li_profiles['member_shorthand_name'], inplace=True)
    df_li_profiles['li_id'] = df_li_profiles['canonical_shorthand_name']  # linkedin id
    df_li_profiles['li_id_alt'] = df_li_profiles['member_shorthand_name']  # linkedin id alt
    # clean html from each element of the lists
    df_li_profiles['experiences'] = df_li_profiles['experiences'].apply(lambda x: [html_to_plain_text(i) for i in x])
    df_li_profiles['projects'] = df_li_profiles['projects'].apply(lambda x: [html_to_plain_text(i) for i in x])
    # drop unnecessary columns
    df_li_profiles_cleaned = df_li_profiles.drop(columns=['member_awards_collection',
                                                          'member_certifications_collection',
                                                          # 'member_education_collection',
                                                          # 'member_experience_collection',
                                                          'member_groups_collection',
                                                          'member_languages_collection',
                                                          # 'member_projects_collection',
                                                          'member_recommendations_collection',
                                                          'member_skills_collection',
                                                          'member_volunteering_positions_collection',
                                                          'member_websites_collection',
                                                          # 'member_shorthand_name',
                                                          'member_shorthand_name_hash',
                                                          'canonical_hash',
                                                          'experience_count',
                                                          'country',
                                                          'industry',
                                                          'first_name',
                                                          'last_name',
                                                          'connections',
                                                          'last_updated',
                                                          ])

    df_li_profiles_cleaned["first_experience_year"], df_li_profiles_cleaned["member_experience_collection"] = zip(
        *df_li_profiles_cleaned["member_experience_collection"].apply(get_min_experience_date)
    )

    return df_li_profiles_cleaned


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


def get_experiences_before_founding(row):
    return [
        x for x in
        row['member_experience_collection']
        # When no start year is found
        if not x.get("start_year")
        or x.get("start_year") < row['org_founded_year']
    ]


if __name__ == "__main__":
    import os

    if not os.path.exists(PATHS["project_relevance"]):
        os.makedirs(PATHS["project_relevance"])

    # NOTE: This should be the dataframe that is similar to a crunchbase export
    # After processing it like in the elemental repo
    # https://github.com/vibrant-data-labs/elemental/blob/64170f9701225f0646e1f2006ffcfefe331085c8/elemental_/prepare_survivorship_data.py
    raw_df = pd.read_json(PATHS["raw_venture_data"]) 

    founder_df = pd.read_excel(PATHS["expanded_orgs_founders"])
    founder_df["primary_organization"].replace(np.nan, '{}', inplace=True)
    founder_df['org_uid'] = founder_df["primary_organization"].apply(lambda x: literal_eval(x.strip('"')).get('uuid'))

    venture_df = load_venture_df()


    venture_id_to_founders = defaultdict(list)
    for i, row in venture_df.iterrows():
        founders = founder_df[founder_df['org_uid'] == row['uuid']].to_dict(orient="records")

        # org_permalink = row["Crunchbase"].split("/")[-1]
        other_founders = founder_df[founder_df['org_permalink'] == row['permalink']].to_dict(orient="records")


        founder_ids = {x['uuid'] for x in founders}
        for founder in other_founders:
            if founder['uuid'] not in founder_ids:
                founders.append(founder)

        if founders:
            venture_id_to_founders[row['uuid']].extend(founders)
        else:
            venture_id_to_founders[row['uuid']] = []


    linkedin_id_to_org_uid = {}
    linkedin_urls = set()
    for org_uid, founders in venture_id_to_founders.items():
        for founder in founders:
            if founder['linkedin'] and isinstance(founder['linkedin'], str):
                founder['linkedin'] = literal_eval(founder['linkedin'].strip('""'))
            else:
                founder['linkedin'] = {}

            if founder.get('linkedin'):
                linkedin_url = founder['linkedin']["value"]
                linkedin_urls.add(linkedin_url)
                linkedin_id = linkedin_url.strip('/').split('/')[-1]
                linkedin_id_to_org_uid[linkedin_id] = org_uid


    if RUN_LINKEDIN_SCRAPER:
        current_data = pd.read_json(PATHS["raw_linkedin_profiles"])
        new_urls = set(linkedin_urls).difference(set(current_data['original_id'].values))
        
        new_linkedin_urls = pd.Series(list(new_urls))
        new_linkedin_df = scrape_profiles(new_linkedin_urls)
        linkedin_df = pd.concat([current_data, new_linkedin_df], ignore_index=True)
        linkedin_df.to_json(PATHS["raw_linkedin_profiles"])
    else:
        linkedin_df = pd.read_json(PATHS["raw_linkedin_profiles"])

    df_li_profiles_cleaned = clean_li_profiles(linkedin_df)

    df_li_profiles_cleaned = df_li_profiles_cleaned[df_li_profiles_cleaned["canonical_url"] != "https://www.linkedin.com/in/en"].copy()

    df_li_profiles_cleaned['org_uid'] = (
        df_li_profiles_cleaned
        .apply(
            lambda x: get_org_uid_for_linkedin_row(x, linkedin_id_to_org_uid),
            axis=1
        )
    )

    df_li_profiles_cleaned = df_li_profiles_cleaned.merge(
        venture_df[['uuid', 'founded_year']],
        left_on='org_uid',
        right_on='uuid'
    ).rename(columns={'founded_year': 'org_founded_year'})


    df_li_profiles_cleaned["experiences_before_founding"] = df_li_profiles_cleaned.apply(get_experiences_before_founding, axis=1)

    df_li_profiles_cleaned.to_json(PATHS["clean_linkedin_profiles"])
    venture_df.to_json(PATHS["venture_orgs_w_founding_year"])