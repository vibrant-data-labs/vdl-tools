from copy import deepcopy
from typing import List
from operator import itemgetter

'''
Output format structure:
{
    description: str,
    headline: str,
    picture_url: str,
    location_raw_address: str,
    experience: [
        {
            title: str,
            description: str,
            company_name: str,
            date_from: str,
            date_to: str,
            location: str,
            department: str,
            management_level: str
        }
    ],
    education: [
        {
            title: str,
            major: str,
            institution_url: str,
            description: str,
            date_from: int,
            date_to: int
        }
    ],
    skills: [str],
    recommendations: [
        {
            recommendation: str,
            referee_name: str,
            referee_url: str
        }
    ],
    languages: [
        {
            language: str
            proficiency: str
            order_in_profile: int
        },
    ],
    organizations: [
        organization: str
        position: str
        description: str
        date_from: str
        date_to: str
        order_in_profile: int
    ],
    publications: [
        {
            title: str,
            publisher: str,
            date: str,
            publication_url: str
        }
    ],
    awards: [
        {
            id: str,
            title: str,
            issuer: str,
            description: str,
            date: str,
            date_year: int,
            date_month: int,
            date_day: int,
            order_in_profile: int,
        },
    ]
}
'''


__exp = {
    'keep': ['title', 'description', 'company_name', 'date_from', 'date_to', 'location', 'company_website', 'company_linkedin_url'],
    'unique': ['title', 'company_name', 'date_from', 'date_to']
}

__edu = {
    'keep': ['title', 'major', 'institution_url', 'date_from', 'date_to', "description"],
    'unique': ['title', 'major']
}

__recommendations = {
    'keep': ['recommendation', 'referee_name', 'referee_url'],
    'unique': ['referee_name', 'referee_url']
}

__publications = {
    'keep': ['title', 'publisher', 'date', 'publication_url', 'description'],
    'unique': ['title', 'date']
}

__awards = {
    'keep': ['title', 'issuer', 'description', 'date', 'order_in_profile'],
    'unique': ['title', 'date']
}

__languages = {
    'keep': ['language', 'proficiency'],
    'unique': ['language']
}

__organizations = {
    'keep': [ "organization", "position", "description", "date_from", "date_to", "order_in_profile"],
    'unique': ['organization', 'position']
}

def __process_profile_arr(data: List[dict], map_config) -> List[dict]:
    getter = itemgetter(*map_config['unique'])
    profile_data = data
    profile_data.sort(key=lambda x: x.get("order_in_profile", 0))
    res = list({getter(v):v for v in profile_data}.values())
    for item in res:
        item_keys = list(item.keys())
        for key in item_keys:
            if key not in map_config['keep']:
                item.pop(key, None)
    return res

def process_profile(profile: dict) -> dict:
    cols_to_keep = {
        'description',
        'headline',
        'picture_url',
        'location_raw_address',
        'experience',
        'education',
        'skills',
        'recommendations',
        'languages',
        'organizations',
        'patents',
        'publications',
        'awards'
    }

    updated_profile = {}
    for key in cols_to_keep.intersection(profile.keys()):
        updated_profile[key] = profile[key]

    if 'experience' in updated_profile:
        updated_profile['experience'] = __process_profile_arr(updated_profile['experience'], __exp)
    
    if 'education' in updated_profile:
        updated_profile['education'] = __process_profile_arr(updated_profile['education'], __edu)
    
    if 'recommendations' in updated_profile:
        updated_profile['recommendations'] = __process_profile_arr(updated_profile['recommendations'], __recommendations)
    
    if 'publications' in updated_profile:
        updated_profile['publications'] = __process_profile_arr(updated_profile['publications'], __publications)

    if 'languages' in updated_profile:
        updated_profile['languages'] = __process_profile_arr(updated_profile['languages'], __languages)

    if 'organizations' in updated_profile:
            updated_profile['organizations'] = __process_profile_arr(updated_profile['organizations'], __organizations)

    if 'awards' in updated_profile:
        updated_profile['awards'] = __process_profile_arr(updated_profile['awards'], __awards)

    return updated_profile


def process_organization(org_data: dict):
    raise NotImplementedError('Not implemented yet')