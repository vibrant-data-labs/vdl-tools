from copy import deepcopy
from typing import List
from operator import itemgetter

'''
Output format structure:
{
    name: str,
    first_name: str,
    last_name: str,
    title: str,
    url: str, # matches the input url
    location: str,
    industry: str,
    summary: str,
    logo_url: str,
    last_updated: date, # date of the last update in CoreSignal
    country: str,
    experience_count: int,
    member_shorthand_name: str,
    member_awards_collection: [
        {
            title: str,
            description: str,
            issuer: str,
            date: date
        }
    ],
    member_certifications_collection: [
        {
            name: str,
            authority: str,
            url: str,
            date_from: date,
            date_to: date
        }
    ],
    member_education_collection: [
        {
            title: str,
            subtitle: str,
            date_from: date,
            date_to: date,
            activities_and_societies: str,
            description: str,
            school_url: str
        }
    ],
    member_experience_collection: [
        {
            title: str,
            location: str,
            company_name: str,
            company_url: str,
            date_from: date,
            date_to: date,
            description: str
        }
    ],
    member_groups_collection: [
        {
            name: str,
            url: str
        }
    ],
    member_languages_collection: [
        {
            language: str,
            proficiency: str
        }
    ],
    member_projects_collection: [
        {
            name: str,
            url: str,
            description: str,
            date_from: date,
            date_to: date,
            team_members: str
        }
    ],
    member_skills_collection: [
        {
            skill: str
        }
    ],
    member_volunteering_positions_collection: [
        {
            organization: str,
            role: str,
            date_from: date,
            date_to: date,
            description: str,
            cause: str
        }
    ],
    member_websites_collection: [
        {
            website: str
        }
    ]
'''


__awards = {
    'keep': ['title', 'description', 'issuer', 'date'],
    'unique': ['title', 'issuer', 'date']
}
__cert = {
    'keep': ['name', 'authority', 'url', 'date_from', 'date_to'],
    'unique': ['name', 'authority', 'date_from', 'date_to']
}

__edu = {
    'keep': ['title', 'subtitle', 'date_from', 'date_to', 'activities_and_societies', 'description', 'school_url'],
    'unique': ['title', 'subtitle', 'date_from', 'date_to']
}

__exp = {
    'keep': ['title', 'location', 'company_name', 'company_url', 'date_from', 'date_to', 'description'],
    'unique': ['title', 'location', 'company_name', 'date_from', 'date_to']
}

__groups = {
    'keep': ['name', 'url'],
    'unique': ['name']
}

__proj = {
    'keep': ['name', 'url', 'description', 'date_from', 'date_to', 'team_members'],
    'unique': ['name']
}

__skills = {
    'keep': ['skill_id', 'member_skill_list'],
    'unique': ['skill_id'],
}

__vol = {
    'keep': ['organization', 'role', 'date_from', 'date_to', 'description', 'member_volunteering_positions_cause_list'],
    'unique': ['organization', 'role', 'date_from', 'date_to']
}

__website = {
    'keep': ['website'],
    'unique': ['website']
}

__lang = {
    'keep': ['member_language_list', 'member_language_proficiency_list'],
    'unique': ['language_id', 'proficiency_id']
}


def __process_profile_arr(data: List[dict], map_config) -> List[dict]:
    getter = itemgetter(*map_config['unique'])
    profile_data = data
    profile_data.sort(key=lambda x: x["created"], reverse=True)
    res = list({getter(v):v for v in profile_data}.values())
    for item in profile_data:
        item_keys = list(item.keys())
        for key in item_keys:
            if key not in map_config['keep']:
                item.pop(key, None)
    
    return res

def process_profile(profile: dict) -> dict:
    cols_to_remove = [
        'id',
        'hash',
        'recommendations_count',
        'last_response_code',
        'created',
        'outdated',
        'deleted',
        'connections_count',
        'last_updated_ux',
        'membed_shorthand_name',
        'membed_shorthand_name_hash',
        # 'canonical_url',
        'canonical_url_hash',
        # 'canonical_shorthand_name',
        'canonical_shorthand_name_hash',
        'member_also_viewed_collection',
        "member_courses_suggestion_collection",
        'member_courses_collection',
        'member_interests_collection',
        'member_organizations_collection',
        'member_patents_collection',
        'member_posts_see_more_urls_collection',
        'member_publications_collection',
        'member_similar_profiles_collection',
        'member_test_scores_collection',
        'member_volunteering_cares_collection',
        'member_volunteering_opportunities_collection',
        'member_volunteering_supports_collection'        
    ]

    updated_profile = deepcopy(profile)
    for key in cols_to_remove:
        updated_profile.pop(key, None)


    updated_profile['member_awards_collection'] = __process_profile_arr(updated_profile['member_awards_collection'], __awards)
    updated_profile['member_certifications_collection'] = __process_profile_arr(updated_profile['member_certifications_collection'], __cert)
    updated_profile['member_education_collection'] = __process_profile_arr(updated_profile['member_education_collection'], __edu)
    updated_profile['member_experience_collection'] = __process_profile_arr(updated_profile['member_experience_collection'], __exp)
    updated_profile['member_experience_collection'] = updated_profile['member_experience_collection'][:updated_profile['experience_count']]
    updated_profile['member_groups_collection'] = __process_profile_arr(updated_profile['member_groups_collection'], __groups)
    updated_profile['member_languages_collection'] = __process_profile_arr(updated_profile['member_languages_collection'], __lang)
    for item in updated_profile['member_languages_collection']:
        item['language'] = item["member_language_list"]["language"]
        if 'proficiency' in item["member_language_proficiency_list"]:
            item['proficiency'] = item["member_language_proficiency_list"]['proficiency']
        item.pop('member_language_list', None)
        item.pop('member_language_proficiency_list', None)

    updated_profile['member_projects_collection'] = __process_profile_arr(updated_profile['member_projects_collection'], __proj)
    updated_profile['member_skills_collection'] = __process_profile_arr(updated_profile['member_skills_collection'], __skills)
    for item in updated_profile['member_skills_collection']:
        item['skill'] = item['member_skill_list']["skill"]
        item.pop('member_skill_list', None)

    updated_profile['member_volunteering_positions_collection'] = __process_profile_arr(updated_profile['member_volunteering_positions_collection'], __vol)
    for item in updated_profile['member_volunteering_positions_collection']:
        cause = item['member_volunteering_positions_cause_list']['cause'] if ('cause' in item['member_volunteering_positions_cause_list']) else None
        item['cause'] = cause
        item.pop('member_volunteering_positions_cause_list', None)

    updated_profile['member_websites_collection'] = __process_profile_arr(updated_profile['member_websites_collection'], __website)
    return updated_profile


def process_organization(org_data: dict):
    raise NotImplementedError('Not implemented yet')
