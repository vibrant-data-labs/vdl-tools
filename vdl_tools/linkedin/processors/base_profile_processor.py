from copy import deepcopy
from typing import List
from operator import itemgetter

'''
Output format structure:
{
    id: int,
    parent_id: int,
    is_parent: int,
    full_name: str,
    first_name: str,
    last_name: str,
    headline: str,
    created_at: str,
    updated_at: str,
    checked_at: str,
    public_profile_id: str,
    profile_url: str,
    location: str,
    city: str,
    state: str,
    industry: str,
    summary: str,
    services: str,
    profile_photo_url: str,
    deleted: int,
    country: str,
    country_iso_2: str,
    country_iso_3: str,
    regions: [
        {
            region: str
        }
    ],
    recommendations_count: int,
    connections_count: int,
    follower_count: int,
    experience_count: int,
    shorthand_name: str,
    canonical_shorthand_name: str,
    shorthand_names: [
        {
            shorthand_name: str
        }
    ],
    historical_ids: [
        {
            id: int
        }
    ],
    also_viewed: [
        {
            id: str,
            full_name: str,
            profile_url: str,
            headline: str,
            location: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
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
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    certifications: [
        {
            id: str,
            title: str,
            issuer: str,
            credential_id: str,
            certificate_url: str,
            certificate_logo_url: str,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            issuer_url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    courses: [
        {
            id: str,
            organizer: str,
            title: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    education: [
        {
            id: str,
            institution: str,
            program: str,
            institution_id: int,
            institution_source_id: int,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            activities_and_societies: str,
            description: str,
            institution_url: str,
            institution_logo_url: str,
            institution_shorthand_name: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    experience: [
        {
            id: str,
            title: str,
            location: str,
            company_id: int,
            company_source_id: int,
            company_name: str,
            company_url: str,
            company_logo_url: str,
            company_shorthand_name: str,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            is_current: int,
            duration: str,
            description: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    groups: [
        {
            id: str,
            title: str,
            url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    interests: [
        {
            id: str,
            interest: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    languages: [
        {
            id: str,
            language: str,
            proficiency: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    organizations: [
        {
            id: str,
            organization: str,
            position: str,
            description: str,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    patents: [
        {
            id: str,
            title: str,
            status: str,
            inventors: [
                {
                    full_name: str,
                    profile_url: str,
                    order_in_profile: int
                }
            ],
            date: str,
            date_year: int,
            date_month: int,
            date_day: int,
            patent_url: str,
            description: str,
            patent_or_application_number: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    posts_see_more_urls: [
        {
            id: str,
            url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    projects: [
        {
            id: str,
            name: str,
            project_url: str,
            description: str,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            team_members: [
                {
                    full_name: str,
                    profile_url: str,
                    order_in_profile: int
                }
            ],
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    publications: [
        {
            id: str,
            title: str,
            publisher: str,
            date: str,
            date_year: int,
            date_month: int,
            date_day: int,
            description: str,
            authors: [
                {
                    full_name: str,
                    profile_url: str,
                    order_in_profile: int
                }
            ],
            publication_url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    recommendations: [
        {
            id: str,
            recommendation: str,
            full_name: str,
            referee_url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    similar_profiles: [
        {
            id: str,
            profile_url: str,
            full_name: str,
            headline: str,
            location: str,
            company: str,
            followers: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    others_named: [
        {
            id: str,
            profile_url: str,
            full_name: str,
            headline: str,
            location: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    skills: [
        {
            id: str,
            skill: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    test_scores: [
        {
            id: str,
            title: str,
            date: str,
            date_year: int,
            date_month: int,
            date_day: int,
            description: str,
            score: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    volunteering_cares: [
        {
            id: str,
            care: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    volunteering_opportunities: [
        {
            id: str,
            opportunity: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    volunteering_positions: [
        {
            id: str,
            organization: str,
            role: str,
            cause: str,
            date_from: str,
            date_from_year: int,
            date_from_month: int,
            date_to: str,
            date_to_year: int,
            date_to_month: int,
            duration: str,
            description: str,
            organization_url: str,
            organization_shorthand_name: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    volunteering_supports: [
        {
            id: str,
            support: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    websites: [
        {
            id: str,
            personal_website: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    course_suggestions: [
        {
            id: str,
            title: str,
            course_url: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    activity: [
        {
            id: str,
            activity_url: str,
            title: str,
            action: str,
            order_in_profile: int,
            deleted: int,
            created_at: str,
            updated_at: str
        }
    ],
    hidden_details: [
        {
            hidden_collection: str
        }
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
    'keep': ["organization", "position", "description", "date_from", "date_to", "order_in_profile"],
    'unique': ['organization', 'position']
}

__volunteering_positions = {
    'keep': ['organization', 'role', 'cause', 'date_from', 'date_to', 'description', 'organization_url', 'order_in_profile'],
    'unique': ['organization', 'role', 'date_from']
}

def __process_profile_arr(data: List[dict], map_config) -> List[dict]:
    # Filter out deleted items (where deleted == 1)
    # BASE Endpoint sometimes returns deleted items
    profile_data = [item for item in data if item.get('deleted', 0) != 1]
    
    getter = itemgetter(*map_config['unique'])
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
        'volunteering_positions'
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

    if 'volunteering_positions' in updated_profile:
        updated_profile['volunteering_positions'] = __process_profile_arr(updated_profile['volunteering_positions'], __volunteering_positions)

    return updated_profile
