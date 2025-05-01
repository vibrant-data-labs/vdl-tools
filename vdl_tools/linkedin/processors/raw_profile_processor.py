from bs4 import BeautifulSoup
import vdl_tools.linkedin.processors.processor_utils as pu

def __parse_edu_item(edu_item):
    title = pu.safe_get_attr(edu_item.find('h3'), "text")
    subtitle = pu.safe_get_attr(edu_item.find('h4'), "text")
    time_items = edu_item.find_all('time')
    date_from = None
    date_to = None
    if len(time_items) > 0:
        date_from = pu.safe_get_attr(time_items[0], 'text')
    if len(time_items) > 1:
        date_to = pu.safe_get_attr(time_items[1], 'text')

    activities_and_societies = ''
    description = pu.safe_get_attr(edu_item.find('span'), 'text')
    link_url = edu_item.find('a', {'class': 'profile-section-card__image-link'})
    if link_url:
        school_url = link_url['href']
    else:
        school_url = ''
    return {
        'title': title.strip(),
        'subtitle': subtitle.strip(),
        'date_from': date_from.strip() if date_from else '',
        'date_to': date_to.strip() if date_to else '',
        'activities_and_societies': activities_and_societies.strip() if activities_and_societies else '',
        'description': description.strip() if description else '',
        'school_url': school_url.strip()
    }


def __parse_exp_item(exp_item):
    title = pu.safe_get_attr(exp_item.find('h3'), "text")
    location = '' # todo: to be implemented
    company_name = pu.safe_get_attr(exp_item.find('h4'), "text")
    company_link = exp_item.find('a')
    company_url = ''
    if company_link:
        company_url = company_link['href']
    time_items = exp_item.find_all('time')
    date_from = None
    date_to = None
    if len(time_items) > 0:
        date_from = pu.safe_get_attr(time_items[0], 'text')
    if len(time_items) > 1:
        date_to = pu.safe_get_attr(time_items[1], 'text')

    description = pu.safe_get_attr(exp_item.find('span'), 'text')
    return {
        'title': title.strip(),
        'location': location.strip(),
        'company_name': company_name.strip(),
        'company_url': company_url.strip(),
        'date_from': date_from.strip() if date_from else '',
        'date_to': date_to.strip() if date_to else '',
        'description': description.strip()
    }


def process_profile(linkedin_id:str, profile_html: str):
    '''
    The processor code follows the CoreSignal structure for the extracted data
    '''
    soup = BeautifulSoup(profile_html, 'html.parser')
    name = soup.find('h1', {'class': 'top-card-layout__title'}).text
    first_name = soup.find('meta', {'property': 'profile:first_name'})['content']
    last_name = soup.find('meta', {'property': 'profile:last_name'})['content']
    title = soup.find('meta', {'property': 'og:title'})['content']
    url = f'https://www.linkedin.com/in/{linkedin_id}/'
    profile_subheader_panel = soup.find('div', {'class': 'profile-info-subheader'})
    subheader_texts = profile_subheader_panel.find_all('span')
    location = subheader_texts[0].text
    industry = None # Looks like it is heuristically calculated by CoreSignal
    summary = soup.find('meta', {'name': 'description'})['content']
    logo_url = soup.find('img', {'class': 'top-card__profile-image'})['src']
    country = "" # todo: run geocoder after caching is done
    experience_list = soup.find('ul', {'class': 'experience__list'})
    if experience_list:
        experience_items = experience_list.find_all('li')
    else:
        experience_items = []
    experience_count = len(experience_items)
    member_shorthand_name = linkedin_id
    # todo: update once public profile with such fields is found
    member_awards_collection = []
    member_certifications_collection = []

    education_list = soup.find('ul', {'class': 'education__list'})
    if education_list:
        member_education_collection = [__parse_edu_item(x) for x in education_list.find_all('li')]
    else:
        member_education_collection = []

    member_experience_collection = [__parse_exp_item(x) for x in experience_items]
    # todo: update once public profile with such fields is found
    member_groups_collection = []
    member_languages_collection = []
    member_projects_collection = []

    # not available in public profile
    member_skills_collection = []
    member_volunteering_positions_collection = [] 
    member_websites_collection = []
    return {
        'name': pu.safe_strip(name),
        'first_name': pu.safe_strip(first_name),
        'last_name': pu.safe_strip(last_name),
        'title': pu.safe_strip(title),
        'url': pu.safe_strip(url),
        'location': pu.safe_strip(location),
        'industry': pu.safe_strip(industry),
        'summary': pu.safe_strip(summary),
        'logo_url': pu.safe_strip(logo_url),
        'last_updated': None,
        'country': pu.safe_strip(country),
        'experience_count': experience_count,
        'member_shorthand_name': pu.safe_strip(member_shorthand_name),
        'member_awards_collection': member_awards_collection,
        'member_certifications_collection': member_certifications_collection,
        'member_education_collection': member_education_collection,
        'member_experience_collection': member_experience_collection,
        'member_groups_collection': member_groups_collection,
        'member_languages_collection': member_languages_collection,
        'member_projects_collection': member_projects_collection,
        'member_skills_collection': member_skills_collection,
        'member_volunteering_positions_collection': member_volunteering_positions_collection,
        'member_websites_collection': member_websites_collection
    }
