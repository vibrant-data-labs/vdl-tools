from bs4 import BeautifulSoup
import vdl_tools.linkedin.processors.processor_utils as pu

__image_attrs = [
    'src',
    'data-delayed-url',
    'data-ghost-url',
]


def __item_from_core_section(core_section, test_id, nested_item = 'dd'):
    row = core_section.find('div', { 'data-test-id': test_id})
    if row:
        return pu.safe_get_attr(row.find(nested_item), 'text')
    return None


def process_org(linkedin_id:str, org_html: str):
    soup = BeautifulSoup(org_html, 'html.parser')

    title = pu.safe_get_attr(soup.find('h1'), "text")
    url = f'https://www.linkedin.com/company/{linkedin_id}'
    company_image_element = soup.find('img', { 'class': 'top-card-layout__entity-image'})
    company_image = None
    if company_image_element:
        for attr in __image_attrs:
            if attr in company_image_element.attrs:
                company_image = company_image_element[attr]
                break

    if company_image:
        company_image = company_image.replace('&amp;', '&').strip()

    summary = pu.safe_get_attr(soup.find('h4', { "class": 'top-card-layout__second-subline'}), "text")

    core_section = soup.find('div', { 'class': 'core-section-container__content'})
    description = pu.safe_get_attr(core_section.find('p'), "text")

    website = __item_from_core_section(core_section, 'about-us__website', 'a')
    industry = __item_from_core_section(core_section, 'about-us__industry')
    company_size = __item_from_core_section(core_section, 'about-us__size')
    hq_location = __item_from_core_section(core_section, 'about-us__headquarters')
    company_type = __item_from_core_section(core_section, 'about-us__organizationType')
    founded = __item_from_core_section(core_section, 'about-us__foundedOn')
    specialties = __item_from_core_section(core_section, 'about-us__specialties')

    location_section = soup.find('section', { 'class': 'locations'})
    locations = []
    if location_section:
        location_list = location_section.find_all('li')
        for location in location_list:
            loc_txt: str = pu.safe_get_attr(location.find('div'), 'text')
            loc_txt = ' '.join([x.strip() for x in loc_txt.splitlines()]).strip()
            locations.append(loc_txt)
    
    return {
        "name": pu.safe_strip(title),
        "image": company_image,
        "url": url,
        "summary": pu.safe_strip(summary),
        "about": pu.safe_strip(description),
        "website": pu.safe_strip(website),
        "industry": pu.safe_strip(industry),
        "specialties": pu.safe_strip(specialties),
        "company_size": pu.safe_strip(company_size),
        "hq location": pu.safe_strip(hq_location),
        "company_type": pu.safe_strip(company_type),
        "founded": pu.safe_strip(founded),
        "locations": locations
    }