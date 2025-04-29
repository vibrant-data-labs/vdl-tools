from io import BytesIO

from bs4 import BeautifulSoup, ResultSet
import re
import logging

from unstructured.partition.html import partition_html
from unstructured.partition.pdf import partition_pdf

from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links
from vdl_tools.shared_tools.tools.logger import logger as logger
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("unstructured").setLevel(logging.WARNING)
logging.getLogger('datasets').setLevel(logging.WARNING)



def parse_out_sections(elements, add_section_links=False):
    sections = []
    current_section = []
    last_type = None
    last_category_depth = 0

    def _break_for_title_change():
        return (
            current_type == "Title"
            and len(current_section) > 1
            and (
                last_type != "Title"
                or current_category_depth < last_category_depth
            )
        )

    def _break_for_list_end():
        return current_type != "ListItem" and last_type == "ListItem"

    for element in elements:
        current_category_depth = element.get("metadata", {}).get(
            "category_depth", last_category_depth
        )
        current_type = element["type"]
        current_links = element.get("metadata", {}).get("link_urls", [])
        if (
            add_section_links
            and len(current_links) > 0
            and current_category_depth == 0
        ):
            current_category_depth = last_category_depth
            current_section.append(element)
        elif _break_for_title_change() or _break_for_list_end():
            sections.append(current_section)
            current_section = [element]
        else:
            current_section.append(element)
        last_category_depth = current_category_depth
        last_type = current_type
    sections.append(current_section)
    return sections



def create_section_text(section, add_section_links=False):
    section_text = ""
    for element in section:
        element_text = clean_scraped_text(element["text"])
        if add_section_links:
            element_links = element.get("metadata", {}).get("link_urls", [])
            element_link_texts = element.get("metadata", {}).get("link_texts", [])
        else:
            element_links = []
            element_link_texts = []
        if element["type"] == "Title" and not element_links:
            category_depth = element.get("metadata", {}).get(
                "category_depth", 0
            )
            hashes = "#" * (category_depth + 1)
            section_text += f"\n{hashes} {element_text}\n"
        elif element["type"] == "ListItem":
            section_text += f"* {element_text}\n"
        elif element["type"] in {"NarrativeText", "UncategorizedText"}:
            section_text += f"\n{element_text}  \n"
        elif element_links and element_link_texts:
            for link, link_text in zip(element_links, element_link_texts):
                markdown_link_text = f"[{link_text}]({link})"
                element_text = element_text.replace(
                    link_text, markdown_link_text
                )
                section_text += f"\n{element_text}\n"
    return section_text.strip()


def create_page_text(elements, add_section_links=False):
    sections = parse_out_sections(elements)
    page_text = ""
    for section in sections:
        page_text += create_section_text(
            section,
            add_section_links=add_section_links,
        ) + "\n\n"
    return page_text.strip()


def get_page_text(url, html, add_section_links=False):
    if isinstance(html, bytes):
        return ""

    if url.endswith('pdf'):
        elements = partition_pdf(file=BytesIO(html))
        page_text = create_page_text([x.to_dict() for x in elements])
        return page_text

    html = html.replace("<strong>", "<b>").replace("</strong>", "</b>")
    html = html.replace("<em>", "<i>").replace("</em>", "</i>")
    try:
        elements = partition_html(text=html, skip_headers_and_footers=True)
        unstructured_page_text = create_page_text(
            [x.to_dict() for x in elements],
            add_section_links=add_section_links
        )

    except Exception as e:
        logger.error("Failed to get page text from Unstructured for %s", url)
        logger.error(e)
        unstructured_page_text = ""


    min_len = 500
    if add_section_links:
        min_len = 1000
    if len(unstructured_page_text) < min_len:
        logger.error("Failed to get page text from Unstructured for %s trying with fallback", url)
        page_text = clean_scraped_text(process_page_source(url, html))
        if page_text is None or len(page_text) < min_len:
            logger.error("Failed to get page text from %s", url)
    else:
        page_text = unstructured_page_text
    return page_text


def process_page_source(url: str, source: str):
    if 'https://challenges.cloudflare.com' in source:
        logger.warn(f'Looks like Cloudflare protection is enabled for {url}. Data may be invalid')

    try:
        html = source
        soup = BeautifulSoup(html, "lxml")

        logger.debug(f"scraping text from ({url})")
    except Exception as e:
        logger.warn(f"URL {url} not valid")
        logger.warn(e)
        return None
    else:
        paragraphs = soup.find_all('p')
        sections = soup.find_all('section')
        extract = " ".join([row.text for row in [*paragraphs, *sections]])
        extract = extract.replace("\xa0", " ")
        extract = extract.replace("\n", " ")
        pattern = re.compile(" {2,}")
        if extract.isspace():
            logger.warn(f"URL {url} is empty")
            return "empty"

        return re.sub(pattern, " ", str(extract))


def filter_anchors(url, links):
    urls = [
        url.replace('www.', ''),
        url[:-1] if url.endswith('/') else url,
        '/'
    ]

    res_links = []
    for x in links:
        matches = [s for s in urls if x.startswith(s) and len(x) > len(s)]
        if len(matches) == 0:
            continue

        # exclude links without protocol like '//www.example.com/contact' -> 'contact'
        if x.startswith('//'):
            split_parts = x[2:].split('/', maxsplit=1)
            split_parts = [s for s in split_parts if s]
            # skip the link if it is relative path to index like '/' or '//'
            if len(split_parts) <= 1:
                continue
            url_parts = split_parts[1]
        else:
            url_parts = x
        # exclude protocol if it is fully qualified url like 'https://example.com/contact' -> 'contact'
        url_parts = url_parts.split(
            '//')[1] if '//' in url_parts else url_parts
        # exclude the domain name and save only a relative path like 'example.com/contact' -> '/contact'
        url_parts = '/' + \
            '/'.join(url_parts.split('/')
                        [1:]) if x.startswith('http') else url_parts
        # skip the link if it is relative path to index like '/'
        if len(url_parts) == 1:
            continue

        # skip the link if it is a link to blob like '/promo.mp4' or '/splash.jpg'
        if '.' in url_parts:
            ext_parts = url_parts.split('.')
            if len(ext_parts) == 2 and ext_parts[1].lower() != 'html':
                continue

        res_links.append(url_parts)

    extracted_links = list(set(res_links))
    return extracted_links, res_links

def extract_website_links(
    url: str,
    website_content: str,
    subpage_type: str,
    max_per_subpath=6,
):
    try:
        html = website_content
        soup = BeautifulSoup(html, "lxml")

        logger.debug(f"getting links from ({url})")
    except Exception as e:
        logger.warning(f"URL {url} not valid")
        logger.warning(e)
        return None
    else:
        anchors = soup.find_all('a')
        links = [x.get('href') for x in anchors if x.get('href')]
        extracted_links, res_links = filter_anchors(url, links)

        if subpage_type == 'all':
            return extracted_links

        if subpage_type == 'about':
            return filter_links(
                links=list(set(res_links)),
                keep_paths=PATHS_TO_KEEP,
                max_per_subpath=max_per_subpath,
            )

        raise ValueError(f"Unknown filter strategy {subpage_type}")
