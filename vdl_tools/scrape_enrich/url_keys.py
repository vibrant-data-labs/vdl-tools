"""The two keys a website URL is reduced to, and the form it is fetched in.
Use these; don't write another.

- ``extract_website_name``: the PAGE key (keeps ``www.``, case and path).
  Keys every row of the scrape cache, so its output must never change.
  ``https://www.Example.org/about?ref=x`` -> ``www.Example.org_about``
- ``website_domain``: the DOMAIN key, for matching, duplicates, redirects and
  the liveness cache. Repairs messy website fields; "" for platforms, free-mail
  and placeholders. ``https://www.Example.org/about`` -> ``example.org``,
  ``info@example.org`` -> ``example.org``, ``facebook.com/x`` -> ``""``
- ``ensure_url_scheme``: the FETCH form, a scheme added when missing and
  lowercased, everything else as given. ``WWW.Example.org/about`` ->
  ``https://WWW.Example.org/about``
- ``clean_website_path``: a SUBPAGE's part of its page key, appended to the
  home page's key with a slash. ``/pages/contact/`` -> ``pages_contact``, so
  the row is ``www.Example.org/pages_contact``. ``check_is_single_page_websites``
  says which hosts (medium.com, linkedin.com, ...) get no subpages at all.
"""

import re
from urllib.parse import urlparse

from vdl_tools.shared_tools.tools.logger import logger

# Platforms and mail providers identify no org: never match two orgs on them.
PLATFORM_DOMAINS = {
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "medium.com", "crunchbase.com", "angel.co", "wellfound.com",
    "wikipedia.org", "en.wikipedia.org", "google.com", "sites.google.com",
    "docs.google.com", "linktr.ee", "notion.so", "github.com",
    "apps.apple.com", "play.google.com",
}

FREE_MAIL = {
    "gmail.com", "yahoo.com", "aol.com", "hotmail.com", "outlook.com", "msn.com",
    "comcast.net", "att.net", "sbcglobal.net", "verizon.net", "bellsouth.net",
    "cox.net", "juno.com", "earthlink.net", "icloud.com", "me.com", "mac.com",
    "live.com", "ymail.com", "protonmail.com",
}

_BROKEN_SCHEME_RE = re.compile(r"^\s*https?:\s*/*\s*")  # "https: eoydc.org"
_WWW_RE = re.compile(r"^(?:w{2,}\d*\.)+")  # "www.", "wwww.", "www2.", "www.www."


def extract_website_name(value: str) -> str:
    '''
    Clean the website name

    https://google.com -> google.com
    https://google.com/ -> google.com
    https://somewebsite.com:port -> somewebsite.com_port
    https://coolwebsite.com/inner/page/1 -> coolwebsite.com_inner_page_1
    https://anotherwebsite.com?query=my-query -> anotherwebsite.com
    '''
    if not value:
        return None
    try:
        if "//" in value:
            website = value.split('//')[1].strip()
        else:
            website = value
        website = website.split('?')[0].strip()
        # remove trailing slash
        website = website[:-1] if website.endswith('/') else website
        return website.replace('/', '_').replace(':', '_')
    except Exception as ex:
        logger.warning(f'Error getting website name: {value}')
        return None


def ensure_url_scheme(url: str) -> str:
    """Ensure URL has a scheme (https:// by default).

    This is necessary for urljoin to work correctly when constructing
    internal page URLs from relative paths.
    """
    if not url:
        return url
    # Scheme check must be case-insensitive: 990-filing websites arrive as
    # HTTPS://WWW.EXAMPLE.ORG and a case-sensitive check double-prefixes
    # them (https://HTTPS://...). Normalize the scheme to lowercase too.
    lowered = url.lower()
    if not lowered.startswith(('http://', 'https://')):
        return f'https://{url}'
    scheme_len = lowered.index('://') + 3
    return url[:scheme_len].lower() + url[scheme_len:]


def website_domain(url) -> str:
    """The domain that identifies the org behind ``url``, or '' when it identifies nothing."""
    if not isinstance(url, str):
        return ""
    # GT has "www. tworiversymca. org"; hosts never contain whitespace.
    url = re.sub(r"\s+", "", url.lower())
    url = _BROKEN_SCHEME_RE.sub("", url)
    if not url:
        return ""
    host = urlparse(url if "://" in url else f"http://{url}").netloc
    host = host.rsplit("@", 1)[-1]  # "info@example.org" -> the domain
    host = _WWW_RE.sub("", host.split(":")[0])
    # No dot ("na", "none"): a placeholder.
    if "." not in host or host in PLATFORM_DOMAINS or host in FREE_MAIL:
        return ""
    return host


# if the page from one of these websites is to be scraped
# then the scraper should only take the first page
SINGLE_PAGE_WEBSITES = [
    'medium.com',
    'facebook.com',
    'linkedin.com',
    'linktr.ee',
    'about.me',
    'wikipedia.org',
    'github.com',
    'scholar.google.com/',
    'meetup.com',
]


BAD_URL_PATH_CHARS = '/?=&#@'


def check_is_single_page_websites(url: str, single_page_websites: list = []):
    return len([x for x in [*SINGLE_PAGE_WEBSITES, *single_page_websites] if x in url.lower()]) > 0


def clean_website_path(value: str) -> str:
    '''
    Clean the website subpath: the part of a page's cache key after the home key

    /pages/contact -> pages_contact
    /pages/contact/ -> pages_contact
    /about-us -> about-us
    /page?page_title=about-us -> page_page_title_about-us
    '''
    if 'www' in value:
        logger.warning(f"Invalid value because path contains 'www': {value}")
        return None
    try:
        path = value.split(
            '/', maxsplit=1)[1] if value.startswith('/') else value
        path = path[:-1] if path.endswith('/') else path

        for c in BAD_URL_PATH_CHARS:
            path = path.replace(c, '_')

        return path
    except Exception as ex:
        logger.warning(f'Error cleaning website path: {value}')
        return None
