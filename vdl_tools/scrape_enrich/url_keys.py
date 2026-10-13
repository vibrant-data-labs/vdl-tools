"""The two keys a website URL is reduced to. Use these; don't write another.

``extract_website_name`` — the PAGE key. Names one fetched page: keeps
``www.``, case and the path (``/`` and ``:`` become ``_``). Every row of the
Postgres scrape cache is keyed by it, so its output must never change.

    https://www.Example.org/about?ref=x -> www.Example.org_about

``website_domain`` — the DOMAIN key. Names the site an organization is behind,
for matching records across sources, finding duplicates and redirects, and the
shared liveness cache. Repairs what raw website fields carry, and returns ""
when the value cannot identify an org (a platform, a mail provider, a
placeholder), so two orgs never match on facebook.com or gmail.com.

    https://www.Example.org/about?ref=x -> example.org
    www. tworiversymca. org             -> tworiversymca.org
    info@example.org                    -> example.org
    https://www.facebook.com/exampleorg -> ""
"""

import re
from urllib.parse import urlparse

from vdl_tools.shared_tools.tools.logger import logger

# Shared-platform domains identify a platform, not an org. A customer URL
# like linkedin.com/company/woodgrove-capital must never domain-match every
# org whose listed website is its LinkedIn page.
PLATFORM_DOMAINS = {
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "medium.com", "crunchbase.com", "angel.co", "wellfound.com",
    "wikipedia.org", "en.wikipedia.org", "google.com", "sites.google.com",
    "docs.google.com", "linktr.ee", "notion.so", "github.com",
    "apps.apple.com", "play.google.com",
}

# A website field holding one of these identifies a mail provider, not an org.
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


def website_domain(url) -> str:
    """The domain that identifies the org behind ``url``, or '' when it identifies nothing."""
    if not isinstance(url, str):
        return ""
    # Hosts never contain whitespace; GT has "www. tworiversymca. org".
    url = re.sub(r"\s+", "", url.lower())
    url = _BROKEN_SCHEME_RE.sub("", url)
    if not url:
        return ""
    host = urlparse(url if "://" in url else f"http://{url}").netloc
    host = host.rsplit("@", 1)[-1]  # "info@example.org" -> the domain
    host = _WWW_RE.sub("", host.split(":")[0])
    # A key without a dot ("na", "none" — both in the GT data) identifies nothing.
    if "." not in host or host in PLATFORM_DOMAINS or host in FREE_MAIL:
        return ""
    return host
