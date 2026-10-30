"""Same-site paths from a vendor's links array, the input to subpage selection.

Today ``filter_anchors`` compares raw href strings against the URL we asked
for, by string prefix. A home page that redirects to another host (11% of
fetched home pages in the vendor pilot: renames, platform moves) then has all
of its links dropped, because they are on the host it landed on. This module
parses each link with ``urllib.parse`` and compares hosts exactly, against the
page as served after redirects. Its output feeds ``filter_links`` unchanged.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin, urlsplit

# A last path segment with one of these extensions, or none, is a page.
# Anything else (.pdf, .jpg, .mp4, .zip, ...) is a file and never a subpage.
PAGE_EXTENSIONS = ("htm", "html", "shtml", "php", "asp", "aspx", "jsp", "cfm")


def normalize_host(host_or_url: str | None) -> str:
    """Lowercase host without scheme, port, credentials or a leading ``www.``."""
    value = (host_or_url or "").strip()
    if not value:
        return ""
    host = urlsplit(value if "://" in value else f"//{value}").hostname or ""
    return host[4:] if host.startswith("www.") else host


def _is_page_path(path: str) -> bool:
    last = path.rstrip("/").rsplit("/", 1)[-1]
    if "." not in last:
        return True
    return last.rsplit(".", 1)[1].lower() in PAGE_EXTENSIONS


def same_site_paths(links: Iterable[str] | None, final_url: str) -> list[str]:
    """Paths of ``links`` on the same site as ``final_url``, sorted and unique.

    ``final_url`` is the page as served (Firecrawl's ``metadata.url``), or a
    bare host. Relative links resolve against it. A link is kept when its
    scheme is http or https, its host equals the final host once lowercased,
    stripped of port and of a leading ``www.``, and its path is a page other
    than the root. Query strings and fragments are dropped; a trailing slash
    is dropped so ``/about`` and ``/about/`` are one path.
    """
    base = final_url if "://" in (final_url or "") else f"https://{final_url}/"
    site = normalize_host(final_url)
    if not site:
        return []
    paths = set()
    for link in links or []:
        if not isinstance(link, str) or not link.strip():
            continue
        try:
            parts = urlsplit(urljoin(base, link.strip()))
        except ValueError:
            continue
        if parts.scheme not in ("http", "https") or normalize_host(parts.netloc) != site:
            continue
        path = parts.path.rstrip("/")
        if not path or not _is_page_path(path):
            continue
        paths.add(path)
    return sorted(paths)
