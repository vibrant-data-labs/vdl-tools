"""The rules for a funder's id: ``cb:<uuid>``, ``ein:<9 digits>`` or ``name:<slug>``, in that order of preference."""
import re

FUNDER_UID_RE = re.compile(r"^(cb:[0-9a-f-]{36}|ein:\d{9}|name:[a-z0-9-]+)$")


def slugify(s: str) -> str:
    """Build the deterministic slug used as a name-based funder_uid.

    Lowercase; '&' becomes 'and'; runs of non-alphanumerics collapse to
    '-'; leading and trailing '-' are stripped.

    Parameters
    ----------
    s : str
        Exact funder name.

    Returns
    -------
    str
        Slug, or an empty string for empty input.
    """
    if not s:
        return ""
    s = str(s).lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def normalize_ein(value):
    """Nine digits without the dash, or ``None`` when ``value`` is not an EIN string."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value.replace("-", "") if re.fullmatch(r"\d{2}-?\d{7}", value) else None
