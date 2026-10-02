"""Turn a 990's two name lines and DBA field into a display name and a DBA.

A 990 carries the organization's name on two lines plus doing-business-as
lines, which the datamart joins into one ``dba_name``. Line 2 is free text: usually the rest of a name that did not fit on
line 1, sometimes a DBA, a care-of line, or a second name. Using line 1 alone
truncates about one name in six.

``resolve_name_lines`` applies four rules, first match wins:

1. A DBA marker (``DBA``, ``FKA``, ``FORMERLY``...) or a match with the DBA
   field says line 2, or part of it, is a DBA.
2. Line 2 holds nothing to keep: a care-of line, a PO box, a person with a
   role, or a repeat of line 1.
3. Line 1 already ends in a legal suffix (``INC``, ``LLC``...), so line 2 is
   kept as an alternate name -- unless it names a local unit (council,
   chapter...), is a lone suffix or place word, or starts with a connecting
   word. Those are part of the name.
4. Everything else is the rest of the name and is appended.

The rules were tuned and measured on three rounds of blind-labeled samples of
Giving Tuesday orgs: about 96% of orgs with a second line come out with both
name and DBA right. What they cannot catch is a second complete name with no
marker (``GOVERNOR DUMMER ACADEMY`` / ``THE GOVERNOR'S ACADEMY``), which is
appended.
"""

import re

MARKER = (
    r"(?<![A-Za-z])(FORMERLY KNOWN AS|DOING BUSINESS AS|F/K/N|F/K/A|FORMELY"
    r"|T/A|D/B/A|D B A|A/K/A|DBA|AKA|FKA|FORMERLY)(?![A-Za-z])"
)
MARKER_RE = re.compile(MARKER, re.IGNORECASE)
MARKER_AT_END_RE = re.compile(MARKER + r"\s*$", re.IGNORECASE)
MARKER_AT_START_RE = re.compile(r"\s*" + MARKER, re.IGNORECASE)
MARKER_IN_PARENS_RE = re.compile(r"\(([^()]*" + MARKER + r"[^()]*)\)", re.IGNORECASE)

# The IRS name field is 35 characters wide on older filings and 40 on newer
# ones. A line 1 of exactly that length may end mid-word.
NAME_FIELD_WIDTHS = (35, 40)

LEGAL_SUFFIX_AT_END_RE = re.compile(r"\b(INC|INCORPORATED|CORP|CORPORATION|LLC|LTD)\.?\s*$")
CONNECTOR_AT_START_RE = re.compile(r"(OF|AND|FOR|IN|ON|AT|TO|&|WITH|BY|THE)(?![A-Za-z])")
CONNECTOR_AT_END_RE = re.compile(r"(?<![A-Za-z])(AND|OF|FOR|THE|IN|ON|TO|AT|BY|WITH|A)$|&$")
LONE_SUFFIX_OR_PLACE = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "LLC", "LTD", "CO", "USA",
    "AMERICA", "INTERNATIONAL", "FOUNDATION", "TRUST", "FUND",
}
UNIT_LINE_RE = re.compile(
    r"\b(COUNCIL|CHAPTER|SECTION|UNIT|AUXILIARY|DIVISION|DISTRICT|PTA|PTO|PTSA)\b"
    r"|POST\s*#?\s*\d+|\d+\s+POST|\bSCOUTING AMERICA\b"
    r"|\bBOY SCOUTS OF AMERICA\b|\bGIRL SCOUTS\b"
)

CARE_OF_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"(?<![A-Za-z])C/O(?![A-Za-z])",
        r"\bCARE OF\b",
        r"\bATTN\b",
        r"\bP\.?\s?O\.?\s?BOX\b",
        r"\bPOST OFFICE BOX\b",
    )
]
CARE_OF_PREFIXES = ("CO ", "C/0", "ATTENTION:", "%")
PERSON_ROLE_RE = re.compile(
    r"\b(TRUSTEE|TTEE|EXECUTOR|PRESIDENT|DIRECTOR|TREASURER|SECRETARY)\b"
)

PLACEHOLDER_DBAS = {"none", "na", "same", "sameasabove", "notapplicable", "n"}
TRAILING_ACRONYM_RE = re.compile(r"\(([^\s()]+)\)\s*$")
ACRONYM_RE = re.compile(r"[A-Z0-9&\-./]{2,10}")
NOT_ACRONYMS = {"GROUP", "INC", "LLC", "USA", "CORP", "NFP", "THE", "AND", "TRUST", "FUND"}
TOKEN_SPLIT_RE = re.compile(r"[\s/-]+")


def _upper(text):
    return " ".join(text.upper().split())


def _norm(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _clean_tail(text):
    """Drop a trailing DBA marker and trailing punctuation."""
    text = text.strip()
    while True:
        before = text
        text = MARKER_AT_END_RE.sub("", text).strip()
        text = re.sub(r"[-,;:(/)\s]+$", "", text)
        if text == before:
            return text


def _clean_head(text):
    """Drop a leading DBA marker, ``KNOWN AS`` / ``AS``, and stray punctuation."""
    text = re.sub(r"\)\s*$", "", text.strip()).strip()
    while True:
        before = text
        text = re.sub(r"^[-,;:()]+\s*", "", text).strip()
        marker = MARKER_AT_START_RE.match(text)
        if marker:
            text = text[marker.end():].strip()
        text = re.sub(r"^(KNOWN AS|AS)\s+", "", text, flags=re.IGNORECASE).strip()
        if text == before:
            return text


def _split_dbas(text):
    """``X D/B/A Y`` -> ``X; Y``, markers removed and repeats dropped."""
    pieces = [_clean_head(_clean_tail(p)) for p in MARKER_RE.split(text) if not MARKER_RE.fullmatch(p.strip())]
    return _join_dbas(*pieces)


def _join(head, tail, line1_width=None):
    """Append ``tail`` to ``head``, mending the seam between the two lines.

    A word repeated across the seam is kept once (``...BIBLE CAMP`` /
    ``CAMP``). When line 1 filled the name field, a word cut at its end is
    replaced by the whole word that starts line 2 (``...STUDENT GOVERNM`` /
    ``GOVERNMENT INC``).
    """
    head, tail = head.strip(), tail.strip()
    if not head or not tail:
        return head or tail
    last = TOKEN_SPLIT_RE.split(head)[-1]
    first = TOKEN_SPLIT_RE.split(tail)[0]
    if last and _upper(last).strip(".,") == _upper(first).strip(".,"):
        head = head[: len(head) - len(last)].rstrip()
    elif (
        line1_width in NAME_FIELD_WIDTHS
        and len(last) >= 2
        and len(first) > len(last)
        and _upper(first).startswith(_upper(last))
    ):
        head = head[: len(head) - len(last)].rstrip()
    return f"{head} {tail}".strip()


def _join_dbas(*dbas):
    """Join DBAs with ``; ``, dropping blanks and repeats."""
    kept = {}
    for dba in dbas:
        if dba and _norm(dba) not in kept:
            kept[_norm(dba)] = dba
    return "; ".join(kept.values())


def _restated(cut, full):
    """True when ``full`` restates ``cut``, an alias cut off at the end of line 1."""
    return bool(_norm(cut)) and _norm(full).startswith(_norm(cut))


def _pop_paren_marker(line1):
    """``FOO (FKA BAR)`` -> ``('FOO', 'BAR')``; ``(line1, '')`` when there is none."""
    in_parens = MARKER_IN_PARENS_RE.search(line1)
    if not in_parens:
        return line1, ""
    inner = in_parens.group(1)
    dba = _clean_head(_clean_tail(inner[MARKER_RE.search(inner).end():]))
    return _clean_tail(line1[: in_parens.start()] + line1[in_parens.end():]), dba


def _marker_after_name(line1):
    """The first DBA marker on line 1 that follows some name ("FORMERLY
    INCARCERATED..." is a name, not a marker)."""
    for marker in MARKER_RE.finditer(line1):
        if _clean_tail(line1[: marker.start()]):
            return marker
    return None


def _clean_dba_field(value, line1):
    """A DBA field's value, or '' when it is a placeholder or repeats line 1."""
    value = _clean_head(value or "")
    key = _norm(value)
    if key.startswith("seeschedule") or key in PLACEHOLDER_DBAS or key == _norm(line1):
        return ""
    return value


def _is_care_of_or_person(line2_upper, line1_upper):
    if any(p.search(line2_upper) for p in CARE_OF_RES):
        return True
    if line2_upper.startswith(CARE_OF_PREFIXES) or PERSON_ROLE_RE.search(line2_upper):
        return True
    # "ATTENTION DEFICIT..." after "...ADULTS WITH" is the rest of a name.
    continues_line1 = CONNECTOR_AT_END_RE.search(line1_upper) or line1_upper.endswith("-")
    return line2_upper.startswith("ATTENTION ") and not continues_line1


def _is_part_of_name(line2_upper):
    """Line 2 continues the name even though line 1 ends in a legal suffix."""
    return bool(
        CONNECTOR_AT_START_RE.match(line2_upper)
        or line2_upper.replace(".", "") in LONE_SUFFIX_OR_PLACE
        or UNIT_LINE_RE.search(line2_upper)
        or line2_upper[:1].isdigit()
    )


def _apply_rules(line1, line2, field_dba, raw_dba):
    """Return ``(organization, dba)`` before the final clean-up."""
    width = len(line1)

    if _norm(line2) == _norm(line1):
        return _clean_tail(line1), field_dba

    # A marker inside parentheses on line 1 -- "(FKA ZANMI)" -- names a DBA
    # and leaves line 2 to be read on its own.
    line1, paren_dba = _pop_paren_marker(line1)

    def with_paren_dba(dba):
        return _join_dbas(dba, paren_dba)

    marker_in_line1 = _marker_after_name(line1)
    if marker_in_line1:
        fragment = line1[marker_in_line1.end():]
        if _norm(fragment) == _norm(line2):
            dba = line2
        elif MARKER_AT_START_RE.match(line2.lstrip("( ")) and _restated(fragment, _split_dbas(line2)):
            # "...(DBA CAMP" / "D/B/A CAMP CONQUEST": line 2 restates the cut alias.
            dba = _split_dbas(line2)
        else:
            dba = _split_dbas(_join(fragment, line2, width))
        return _clean_tail(line1[: marker_in_line1.start()]), with_paren_dba(dba)

    # A DBA field that holds the name itself, is line 2, or ends with line 2.
    if _norm(raw_dba) == _norm(line1) + _norm(line2):
        return _clean_tail(_join(line1, line2, width)), paren_dba
    field = _norm(field_dba)
    if field and field == _norm(line2):
        return _clean_tail(line1), with_paren_dba(line2)
    if field and field.endswith(_norm(line2)):
        return _clean_tail(_join(line1, line2, width)), with_paren_dba(_clean_tail(field_dba))

    marker_in_line2 = MARKER_RE.search(line2)
    if marker_in_line2:
        if MARKER_AT_START_RE.match(line2.lstrip("( ")):
            return _clean_tail(line1), with_paren_dba(_split_dbas(line2))
        before = re.sub(r"\(\s*$", "", line2[: marker_in_line2.start()])
        organization = _clean_tail(_join(line1, before, width))
        return organization, with_paren_dba(_split_dbas(line2[marker_in_line2.start():]))

    line1_upper, line2_upper = _upper(line1), _upper(line2)
    if _is_care_of_or_person(line2_upper, line1_upper):
        # Text before a mid-line "C/O" still belongs to the name.
        starts = [m.start() for m in (p.search(line2) for p in CARE_OF_RES) if m]
        lead = line2[: min(starts)] if starts else ""
        return _clean_tail(_join(line1, lead, width)), with_paren_dba(field_dba)

    if LEGAL_SUFFIX_AT_END_RE.search(line1_upper) and not _is_part_of_name(line2_upper):
        # A one-word line 2 here is usually a surname, not an alternate name.
        one_word = len([t for t in TOKEN_SPLIT_RE.split(line2) if t]) == 1
        return _clean_tail(line1), with_paren_dba(field_dba if one_word else line2)

    return _clean_tail(_join(line1, line2, width)), with_paren_dba(field_dba)


def resolve_name_lines(name, name_secondary=None, dba_name=None):
    """Return ``(organization, dba)`` for one org; ``dba`` is ``''`` when there is none."""
    line1 = (name or "").strip()
    line2 = (name_secondary or "").strip()
    raw_dba = (dba_name or "").strip()
    field_dba = _clean_dba_field(raw_dba, line1)

    # A closing "(METCO)" is the org's acronym, not part of its name.
    acronym = ""
    trailing = TRAILING_ACRONYM_RE.search(line2)
    if trailing and ACRONYM_RE.fullmatch(trailing.group(1)) and trailing.group(1) not in NOT_ACRONYMS:
        acronym = trailing.group(1)
        line2 = line2[: trailing.start()].strip()

    if line2:
        organization, dba = _apply_rules(line1, line2, field_dba, raw_dba)
    else:
        # Without line 2, a marker on line 1 still separates a DBA.
        line1, paren_dba = _pop_paren_marker(line1)
        marker = _marker_after_name(line1)
        line1_dba = _split_dbas(line1[marker.start():]) if marker else ""
        if _restated(line1_dba, field_dba):
            line1_dba = ""
        organization = _clean_tail(line1[: marker.start()] if marker else line1)
        dba = _join_dbas(line1_dba, paren_dba, field_dba)
    dba = dba or acronym

    if organization.count("(") != organization.count(")"):
        organization = re.sub(r"[()]", "", organization)
    # Line 2 can cut a DBA short that the DBA field carries in full.
    if dba and "; " not in dba and _norm(field_dba).startswith(_norm(dba)) and len(_norm(field_dba)) > len(_norm(dba)):
        dba = field_dba
    organization, dba = " ".join(organization.split()), " ".join(dba.split())
    if _norm(dba) == _norm(organization):
        dba = ""
    return organization, dba
