"""Turn a 990's two name lines and DBA fields into a display name and a DBA.

A 990 carries the organization's name on two lines plus two doing-business-as
fields. Line 2 is free text: usually the rest of a name that did not fit on
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
CONNECTOR_AT_END_RE = re.compile(r"(AND|OF|FOR|THE|IN|ON|TO|AT|BY|WITH|A|&)$")
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
TRAILING_ACRONYM_RE = re.compile(r"\s*\(([^\s()]+)\)\s*$")
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
        text = re.sub(r"[-,;:(/)]\s*$", "", text).strip()
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
    pieces, seen = [], set()
    for piece in MARKER_RE.split(text):
        if MARKER_RE.fullmatch(piece.strip()):
            continue
        piece = _clean_head(_clean_tail(piece))
        if piece and _norm(piece) not in seen:
            pieces.append(piece)
            seen.add(_norm(piece))
    return "; ".join(pieces)


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


def _apply_rules(line1, line2, dba_1, dba_2, raw_dba_1, raw_dba_2):
    """Return ``(organization, dba)`` before the final clean-up."""
    width = len(line1)
    field_dba = dba_1 or dba_2

    if _norm(line2) == _norm(line1):
        return _clean_tail(line1), field_dba

    # A marker inside parentheses on line 1 -- "(FKA ZANMI)" -- names a DBA
    # and leaves line 2 to be read on its own.
    paren_dba = ""
    in_parens = MARKER_IN_PARENS_RE.search(line1)
    if in_parens:
        inner = in_parens.group(1)
        paren_dba = _clean_head(_clean_tail(inner[MARKER_RE.search(inner).end():]))
        line1 = _clean_tail(line1[: in_parens.start()] + line1[in_parens.end():])

    def with_paren_dba(dba):
        return "; ".join(d for d in (dba, paren_dba) if d)

    marker_in_line1 = MARKER_RE.search(line1)
    if marker_in_line1:
        fragment = line1[marker_in_line1.end():]
        marker_in_line2 = MARKER_RE.search(line2)
        if _norm(fragment) == _norm(line2):
            dba = line2
        elif marker_in_line2:
            dba = _split_dbas(line2[marker_in_line2.end():])
        else:
            dba = _split_dbas(_join(fragment, line2, width))
        return _clean_tail(line1[: marker_in_line1.start()]), with_paren_dba(dba)

    # DBA fields that hold the name itself, split the way the name lines are.
    raw_1, raw_2 = _norm(raw_dba_1), _norm(raw_dba_2)
    if {raw_1, raw_2} == {_norm(line1), _norm(line2)}:
        return _clean_tail(_join(line1, line2, width)), paren_dba
    if raw_2 == _norm(line2) and raw_1:
        dba = _clean_head(_clean_tail(_join(raw_dba_1, raw_dba_2)))
        return _clean_tail(_join(line1, line2, width)), with_paren_dba(dba)

    if _norm(line2) in {_norm(d) for d in (dba_1, dba_2) if d}:
        return _clean_tail(line1), with_paren_dba(line2)

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


def resolve_name_lines(name, name_secondary=None, dba_1=None, dba_2=None):
    """Return ``(organization, dba)`` for one org; ``dba`` is ``''`` when there is none."""
    line1 = (name or "").strip()
    line2 = (name_secondary or "").strip()
    raw_dba_1, raw_dba_2 = (dba_1 or "").strip(), (dba_2 or "").strip()
    dba_1 = _clean_dba_field(raw_dba_1, line1)
    dba_2 = _clean_dba_field(raw_dba_2, line1)

    # A closing "(METCO)" is the org's acronym, not part of its name.
    acronym = ""
    trailing = TRAILING_ACRONYM_RE.search(line2)
    if trailing and ACRONYM_RE.fullmatch(trailing.group(1)) and trailing.group(1) not in NOT_ACRONYMS:
        acronym = trailing.group(1)
        line2 = line2[: trailing.start()].strip()

    if line2:
        organization, dba = _apply_rules(line1, line2, dba_1, dba_2, raw_dba_1, raw_dba_2)
    else:
        organization, dba = _clean_tail(line1), dba_1 or dba_2
    dba = dba or acronym

    if organization.count("(") != organization.count(")"):
        organization = re.sub(r"[()]", "", organization)
    # Line 2 can cut a DBA short that the DBA field carries in full.
    for field in (dba_1, dba_2):
        if dba and "; " not in dba and _norm(field).startswith(_norm(dba)) and len(_norm(field)) > len(_norm(dba)):
            dba = field
            break
    organization, dba = " ".join(organization.split()), " ".join(dba.split())
    if _norm(dba) == _norm(organization):
        dba = ""
    return organization, dba
