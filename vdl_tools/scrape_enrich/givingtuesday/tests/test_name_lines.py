import pytest

from vdl_tools.scrape_enrich.givingtuesday.name_lines import resolve_name_lines

CUT_AT_FIELD_WIDTH = "RIVERBEND COLLEGE STUDENT GOVERNMEN"

# (name, name_secondary, dba_1, dba_2) -> (organization, dba)
CASES = {
    # Rule 4: line 2 is the rest of the name.
    "continuation": (
        ("NORTHFIELD YOUTH CLUBS OF HARLOW", "COUNTY INC", None, None),
        ("NORTHFIELD YOUTH CLUBS OF HARLOW COUNTY INC", ""),
    ),
    "word_cut_at_field_width": (
        (CUT_AT_FIELD_WIDTH, "GOVERNMENT INC", None, None),
        ("RIVERBEND COLLEGE STUDENT GOVERNMENT INC", ""),
    ),
    "same_shape_but_line_1_is_short": (
        ("HARLOW COUNCIL FOR ETHICS IN", "INTERNATIONAL AFFAIRS INC", None, None),
        ("HARLOW COUNCIL FOR ETHICS IN INTERNATIONAL AFFAIRS INC", ""),
    ),
    "word_repeated_across_the_lines": (
        ("NORTHGATE CRISIS CENTER", "CENTER INC", None, None),
        ("NORTHGATE CRISIS CENTER INC", ""),
    ),
    "repeated_word_inside_line_1_stays": (
        ("CEDAR STAGE WALLA WALLA", None, None, None),
        ("CEDAR STAGE WALLA WALLA", ""),
    ),
    "dba_field_rides_along": (
        ("HARLOW CHILDREN'S HOME", "AND FAMILY SERVICES", "BRIGHTTREE FAMILY SERVICES", None),
        ("HARLOW CHILDREN'S HOME AND FAMILY SERVICES", "BRIGHTTREE FAMILY SERVICES"),
    ),
    "acronym_in_parentheses": (
        ("METRO COUNCIL FOR", "EDUCATIONAL OPPORTUNITY INC (MCEO)", None, None),
        ("METRO COUNCIL FOR EDUCATIONAL OPPORTUNITY INC", "MCEO"),
    ),
    "parenthesised_word_is_not_an_acronym": (
        ("HARLOW KIDS INC", "(GROUP)", None, None),
        ("HARLOW KIDS INC", ""),
    ),
    # Rule 1: a marker or the DBA field says it is a DBA.
    "marker_starts_line_2": (
        ("RESTORE HARLOW", "DBA KIND WORKS", None, None),
        ("RESTORE HARLOW", "KIND WORKS"),
    ),
    "marker_mid_line_2": (
        ("HARLOW COUNTY MEDICAL SOCIETY", "FOUNDATION DBA CHAMPIONS FOR WELLNESS", None, None),
        ("HARLOW COUNTY MEDICAL SOCIETY FOUNDATION", "CHAMPIONS FOR WELLNESS"),
    ),
    "marker_ends_line_1_and_field_has_the_full_dba": (
        ("HARLOW PARTNERS FOR YOUTH INC DBA", "BIG FRIENDS OF GREATER HARL", "BIG FRIENDS OF GREATER HARLOW", None),
        ("HARLOW PARTNERS FOR YOUTH INC", "BIG FRIENDS OF GREATER HARLOW"),
    ),
    "formerly_known_as_across_the_lines": (
        ("THE FAMILY PLACE HARLOW FORMERLY KNOWN AS", "CHILD & FAMILY SUPPORT CENTER", None, None),
        ("THE FAMILY PLACE HARLOW", "CHILD & FAMILY SUPPORT CENTER"),
    ),
    "marker_in_parentheses_on_line_1": (
        ("SUMMIT LEARNING (FKA ZANTE)", "C/O PARTNERS IN HEALTH", None, None),
        ("SUMMIT LEARNING", "ZANTE"),
    ),
    "two_markers": (
        ("HARLOW REPERTORY THEATRE", "D/B/A THEATRE FOUR D/B/A BARKDALE THEATRE", None, None),
        ("HARLOW REPERTORY THEATRE", "THEATRE FOUR; BARKDALE THEATRE"),
    ),
    "line_2_matches_the_dba_field": (
        ("Chosen Dale Inc", "Enfold Shaker Museum", "Enfold Shaker Museum", None),
        ("Chosen Dale Inc", "Enfold Shaker Museum"),
    ),
    "dba_fields_mirror_the_split_name": (
        ("THE GEORGE E BRAUN UNITED", "FOUNDATION FOR SCIENCE INC", "THE GEORGE E BRAUN UNITED", "FOUNDATION FOR SCIENCE INC"),
        ("THE GEORGE E BRAUN UNITED FOUNDATION FOR SCIENCE INC", ""),
    ),
    "dba_split_across_both_fields": (
        ("Radio News Directors", "Association", "Radio Digital News", "Association"),
        ("Radio News Directors Association", "Radio Digital News Association"),
    ),
    "placeholder_in_the_dba_field": (
        ("HARLOW COMMUNITY ACTION", "COMMITTEE INC", "SEE SCHEDULE O", None),
        ("HARLOW COMMUNITY ACTION COMMITTEE INC", ""),
    ),
    "dba_field_carries_its_own_marker": (
        ("HARLOW HEALTH INITIATIVE INC", None, "DBA THE KENSEY FORUM", None),
        ("HARLOW HEALTH INITIATIVE INC", "THE KENSEY FORUM"),
    ),
    "everything_repeats_the_name": (
        ("SUGO FOUNDATION", "SUGO FOUNDATION", "SUGO FOUNDATION", None),
        ("SUGO FOUNDATION", ""),
    ),
    "no_line_2": (
        ("Harlow Audubon Society", None, "Harlow Audubon", None),
        ("Harlow Audubon Society", "Harlow Audubon"),
    ),
    # Rule 2: line 2 holds nothing to keep.
    "care_of": (
        ("BRING OMAR HOME INC", "C/O MICHAEL KANE", None, None),
        ("BRING OMAR HOME INC", ""),
    ),
    "care_of_mid_line": (
        ("SUMMIT ACADEMY TRANSITION HIGH SCHOOL -", "HARLOW - C/O SUMMIT MANAGEMENT", None, None),
        ("SUMMIT ACADEMY TRANSITION HIGH SCHOOL - HARLOW", ""),
    ),
    "po_box": (
        ("SHORE UP HARLOW INC", "P O BOX 430", None, None),
        ("SHORE UP HARLOW INC", ""),
    ),
    "person_with_a_role": (
        ("HARLOW FOUNDATION", "BRETT BARLOW PRESIDENT", None, None),
        ("HARLOW FOUNDATION", ""),
    ),
    "attention_line": (
        ("SEEK FIRST MINISTRIES", "ATTENTION HEAD OF SCHOOL", "HIGHLAND RIDGE ACADEMY", None),
        ("SEEK FIRST MINISTRIES", "HIGHLAND RIDGE ACADEMY"),
    ),
    "attention_that_continues_a_name": (
        ("CHILDREN AND ADULTS WITH", "ATTENTION DEFICIT DISORDER", None, None),
        ("CHILDREN AND ADULTS WITH ATTENTION DEFICIT DISORDER", ""),
    ),
    # Rule 3: line 1 is already a complete legal name.
    "alternate_name_after_legal_suffix": (
        ("B-Side Worldwide Inc", "Beacon Media", None, None),
        ("B-Side Worldwide Inc", "Beacon Media"),
    ),
    "surname_after_legal_suffix": (
        ("VALLEY CHRISTIAN SCHOOL INC", "THOMPSON", None, None),
        ("VALLEY CHRISTIAN SCHOOL INC", ""),
    ),
    "unit_line_is_part_of_the_name": (
        ("HARLOW AREA COUNCIL INC", "BOY SCOUTS OF AMERICA", None, None),
        ("HARLOW AREA COUNCIL INC BOY SCOUTS OF AMERICA", ""),
    ),
    "chapter_line_is_part_of_the_name": (
        ("Child Fellowship Inc", "GA Chapter", None, None),
        ("Child Fellowship Inc GA Chapter", ""),
    ),
    "numbered_post_is_part_of_the_name": (
        ("HARLOW WAR VETERANS INC", "VALLEY POST 603", None, None),
        ("HARLOW WAR VETERANS INC VALLEY POST 603", ""),
    ),
    "the_word_post_alone_is_not_a_unit": (
        ("STUDENT-LED INITIATIVES INC", "PLAN - POST-LANDFILL ACTION NETWORK", None, None),
        ("STUDENT-LED INITIATIVES INC", "PLAN - POST-LANDFILL ACTION NETWORK"),
    ),
    "lone_suffix_after_legal_suffix": (
        ("Harlow Community Housing Corporation", "Inc", None, None),
        ("Harlow Community Housing Corporation Inc", ""),
    ),
    "connecting_word_after_legal_suffix": (
        ("COMMUNITY ACTION INC", "OF CENTRAL HARLOW", None, None),
        ("COMMUNITY ACTION INC OF CENTRAL HARLOW", ""),
    ),
}


def test_the_cut_case_really_fills_the_name_field():
    assert len(CUT_AT_FIELD_WIDTH) == 35


@pytest.mark.parametrize("case", CASES)
def test_resolve_name_lines(case):
    inputs, expected = CASES[case]
    assert resolve_name_lines(*inputs) == expected
