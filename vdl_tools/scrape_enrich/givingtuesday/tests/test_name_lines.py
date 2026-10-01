import pytest

from vdl_tools.scrape_enrich.givingtuesday.name_lines import resolve_name_lines

CUT_AT_FIELD_WIDTH = "RIVERBEND COLLEGE STUDENT GOVERNMEN"

# (name, name_secondary, dba_name) -> (organization, dba)
CASES = {
    # Rule 4: line 2 is the rest of the name.
    "continuation": (
        ("NORTHFIELD YOUTH CLUBS OF HARLOW", "COUNTY INC", None),
        ("NORTHFIELD YOUTH CLUBS OF HARLOW COUNTY INC", ""),
    ),
    "word_cut_at_field_width": (
        (CUT_AT_FIELD_WIDTH, "GOVERNMENT INC", None),
        ("RIVERBEND COLLEGE STUDENT GOVERNMENT INC", ""),
    ),
    "same_shape_but_line_1_is_short": (
        ("HARLOW COUNCIL FOR ETHICS IN", "INTERNATIONAL AFFAIRS INC", None),
        ("HARLOW COUNCIL FOR ETHICS IN INTERNATIONAL AFFAIRS INC", ""),
    ),
    "word_repeated_across_the_lines": (
        ("NORTHGATE CRISIS CENTER", "CENTER INC", None),
        ("NORTHGATE CRISIS CENTER INC", ""),
    ),
    "repeated_word_inside_line_1_stays": (
        ("CEDAR STAGE WALLA WALLA", None, None),
        ("CEDAR STAGE WALLA WALLA", ""),
    ),
    "dba_field_rides_along": (
        ("HARLOW CHILDREN'S HOME", "AND FAMILY SERVICES", "BRIGHTTREE FAMILY SERVICES"),
        ("HARLOW CHILDREN'S HOME AND FAMILY SERVICES", "BRIGHTTREE FAMILY SERVICES"),
    ),
    "acronym_in_parentheses": (
        ("METRO COUNCIL FOR", "EDUCATIONAL OPPORTUNITY INC (MCEO)", None),
        ("METRO COUNCIL FOR EDUCATIONAL OPPORTUNITY INC", "MCEO"),
    ),
    "parenthesised_word_is_not_an_acronym": (
        ("HARLOW KIDS INC", "(GROUP)", None),
        ("HARLOW KIDS INC", ""),
    ),
    # Rule 1: a marker or the DBA field says it is a DBA.
    "marker_starts_line_2": (
        ("RESTORE HARLOW", "DBA KIND WORKS", None),
        ("RESTORE HARLOW", "KIND WORKS"),
    ),
    "marker_mid_line_2": (
        ("HARLOW COUNTY MEDICAL SOCIETY", "FOUNDATION DBA CHAMPIONS FOR WELLNESS", None),
        ("HARLOW COUNTY MEDICAL SOCIETY FOUNDATION", "CHAMPIONS FOR WELLNESS"),
    ),
    "marker_ends_line_1_and_field_has_the_full_dba": (
        ("HARLOW PARTNERS FOR YOUTH INC DBA", "BIG FRIENDS OF GREATER HARL", "BIG FRIENDS OF GREATER HARLOW"),
        ("HARLOW PARTNERS FOR YOUTH INC", "BIG FRIENDS OF GREATER HARLOW"),
    ),
    "formerly_known_as_across_the_lines": (
        ("THE FAMILY PLACE HARLOW FORMERLY KNOWN AS", "CHILD & FAMILY SUPPORT CENTER", None),
        ("THE FAMILY PLACE HARLOW", "CHILD & FAMILY SUPPORT CENTER"),
    ),
    "marker_in_parentheses_on_line_1": (
        ("SUMMIT LEARNING (FKA ZANTE)", "C/O PARTNERS IN HEALTH", None),
        ("SUMMIT LEARNING", "ZANTE"),
    ),
    "two_markers": (
        ("HARLOW REPERTORY THEATRE", "D/B/A THEATRE FOUR D/B/A BARKDALE THEATRE", None),
        ("HARLOW REPERTORY THEATRE", "THEATRE FOUR; BARKDALE THEATRE"),
    ),
    "line_2_matches_the_dba_field": (
        ("Chosen Dale Inc", "Enfold Shaker Museum", "Enfold Shaker Museum"),
        ("Chosen Dale Inc", "Enfold Shaker Museum"),
    ),
    "dba_field_mirrors_the_split_name": (
        ("THE GEORGE E BRAUN UNITED", "FOUNDATION FOR SCIENCE INC", "THE GEORGE E BRAUN UNITED FOUNDATION FOR SCIENCE INC"),
        ("THE GEORGE E BRAUN UNITED FOUNDATION FOR SCIENCE INC", ""),
    ),
    "dba_field_ends_with_line_2": (
        ("Radio News Directors", "Association", "Radio Digital News Association"),
        ("Radio News Directors Association", "Radio Digital News Association"),
    ),
    "placeholder_in_the_dba_field": (
        ("HARLOW COMMUNITY ACTION", "COMMITTEE INC", "SEE SCHEDULE O"),
        ("HARLOW COMMUNITY ACTION COMMITTEE INC", ""),
    ),
    "dba_field_carries_its_own_marker": (
        ("HARLOW HEALTH INITIATIVE INC", None, "DBA THE KENSEY FORUM"),
        ("HARLOW HEALTH INITIATIVE INC", "THE KENSEY FORUM"),
    ),
    "everything_repeats_the_name": (
        ("SUGO FOUNDATION", "SUGO FOUNDATION", "SUGO FOUNDATION"),
        ("SUGO FOUNDATION", ""),
    ),
    "no_line_2": (
        ("Harlow Audubon Society", None, "Harlow Audubon"),
        ("Harlow Audubon Society", "Harlow Audubon"),
    ),
    # Rule 2: line 2 holds nothing to keep.
    "care_of": (
        ("BRING OMAR HOME INC", "C/O MICHAEL KANE", None),
        ("BRING OMAR HOME INC", ""),
    ),
    "care_of_mid_line": (
        ("SUMMIT ACADEMY TRANSITION HIGH SCHOOL -", "HARLOW - C/O SUMMIT MANAGEMENT", None),
        ("SUMMIT ACADEMY TRANSITION HIGH SCHOOL - HARLOW", ""),
    ),
    "po_box": (
        ("SHORE UP HARLOW INC", "P O BOX 430", None),
        ("SHORE UP HARLOW INC", ""),
    ),
    "person_with_a_role": (
        ("HARLOW FOUNDATION", "BRETT BARLOW PRESIDENT", None),
        ("HARLOW FOUNDATION", ""),
    ),
    "attention_line": (
        ("SEEK FIRST MINISTRIES", "ATTENTION HEAD OF SCHOOL", "HIGHLAND RIDGE ACADEMY"),
        ("SEEK FIRST MINISTRIES", "HIGHLAND RIDGE ACADEMY"),
    ),
    "attention_that_continues_a_name": (
        ("CHILDREN AND ADULTS WITH", "ATTENTION DEFICIT DISORDER", None),
        ("CHILDREN AND ADULTS WITH ATTENTION DEFICIT DISORDER", ""),
    ),
    # Rule 3: line 1 is already a complete legal name.
    "alternate_name_after_legal_suffix": (
        ("B-Side Worldwide Inc", "Beacon Media", None),
        ("B-Side Worldwide Inc", "Beacon Media"),
    ),
    "surname_after_legal_suffix": (
        ("VALLEY CHRISTIAN SCHOOL INC", "THOMPSON", None),
        ("VALLEY CHRISTIAN SCHOOL INC", ""),
    ),
    "unit_line_is_part_of_the_name": (
        ("HARLOW AREA COUNCIL INC", "BOY SCOUTS OF AMERICA", None),
        ("HARLOW AREA COUNCIL INC BOY SCOUTS OF AMERICA", ""),
    ),
    "chapter_line_is_part_of_the_name": (
        ("Child Fellowship Inc", "GA Chapter", None),
        ("Child Fellowship Inc GA Chapter", ""),
    ),
    "numbered_post_is_part_of_the_name": (
        ("HARLOW WAR VETERANS INC", "VALLEY POST 603", None),
        ("HARLOW WAR VETERANS INC VALLEY POST 603", ""),
    ),
    "the_word_post_alone_is_not_a_unit": (
        ("STUDENT-LED INITIATIVES INC", "PLAN - POST-LANDFILL ACTION NETWORK", None),
        ("STUDENT-LED INITIATIVES INC", "PLAN - POST-LANDFILL ACTION NETWORK"),
    ),
    "lone_suffix_after_legal_suffix": (
        ("Harlow Community Housing Corporation", "Inc", None),
        ("Harlow Community Housing Corporation Inc", ""),
    ),
    "connecting_word_after_legal_suffix": (
        ("COMMUNITY ACTION INC", "OF CENTRAL HARLOW", None),
        ("COMMUNITY ACTION INC OF CENTRAL HARLOW", ""),
    ),
}


def test_the_cut_case_really_fills_the_name_field():
    assert len(CUT_AT_FIELD_WIDTH) == 35


@pytest.mark.parametrize("case", CASES)
def test_resolve_name_lines(case):
    inputs, expected = CASES[case]
    assert resolve_name_lines(*inputs) == expected
