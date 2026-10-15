import pandas as pd
import pytest

from vdl_tools.scrape_enrich import display_name
from vdl_tools.scrape_enrich.display_name import (
    PROMPT,
    PROMPT_FILED_NAME,
    PROMPT_SHARED_SITE,
    NameChoice,
    build_input_text,
    drop_legal_suffix,
    normalize_name,
    pick_display_names,
    restore_apostrophes,
)

ROW = {
    "Organization": "NORTHFIELD YOUTH CLUBS OF HARLOW COUNTY INC",
    "businessname1": "NORTHFIELD YOUTH CLUBS OF HARLOW",
    "businessname2": "COUNTY INC",
    "DBA": "HARLOW YOUTH CLUBS",
    "dba_name": None,
    "website": "harlowyouth.org",
    "summary": "After-school clubs for Harlow County teens.",
    "text": "word " * 1200,
}


def test_input_has_a_line_per_field_with_a_value():
    text = build_input_text(
        ROW, "Organization", dba_col="DBA", website_col="website", summary_col="summary"
    )
    assert text.splitlines() == [
        "Legal name: NORTHFIELD YOUTH CLUBS OF HARLOW COUNTY INC",
        "DBA: HARLOW YOUTH CLUBS",
        "Website: harlowyouth.org",
        "Website summary: After-school clubs for Harlow County teens.",
    ]


def test_other_organizations_on_the_website_are_listed_after_it():
    row = {**ROW, "siblings": ["Harlow Youth Clubs - North", "Harlow Youth Clubs - South"]}
    text = build_input_text(
        row, "Organization", website_col="website", siblings_col="siblings", max_siblings=1
    )
    assert text.splitlines()[-2:] == [
        "Website: harlowyouth.org",
        "Other organizations using this website: Harlow Youth Clubs - North (and 1 more)",
    ]
    assert "Other organizations" not in build_input_text(row, "Organization", website_col="website")


def test_each_prompt_variant_adds_only_what_it_is_for():
    assert PROMPT_SHARED_SITE.startswith(PROMPT[:200])
    assert "uses the same website" not in PROMPT
    assert (
        "uses the same website" in PROMPT_SHARED_SITE
        or "use the same website" in PROMPT_SHARED_SITE
    )
    assert "Keep every word of the filed name" in PROMPT_FILED_NAME


def test_filed_lines_and_raw_dba_are_sent_only_when_asked_for():
    text = build_input_text(
        ROW,
        "Organization",
        dba_col="DBA",
        name_line_cols=("businessname1", "businessname2"),
        dba_field_col="dba_name",
    )
    assert "Name line 1: NORTHFIELD YOUTH CLUBS OF HARLOW" in text
    assert "Name line 2: COUNTY INC" in text


def test_scraped_text_is_cut_to_the_word_limit():
    text = build_input_text(ROW, "Organization", text_col="text", max_text_words=1000)
    assert len(text.splitlines()[-1].split()) == 1000 + 2


def test_missing_optional_columns_are_skipped():
    text = build_input_text({"Organization": "CEDAR STAGE"}, "Organization", website_col="website")
    assert text == "Legal name: CEDAR STAGE"


def test_unusual_hyphens_quotes_and_spaces_become_plain():
    assert (
        normalize_name("YMCA of Auburn\u2011Lewiston\u00a0Maine ")
        == "YMCA of Auburn-Lewiston Maine"
    )
    assert normalize_name("Children\u2019s \u201cCenter\u201d") == 'Children\'s "Center"'


@pytest.mark.parametrize(
    "name, filed, expected",
    [
        ("Martha OBryan Center", "MARTHA O'BRYAN CENTER INC", "Martha O'Bryan Center"),
        ("Saint Gregorys School", "SAINT GREGORY'S SCHOOL", "Saint Gregory's School"),
        ("Childrens Center", "CHILDREN\u2019S CENTER", "Children's Center"),
        ("Taylors Crossing", "Taylors Crossing Public Charter School", "Taylors Crossing"),
        ("Taylor's Crossing", "Taylors Crossing Public Charter School", "Taylor's Crossing"),
        ("Saint Gregory's School", "SAINT GREGORY'S SCHOOL", "Saint Gregory's School"),
        # an apostrophe after an acronym or a digit is a plural written wrong
        ("Ohio Alliance of YMCAs", "Ohio Alliance of YMCA's Foundation", "Ohio Alliance of YMCAs"),
        ("4Cs", "4C'S", "4Cs"),
    ],
)
def test_apostrophes_dropped_from_the_website_come_back(name, filed, expected):
    assert restore_apostrophes(name, filed) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Acme Labs, Inc.", "Acme Labs"),
        ("Acme Labs Inc", "Acme Labs"),
        ("Acme Labs LLC", "Acme Labs"),
        ("Acme Labs, L.L.C.", "Acme Labs"),
        ("Acme Labs Incorporated", "Acme Labs"),
        ("Acme Labs Ltd.", "Acme Labs"),
        ("Acme Labs, Inc., LLC", "Acme Labs"),
        ("Corporation for Learning", "Corporation for Learning"),
        ("Princeton Inc Foundation", "Princeton Inc Foundation"),
        ("Inc", "Inc"),
    ],
)
def test_legal_suffixes_at_the_end_are_dropped(name, expected):
    assert drop_legal_suffix(name) == expected


def test_apostrophe_can_come_from_the_dba():
    assert restore_apostrophes("Joes Garage", "GARAGE INC", "JOE'S GARAGE") == "Joe's Garage"


class FakeCache:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def bulk_get_cache_or_run(self, given_ids_texts, **kwargs):
        choice = NameChoice(
            display_name="Harlow Youth Clubs",
            source="dba",
            confidence="high",
            reason="The website uses the DBA.",
        )
        # the second org's call failed, so it is left out of the results
        given_id, _ = given_ids_texts[0]
        return {given_id: {"response_text": choice.model_dump_json()}}


def test_orgs_with_an_unparseable_answer_are_left_out(monkeypatch):
    class FreeTextCache(FakeCache):
        def bulk_get_cache_or_run(self, given_ids_texts, **kwargs):
            return {given_ids_texts[0][0]: {"response_text": "Harlow Youth Clubs"}}

    monkeypatch.setattr(display_name, "PromptResponseCacheSQL", FreeTextCache)
    result = pick_display_names(
        pd.DataFrame([{"id": "a", **ROW}]), session=None, name_col="Organization"
    )
    assert result.empty


def test_picked_names_lose_a_legal_suffix_the_model_left(monkeypatch):
    class SuffixCache(FakeCache):
        def bulk_get_cache_or_run(self, given_ids_texts, **kwargs):
            choice = NameChoice(
                display_name="Harlow Youth Clubs, Inc.",
                source="legal_name",
                confidence="high",
                reason="The filed name.",
            )
            return {given_ids_texts[0][0]: {"response_text": choice.model_dump_json()}}

    monkeypatch.setattr(display_name, "PromptResponseCacheSQL", SuffixCache)
    df = pd.DataFrame([{"id": "a", "Organization": "HARLOW YOUTH CLUBS INC"}])
    assert pick_display_names(df, session=None, name_col="Organization")[
        "display_name"
    ].tolist() == ["Harlow Youth Clubs"]


def test_picked_names_get_their_apostrophes_back(monkeypatch):
    class DroppedApostropheCache(FakeCache):
        def bulk_get_cache_or_run(self, given_ids_texts, **kwargs):
            choice = NameChoice(
                display_name="Martha OBryan Center",
                source="website",
                confidence="high",
                reason="The website uses it.",
            )
            return {given_ids_texts[0][0]: {"response_text": choice.model_dump_json()}}

    monkeypatch.setattr(display_name, "PromptResponseCacheSQL", DroppedApostropheCache)
    df = pd.DataFrame([{"id": "a", "Organization": "MARTHA O'BRYAN CENTER INC"}])
    assert pick_display_names(df, session=None, name_col="Organization")[
        "display_name"
    ].tolist() == ["Martha O'Bryan Center"]


def test_orgs_whose_call_failed_are_left_out(monkeypatch):
    monkeypatch.setattr(display_name, "PromptResponseCacheSQL", FakeCache)
    df = pd.DataFrame([{"id": "a", **ROW}, {"id": "b", **ROW}])
    result = pick_display_names(df, session=None, name_col="Organization")
    assert result.to_dict("records") == [
        {
            "id": "a",
            "display_name": "Harlow Youth Clubs",
            "source": "dba",
            "confidence": "high",
            "reason": "The website uses the DBA.",
        }
    ]
