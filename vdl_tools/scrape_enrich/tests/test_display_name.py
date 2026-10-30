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
    name_words,
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


def test_name_words_leave_out_the_words_that_say_what_kind_of_organization_it_is():
    assert name_words("The Harlow Charitable Trust, Inc.") == {"harlow"}
    assert name_words("Northfield School of Music") == {"northfield", "music"}


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


def test_keeps_every_word():
    assert display_name.keeps_every_word("Caldwell County School", "Caldwell County Schools")
    assert display_name.keeps_every_word("Education Development Center (EDC)", "Education Development Center")
    assert display_name.keeps_every_word("Partnership for Students, & Sustainability", "Partnership for Students and Sustainability")
    assert display_name.keeps_every_word("HORIZON EDUCATIONAL SERVICES OF COLUMBUSINC", "Horizon Educational Services of Columbus")
    assert not display_name.keeps_every_word("LEAP", "Learning through an Expanded Arts Program")
    assert not display_name.keeps_every_word("goEzz", "GoEasy")
    assert not display_name.keeps_every_word("YouthDevelopment Corporation", "Youth Development")


def test_siblings_are_the_other_names_on_the_site_sorted():
    names = ["Zeta Campus", "Alpha Campus", "Solo", "No Site", "Alpha Campus"]
    sites = ["net.org", "net.org", "solo.org", None, "net.org"]
    assert display_name.site_siblings(names, sites) == [
        ["Alpha Campus", "Alpha Campus"], ["Alpha Campus", "Zeta Campus"], [], [], ["Alpha Campus", "Zeta Campus"],
    ]


def test_site_texts_skip_junk_pages_and_try_the_key_without_www(monkeypatch):
    pages = {"good.org": ("Good Org runs after-school clubs. " * 20, 0), "parked.org": ("This domain is for sale.", 0)}
    monkeypatch.setattr(display_name, "combined_texts", lambda keys, session: {k: pages[k] for k in keys if k in pages})
    out = display_name.site_texts(["https://WWW.Good.org/", "parked.org", None], session=None)
    assert list(out) == ["https://WWW.Good.org/"]


class AnswerPicker:
    """Stands in for ``pick_display_names``: records each call and answers ``(name, confidence)`` from ``answers``."""

    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def __call__(self, rows, session, name_col, id_col, prompt=None, **kwargs):
        self.calls.append({"prompt": prompt, "ids": list(rows[id_col]), **kwargs})
        found = [(i, *self.answers[(prompt, i)]) for i in rows[id_col] if (prompt, i) in self.answers]
        return pd.DataFrame(
            {id_col: [f[0] for f in found], "display_name": [f[1] for f in found],
             "confidence": [f[2] for f in found], "source": "legal_name", "reason": ""}
        )


def network():
    return pd.DataFrame(
        {
            "id": ["n", "s", "x"],
            "Organization": ["RIVERBEND PREP NORTH", "RIVERBEND PREP SOUTH", "HARLOW YOUTH CLUBS"],
            "Website": ["riverbend.org", "https://www.riverbend.org/south", "old-harlow.org"],
        }
    )


def test_siblings_on_one_site_with_one_name_are_named_again_from_their_filed_names(monkeypatch):
    picker = AnswerPicker(
        {
            (PROMPT_SHARED_SITE, "n"): ("Riverbend Prep", "high"),
            (PROMPT_SHARED_SITE, "s"): ("Riverbend Prep", "high"),
            (PROMPT, "x"): ("Harlow Youth Clubs", "high"),
            (PROMPT_FILED_NAME, "n"): ("Riverbend Prep North", "high"),
            (PROMPT_FILED_NAME, "s"): ("Riverbend Prep South", "high"),
        }
    )
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = display_name.pick_names(network(), None, "id", "Organization", website_col="Website")
    assert names.to_dict() == {"x": "Harlow Youth Clubs", "n": "Riverbend Prep North", "s": "Riverbend Prep South"}
    assert [c["prompt"] for c in picker.calls] == [PROMPT, PROMPT_SHARED_SITE, PROMPT_FILED_NAME]
    assert "siblings_col" not in picker.calls[2]


def test_sites_follow_redirects_from_the_domain_cache(monkeypatch):
    picker = AnswerPicker({})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    cache = {"old-harlow.org": {"final_domain": "riverbend.org"}}
    display_name.pick_names(network(), None, "id", "Organization", domain_cache=cache, website_col="Website")
    (shared,) = [c for c in picker.calls if c["prompt"] == PROMPT_SHARED_SITE]
    assert shared["ids"] == ["n", "s", "x"]


def test_a_low_confidence_or_refused_answer_is_named_again_from_the_filed_name(monkeypatch):
    picker = AnswerPicker(
        {
            (PROMPT, "x"): ("Somebody Else", "low"),
            (PROMPT_SHARED_SITE, "n"): ("Riverbend North", "high"),
            (PROMPT_SHARED_SITE, "s"): ("Riverbend South", "high"),
            (PROMPT_FILED_NAME, "x"): ("Harlow Youth Clubs", "high"),
            (PROMPT_FILED_NAME, "s"): ("Riverbend Academy", "high"),
        }
    )
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    refuse = lambda answers, rows: answers["id"] == "s"  # noqa: E731
    names = display_name.pick_names(network(), None, "id", "Organization", refuse=refuse, website_col="Website")
    # "Riverbend Academy" drops words of the filed name, so "s" gets no name.
    assert names.to_dict() == {"n": "Riverbend North", "x": "Harlow Youth Clubs"}


def test_without_choose_every_row_only_gets_its_filed_name_cased(monkeypatch):
    picker = AnswerPicker({(PROMPT_FILED_NAME, "x"): ("Harlow Youth Clubs", "high")})
    monkeypatch.setattr(display_name, "pick_display_names", picker)
    names = display_name.pick_names(network(), None, "id", "Organization", choose=False, website_col="Website")
    assert [c["prompt"] for c in picker.calls] == [PROMPT_FILED_NAME]
    assert names.to_dict() == {"x": "Harlow Youth Clubs"}
