from vdl_tools.scrape_enrich.website_quality import (
    is_junk_summary,
    load_website_decisions,
    normalize_website,
)


def test_normalize_website_repairs_raw_fields():
    assert normalize_website("https://www.exampleschool.org/about") == "exampleschool.org"
    assert normalize_website("www. exampleschool. org") == "exampleschool.org"
    assert normalize_website("https: exampleschool.org") == "exampleschool.org"
    assert normalize_website("info@exampleschool.org") == "exampleschool.org"
    assert normalize_website("www2.exampleschool.org") == "exampleschool.org"
    # identifies nothing: a mail provider, a platform, a dot-less placeholder
    assert normalize_website("someone@gmail.com") == ""
    assert normalize_website("https://www.facebook.com/exampleschool") == ""
    assert normalize_website("none") == ""


def test_decisions_match_an_ein_in_either_form(tmp_path):
    path = tmp_path / "decisions.csv"
    path.write_text(
        "id,action,note\n"
        "12-3456789,remove_website,bare EIN for a GivingTuesday org\n"
        "givingtuesday_98-7654321,remove_url,prefixed EIN for a Candid org\n"
        "11-1111111,keep,EIN present under both forms\n"
        "00-0000000,remove_website,no such org\n"
    )
    present = {
        "givingtuesday_12-3456789",
        "98-7654321",
        "11-1111111",
        "givingtuesday_11-1111111",
        "a1b2c3d4-crunchbase-uuid",
    }
    decisions = load_website_decisions({"website_quality_decisions": path}, present)
    assert decisions == {
        "givingtuesday_12-3456789": "remove_website",
        "98-7654321": "remove_url",
        "11-1111111": "keep",
        "givingtuesday_11-1111111": "keep",
    }


def test_junk_summary_checks_the_orgs_own_records():
    casino = "Example88 is an online casino offering slot games and sports betting."
    assert is_junk_summary(casino, "Example Youth Mentoring Program")
    # a real gaming company describing itself is not a squatter
    assert not is_junk_summary(casino, "Example Gaming Inc online casino operator")
    assert is_junk_summary("The domain exampleschool.org is listed on HugeDomains.", "Example School")
    assert not is_junk_summary("Example School teaches K-8 students.", "Example School")
