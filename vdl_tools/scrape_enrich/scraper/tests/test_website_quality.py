from vdl_tools.scrape_enrich.website_junk_rules import is_junk_summary
from vdl_tools.scrape_enrich.website_quality import (
    _save_cache,
    load_domain_cache,
    load_website_decisions,
)


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
    decisions = load_website_decisions(path, present)
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


def test_domain_cache_round_trips_and_keeps_another_runs_newer_checks(tmp_path):
    uri = tmp_path / "website_domain_cache.parquet"
    assert load_domain_cache(uri) == {}
    old = {"final_domain": "example.org", "alive": False, "checked": "2026-01-01"}
    new = {"final_domain": "example.org", "alive": True, "checked": "2026-10-01"}
    # another project saved a newer check for example.org meanwhile
    _save_cache({"example.org": new}, uri)
    _save_cache({"example.org": old, "other.org": old}, uri)
    assert load_domain_cache(uri) == {"example.org": new, "other.org": old}


def test_final_domain_follows_the_cache_and_falls_back_to_the_domain():
    from vdl_tools.scrape_enrich.website_quality import final_domain

    cache = {"old.org": {"final_domain": "new.org"}}
    assert final_domain("https://www.old.org/about", cache) == "new.org"
    assert final_domain("other.org", cache) == "other.org"
    assert final_domain("other.org", None) == "other.org"
    assert final_domain("na", cache) is None
