from vdl_tools.scrape_enrich.url_keys import extract_website_name, website_domain


def test_page_key_is_unchanged():
    # The scrape cache is keyed by this output; it must never change.
    assert extract_website_name("https://google.com/") == "google.com"
    assert extract_website_name("https://www.Example.org/about-us?ref=x") == "www.Example.org_about-us"
    assert extract_website_name("http://example.org:8080/") == "example.org_8080"
    assert extract_website_name("") is None


def test_website_domain_reduces_a_url_to_its_site():
    assert website_domain("https://www.exampleschool.org/about") == "exampleschool.org"
    assert website_domain("http://Example.org:8080/") == "example.org"
    # repairs what raw website fields carry
    assert website_domain("www. exampleschool. org") == "exampleschool.org"
    assert website_domain("https: exampleschool.org") == "exampleschool.org"
    assert website_domain("info@exampleschool.org") == "exampleschool.org"
    assert website_domain("www2.exampleschool.org") == "exampleschool.org"
    assert website_domain("www.www.exampleschool.org") == "exampleschool.org"


def test_website_domain_is_empty_when_the_value_identifies_no_org():
    assert website_domain("someone@gmail.com") == ""
    assert website_domain("https://www.facebook.com/exampleschool") == ""
    assert website_domain("N/A") == ""
    assert website_domain(None) == ""
