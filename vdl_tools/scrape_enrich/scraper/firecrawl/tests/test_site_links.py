from vdl_tools.scrape_enrich.scraper.firecrawl.site_links import normalize_host, same_site_paths


def test_normalize_host():
    assert normalize_host("https://WWW.Example.org:8443/about?x=1") == "example.org"
    assert normalize_host("www.example.org") == "example.org"
    assert normalize_host("give.example.org") == "give.example.org"
    assert normalize_host("") == ""
    assert normalize_host(None) == ""


def test_keeps_same_site_pages_only():
    links = [
        "https://www.example.org/about",
        "https://example.org/our-work/",          # trailing slash merges with below
        "https://example.org/our-work",
        "http://EXAMPLE.org/news?page=2#top",     # scheme, case, query and fragment
        "/team",                                  # relative
        "contact",                                # relative to the final URL
        "https://give.example.org/donate",        # another host, even a subdomain
        "https://twitter.com/example",
        "mailto:info@example.org",
        "tel:+15555550100",
        "javascript:void(0)",
        "https://example.org/",                   # the root
        "https://example.org/report.pdf",         # a file
        "https://example.org/logo.PNG",
        "https://example.org/history.html",       # page extensions stay
        "https://example.org/index.php/about/station-1",
        "",
        None,
    ]
    assert same_site_paths(links, "https://www.example.org/") == [
        "/about", "/contact", "/history.html", "/index.php/about/station-1",
        "/news", "/our-work", "/team",
    ]


def test_compares_against_the_host_the_page_was_served_from():
    # Asked for oldname.org, landed on newname.org: the links are on the new host.
    links = ["https://newname.org/about", "https://oldname.org/legacy", "/programs"]
    assert same_site_paths(links, "https://newname.org/") == ["/about", "/programs"]


def test_bare_host_and_bad_input():
    assert same_site_paths(["https://example.org/about"], "example.org") == ["/about"]
    assert same_site_paths(["http://[bad", "https://example.org/ok"], "https://example.org") == ["/ok"]
    assert same_site_paths(None, "https://example.org") == []
    assert same_site_paths(["https://example.org/about"], "") == []


def test_output_does_not_depend_on_input_order():
    links = [f"https://example.org/p{i}" for i in range(50)]
    assert same_site_paths(links, "https://example.org") == same_site_paths(links[::-1], "https://example.org")
