r"""get_page_text: Unicode spaces and tabs on the fallback path become spaces.

Unstructured collapses every whitespace run itself, so in-house text only met
clean_non_ascii_text's Unicode-space bug on the BeautifulSoup fallback
(process_page_source), which runs when Unstructured yields under 500
characters and replaces only "\xa0". Every in-house page text the fix changed
in the vendor pilot came from that path ("10:35\u202fAM" became "10:35AM").
"""
from vdl_tools.scrape_enrich.scraper import website_processor


def _page(body):
    return f"<html><body>{body}</body></html>"


def test_fallback_text_keeps_words_apart_across_unicode_spaces():
    text = website_processor.get_page_text(
        "https://example.org", _page("<p>Doors open at 10:35\u202fAM daily.</p>")
    )
    assert text == "Doors open at 10:35 AM daily."


def test_fallback_text_turns_tabs_into_spaces():
    text = website_processor.get_page_text(
        "https://example.org", _page("<p>News List\tHead of School</p>")
    )
    assert text == "News List Head of School"
