from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality

LONG = "Real org text about climate work. " * 20


def test_quality_verdicts():
    assert classify_text_quality(LONG) == "ok"
    assert classify_text_quality("tiny") == "thin"
    assert classify_text_quality("THIS DOMAIN IS FOR SALE - act now " * 20) == "parked"
    assert classify_text_quality(None, num_errors=3) == "dead"
    assert classify_text_quality("", num_errors=0) == "empty"
    assert classify_text_quality(float("nan"), num_errors=1) == "dead"
