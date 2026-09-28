from vdl_tools.scrape_enrich.scraper.text_quality import (
    classify_text_quality,
    looks_like_bot_wall,
    looks_like_garbled_text,
)

LONG = "Real org text about climate work. " * 20

# Interleaved control bytes at a density (1 in 5 chars) well above
# GARBLED_RATIO, the shape of a mis-decoded response rather than real text.
GARBLED = "".join(chr(i % 20 + 1) if i % 5 == 0 else "x" for i in range(500))

WALL = "This website uses a security service to protect against malicious bots."
LONG_MENTIONS_WALL_VENDOR = "We serve this site through Cloudflare. " * 60


def test_quality_verdicts():
    assert classify_text_quality(LONG) == "ok"
    assert classify_text_quality("tiny") == "thin"
    assert classify_text_quality("THIS DOMAIN IS FOR SALE - act now " * 20) == "parked"
    assert classify_text_quality(None, num_errors=3) == "dead"
    assert classify_text_quality("", num_errors=0) == "empty"
    assert classify_text_quality(float("nan"), num_errors=1) == "dead"


def test_marketplace_names_are_parked_even_without_sale_wording():
    # A marketplace's own copy rarely uses the word "sale".
    assert classify_text_quality("Squadhelp -- premium domain marketplace. " * 10) == "parked"
    assert classify_text_quality("Buy on Afternic or Dan.com today. " * 10) == "parked"


def test_garbled_is_a_verdict():
    assert looks_like_garbled_text(GARBLED)
    assert classify_text_quality(GARBLED) == "garbled"
    assert not looks_like_garbled_text(LONG)
    assert not looks_like_garbled_text("")
    assert not looks_like_garbled_text(None)


def test_bot_wall_is_a_verdict():
    assert looks_like_bot_wall(WALL)
    assert classify_text_quality(WALL) == "blocked"
    # a real page that merely mentions the vendor runs long; a wall does not
    assert not looks_like_bot_wall(LONG_MENTIONS_WALL_VENDOR)
    assert classify_text_quality(LONG_MENTIONS_WALL_VENDOR) == "ok"


def test_verdict_precedence_garbled_beats_parked_beats_blocked():
    # A garbled parked page (a scrape that half-decoded a for-sale listing)
    # is garbled first: there is no reliable "for sale" text to trust in it.
    half_decoded = GARBLED + "domain is for sale"
    assert classify_text_quality(half_decoded) == "garbled"
