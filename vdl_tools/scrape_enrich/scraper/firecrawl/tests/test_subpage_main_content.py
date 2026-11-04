"""The D9 measures: Q2 (subpage onlyMainContent) is decided on these helpers."""

import asyncio
import json

import httpx

from vdl_tools.scrape_enrich.scraper.firecrawl.validation import subpage_main_content as d9
from vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl import FirecrawlRunner


def test_sentences_keep_five_word_sentences_once_and_normalized():
    text = "We restore rivers in Ohio. Short one! We restore rivers in Ohio.\nThe Akron board meets monthly?"
    assert d9.sentences(text) == ["we restore rivers in ohio", "the akron board meets monthly"]
    assert d9.sentences(None) == []


def test_recall_ignores_markdown_and_punctuation_but_needs_whole_sentences():
    ref = d9.sentences("We restore wetlands across northern Ohio. Our staff of twelve works statewide.")
    text = "## We restore wetlands across **northern Ohio**\nOur staff works statewide."
    assert d9.recall(ref, text) == (1, 2)
    # a sentence must match on word boundaries, not inside a longer word
    assert d9.recall(d9.sentences("The farm grows corn and beans."), "the farm grows corn and beanstalks") == (0, 1)


def test_dup_share_counts_lines_seen_on_an_earlier_page():
    pages = ["Home\nAbout us\nOur mission", "About us\nNews from the field", "Our mission"]
    dup = len("About us") + len("Our mission")
    total = sum(len(line) for p in pages for line in p.split("\n"))
    assert d9._dup_share(pages) == dup / total
    assert d9._dup_share(["", ""]) is None


def test_payload_overrides_reach_the_request():
    sent = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "data": {
            "markdown": "# Hello", "metadata": {"statusCode": 200, "creditsUsed": 1}}})

    runner = FirecrawlRunner("key", payload_overrides={"formats": ["markdown"], "onlyMainContent": True})

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await runner.fetch(client, "https://example.org/about")

    result = asyncio.run(go())
    assert sent["formats"] == ["markdown"] and sent["onlyMainContent"] is True
    assert sent["url"] == "https://example.org/about" and sent["maxAge"] == 0
    assert result.success and result.http_status == 200
