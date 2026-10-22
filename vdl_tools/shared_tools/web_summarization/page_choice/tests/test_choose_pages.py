import json
import os
import random
import subprocess
import sys
from pathlib import Path

from vdl_tools.shared_tools.web_summarization.page_choice import choose_pages
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import (
    deterministic_len_key,
    filter_links,
)

KEEP_PATHS = ("about-us", "news")
MAX_PER_SUBPATH = 3
# More /news candidates than the cap allows, so the cap has to choose among them.
LINKS = ["/", "/about-us", "/about-us/team"] + [f"/news/post-{i}" for i in range(12)]


def _filter_links_in_subprocess(hash_seed: str) -> list[str]:
    # The checkout that owns the module under test goes first on sys.path, so the
    # child runs this same code and not another installed copy of vdl_tools.
    repo_root = Path(choose_pages.__file__).resolve().parents[4]
    script = "\n".join([
        "import json, sys",
        f"sys.path.insert(0, {str(repo_root)!r})",
        "from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links",
        "links, keep_paths, cap = json.loads(sys.stdin.read())",
        "print(json.dumps(filter_links(links, keep_paths=tuple(keep_paths), max_per_subpath=cap)))",
    ])
    result = subprocess.run(
        [sys.executable, "-c", script],
        input=json.dumps([LINKS, KEEP_PATHS, MAX_PER_SUBPATH]),
        env={**os.environ, "PYTHONHASHSEED": hash_seed},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_filter_links_picks_the_same_pages_under_every_hash_seed():
    # Scraped pages are cached by path, so the pick must not depend on the
    # interpreter's string hashing (PYTHONHASHSEED), which differs per process.
    picks = {seed: _filter_links_in_subprocess(seed) for seed in ("0", "1", "2", "3")}
    assert len({json.dumps(pick) for pick in picks.values()}) == 1, picks
    assert picks["0"] == filter_links(LINKS, KEEP_PATHS, MAX_PER_SUBPATH)


def test_filter_links_picks_the_same_pages_whatever_the_input_order():
    expected = filter_links(LINKS, KEEP_PATHS, MAX_PER_SUBPATH)
    rng = random.Random(0)
    for _ in range(5):
        shuffled = list(LINKS)
        rng.shuffle(shuffled)
        assert filter_links(shuffled, KEEP_PATHS, MAX_PER_SUBPATH) == expected
    assert filter_links(LINKS * 2, KEEP_PATHS, MAX_PER_SUBPATH) == expected


def test_filter_links_keeps_the_shortest_paths_when_a_section_exceeds_the_cap():
    kept = filter_links(LINKS, KEEP_PATHS, MAX_PER_SUBPATH)
    news_candidates = [link.strip("/") for link in LINKS if link.startswith("/news/")]
    expected_news = sorted(news_candidates, key=deterministic_len_key)[:MAX_PER_SUBPATH]
    assert [page for page in kept if page.startswith("news/")] == expected_news
    # The cap only decides which bottom pages survive; eligibility is untouched.
    assert set(kept) == {"", "about-us", "about-us/team", "news", *expected_news}


def test_filter_links_docstring_example():
    links = [
        "/",
        "/about-us",
        "/about-us/news",
        "/about-us/news/our-work-in-africa",
        "/about-us/news/our-work-in-asia",
        "/about-us/news/our-work-in-europe",
        "/about-us/news/our-work-in-north-america",
        "/about-us/news/our-work-in-south-america",
        "/privacy-policy",
    ]
    kept = filter_links(links, keep_paths=("about-us", "news"), max_per_subpath=4)
    bottom_pages = sorted(
        (link.strip("/") for link in links if link.startswith("/about-us/news/")),
        key=deterministic_len_key,
    )
    assert set(kept) == {"", "about-us", *bottom_pages[:4]}
    # A middle path and a path outside keep_paths never make it through.
    assert "about-us/news" not in kept
    assert "privacy-policy" not in kept
