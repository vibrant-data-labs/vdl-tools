"""Tests for the Firecrawl text rules (markdown_clean).

Golden tests run every page in fixtures/markdown (real pilot
markdown, chosen for the case each one pins down; see cases.json) through
``page_text`` and one four-page org through ``org_text``, and compare against
the expected files byte for byte. A rule change that alters any output must
bump ``CLEANER_VERSION``; then regenerate with

    REGENERATE_GOLDEN=1 pytest vdl_tools/scrape_enrich/scraper/firecrawl/tests/test_markdown_clean.py -k golden

which refuses to overwrite changed outputs under an unchanged version.
"""

import json
import os
import random
import string
import subprocess
import sys
from pathlib import Path

import pytest

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc

FIXTURES = Path(__file__).parent / "fixtures" / "markdown"
EXPECTED = FIXTURES / "expected"
LONG = "Real org text about restoring wetlands in the river basin. " * 20


def _words(n: int, seed: int) -> str:
    """Varied filler text; make_group_text drops text with heavily repeated bigrams."""
    rng = random.Random(seed)
    return " ".join("".join(rng.choice(string.ascii_lowercase) for _ in range(rng.randint(3, 9)))
                    for _ in range(n))

FRAME_CLIENT = """loadbalancer.visitor-analytics.io

# loadbalancer.visitor-analytics.io is blocked

This page has been blocked by an extension

- Try disabling your extensions.

ERR\\_BLOCKED\\_BY\\_CLIENT

Reload


This page has been blocked by an extension

![](<Base64-Image-Removed>)![](<Base64-Image-Removed>)"""

FRAME_RESPONSE = """give.bicyclecoalition.org

# give.bicyclecoalition.org is blocked

**give.bicyclecoalition.org** refused to connect.

ERR\\_BLOCKED\\_BY\\_RESPONSE

**give.bicyclecoalition.org** refused to connect."""

FRAME_NO_RELOAD = """create.leadid.com

# create.leadid.com is blocked

This page has been blocked by an extension

- Try disabling your extensions.

ERR\\_BLOCKED\\_BY\\_CLIENT

This page has been blocked by an extension"""


# ---------------------------------------------------------------------------
# clean_markdown
# ---------------------------------------------------------------------------

def test_images_go_and_link_labels_stay():
    md = (
        "![s1 img 3.png](https://images.squarespace-cdn.com/content/v1/63204c8f/s1+img+3.png)\n"
        "We restore [the river](https://example.org/river \"River\") and its "
        "[floodplain](https://en.wikipedia.org/wiki/Floodplain_(geography)).\n"
        "[![Logo](https://cdn.example.org/logo.png)](https://example.org/)"
    )
    assert mc.clean_markdown(md) == "We restore the river and its floodplain."


def test_menu_only_lines_are_dropped():
    md = "\n".join([
        "- [VISIT](https://example.org/visit)",
        "  - [MEMBERSHIP](https://example.org/membership)",
        "1. [About](https://example.org/about)",
        "[Home](https://example.org/) | [Donate](https://example.org/donate)",
        "[Skip to content](https://example.org/#main)",
        "## [Our 2026 restoration report](https://example.org/report)",
        "Read [our report](https://example.org/report) for details.",
    ])
    assert mc.clean_markdown(md) == (
        "## Our 2026 restoration report\n"
        "Read our report for details."
    )


def test_multiline_link_label_is_kept_as_text():
    md = "[\n\nPlanting 400 acres of native prairie\n\n](https://example.org/prairie)"
    assert mc.clean_markdown(md) == "Planting 400 acres of native prairie"


def test_bare_and_json_escaped_urls_and_escapes_go():
    md = (
        "Visit https://example.org/visit?x=1 or <https://example.org/a>.\n"
        '{"link_url":"https:\\/\\/www.example.org\\/news\\/item"}\n'
        "ERR\\_CODE and A \\| B"
    )
    out = mc.clean_markdown(md)
    assert "http" not in out
    assert "ERR_CODE and A | B" in out


def test_blank_and_markup_only_lines_go_and_lines_are_stripped():
    md = "# Title\n\n\n-\n  ---  \n   Body text here.   \n·\n"
    assert mc.clean_markdown(md) == "# Title\nBody text here."


def test_unicode_spaces_hard_breaks_and_emphasis():
    md = "formed in 2017\u00a0focused on\u202fenergy\n**Credit Insurance** \\\n\\\nCoverage for __growers__."
    assert mc.clean_markdown(md) == "formed in 2017 focused on energy\nCredit Insurance\nCoverage for growers."
    # and the markers see the space too
    assert mc.page_text("Buy\u00a0this\u00a0domain today. " + LONG).verdict == "parked"


def test_clean_scraped_text_normalization_applies():
    # The normalization in-house text already passes through: non-ASCII out,
    # repeated words and whitespace collapsed.
    assert mc.clean_markdown("Café  open open daily​") == "Caf open daily"


def test_empty_inputs():
    assert mc.clean_markdown(None) == ""
    assert mc.clean_markdown("") == ""
    assert mc.markdown_to_plain(None) == ""


# ---------------------------------------------------------------------------
# Browser chrome
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("block", [FRAME_CLIENT, FRAME_RESPONSE, FRAME_NO_RELOAD])
def test_blocked_frame_variants_are_removed_with_their_host_line(block):
    md = f"Real intro paragraph.\n\n{block}\n\nReal closing paragraph."
    out, n = mc.strip_blocked_frames(md)
    assert n == 1
    assert "blocked" not in out.lower()
    assert "refused to connect" not in out
    assert out.split()[:3] == ["Real", "intro", "paragraph."]
    assert "Real closing paragraph." in out
    assert "visitor-analytics" not in out and "leadid" not in out and "give.bicycle" not in out


def test_blocked_frame_keeps_a_preceding_line_that_is_not_its_host():
    md = "Our partners\n\n" + FRAME_CLIENT.split("\n", 2)[2]
    out, n = mc.strip_blocked_frames(md)
    assert n == 1
    assert out.startswith("Our partners")


def test_is_blocked_heading_without_an_error_code_is_content():
    md = "# Access to the river is blocked\n\nThe county closed the trail after the flood."
    assert mc.strip_blocked_frames(md) == (md, 0)


def test_captcha_widget_and_notice_lines_are_chrome():
    md = "\n".join([
        "Sign up for our newsletter",
        "reCAPTCHA",
        "Recaptcha requires verification.",
        "I'm not a robot",
        "This site is protected by **reCAPTCHA** and the Google Privacy Policy apply.",
        "Real content line.",
    ])
    out, n = mc.strip_browser_chrome(md)
    assert n == 0
    assert out == "Sign up for our newsletter\nReal content line."


# ---------------------------------------------------------------------------
# page_text: the gate runs before the cleaner
# ---------------------------------------------------------------------------

def test_parked_label_on_a_link_only_line_is_caught_before_cleaning():
    md = "[Buy this domain.](https://www.mydomaincontact.com/?domain_name=x.com)\n\n" + LONG
    assert "buy this domain" not in mc.clean_markdown(md).lower()
    assert mc.page_text(md).verdict == "parked"


def test_page_that_is_only_a_blocked_frame_is_blocked():
    md = FRAME_CLIENT + "\n\n[Do Not Sell or Share My Personal Information](https://x.org/#)"
    page = mc.page_text(md)
    assert page.verdict == "blocked"


def test_short_real_page_with_a_blocked_frame_is_ok():
    md = LONG[:400] + "\n\n" + FRAME_CLIENT
    page = mc.page_text(md)
    assert page.verdict == "ok"
    assert "blocked" not in page.text.lower()


def test_recaptcha_form_on_a_short_page_is_not_a_wall():
    md = LONG[:700] + "\n\nreCAPTCHA\n\nRecaptcha requires verification.\n\nI'm not a robot\n\nreCAPTCHA"
    assert mc.page_text(md).verdict == "ok"


def test_real_walls_are_still_blocked():
    md = "# Checking the site connection security\n\nThis process is automatic. Your browser will redirect shortly."
    assert mc.page_text(md).verdict == "blocked"


def test_wall_markers_do_not_run_again_on_cleaned_text():
    # 900 characters of real text with a footer that mentions Cloudflare: long
    # as markdown, short once the menu goes. Only the length check runs on the
    # cleaned text.
    menu = "\n".join(f"- [Item {i}](https://example.org/item-{i})" for i in range(60))
    md = menu + "\n\n" + LONG[:900] + "\nSite served by Cloudflare."
    assert mc.page_text(md).verdict == "ok"


def test_thin_and_empty_by_length():
    assert mc.page_text("Short real sentence about us.").verdict == "thin"
    assert mc.page_text("- [Home](https://x.org/)\n- [About](https://x.org/about)").verdict == "empty"
    assert mc.page_text(None).verdict == "empty"


# ---------------------------------------------------------------------------
# dedupe, cap, org_text
# ---------------------------------------------------------------------------

def test_dedupe_keeps_first_occurrence_across_and_within_pages():
    home = "# About Us\nWe restore rivers.\nOpen MenuClose Menu\nOpen MenuClose Menu"
    sub = "About us\n  we restore   RIVERS.\nOur 2026 projects.\nOpen MenuClose Menu"
    assert mc.dedupe_lines([home, sub]) == [
        "# About Us\nWe restore rivers.\nOpen MenuClose Menu",
        "Our 2026 projects.",
    ]


def test_cap_tokens_cuts_from_the_end_on_a_word_boundary():
    text = " ".join(f"word{i}" for i in range(20000))
    capped = mc.cap_tokens(text, 1000)
    assert mc.count_tokens(capped) <= 1000
    assert text.startswith(capped)
    assert text[len(capped)] == " "
    assert mc.cap_tokens(capped, 1000) == capped
    assert mc.cap_tokens("short text", 1000) == "short text"


def _org_pages():
    return [
        mc.OrgPage("/", True, "ok", "Home page text about the org and its mission. " * 5),
        mc.OrgPage("about", False, "ok", "About page text, different from home. " * 5),
        mc.OrgPage("news/a", False, "ok", "A news post with its own content here. " * 5),
        mc.OrgPage("blog", False, "parked", "This domain is for sale. " * 10),
        mc.OrgPage("team", False, "thin", "Our team: Ada Lovelace and Lin Wu, both long-time board members."),
    ]


def test_org_text_orders_pages_and_excludes_junk_pages():
    org = mc.org_text("https://example.org", _org_pages())
    assert org.verdict == "ok"
    assert org.text.startswith("URL: https://example.org/\nTEXT: Home page text")
    assert "for sale" not in org.text
    assert org.subpaths == ["/", "team", "about", "news/a"]
    assert org.extractor == mc.EXTRACTOR


def test_org_text_does_not_depend_on_input_order():
    pages = _org_pages()
    reference = mc.org_text("https://example.org", pages)
    for seed in range(5):
        shuffled = pages[:]
        random.Random(seed).shuffle(shuffled)
        assert mc.org_text("https://example.org", shuffled) == reference


def test_home_page_stays_first_next_to_a_one_character_subpath():
    pages = [mc.OrgPage("a", False, "ok", _words(40, 1)),
             mc.OrgPage("/", True, "ok", "Home text. " + _words(40, 2))]
    org = mc.org_text("https://example.org", pages)
    assert org.text.startswith("URL: https://example.org/\nTEXT: Home text")


def test_junk_home_makes_the_org_junk():
    pages = [mc.OrgPage("/", True, "parked", "Buy this domain"),
             mc.OrgPage("about", False, "ok", LONG)]
    org = mc.org_text("https://example.org", pages)
    assert org.verdict == "parked"
    assert org.subpaths == ["about"]


def test_cap_applies_and_lineage_lists_only_pages_in_the_text():
    pages = [mc.OrgPage("/", True, "ok", _words(1500, 0))] + [
        mc.OrgPage(f"page{i}", False, "ok", _words(1200, i + 1)) for i in range(5)
    ]
    org = mc.org_text("https://example.org", pages)
    assert org.tokens <= mc.ORG_TOKEN_CAP < org.tokens_before_cap
    assert org.truncated
    assert org.subpaths[0] == "/"
    assert len(org.subpaths) < len(pages)
    assert all(f"/{sp}\nTEXT: " in org.text or sp == "/" for sp in org.subpaths)


def test_no_usable_pages_gives_empty():
    org = mc.org_text("https://example.org", [mc.OrgPage("/", True, "empty", "")])
    assert (org.text, org.verdict, org.subpaths, org.tokens) == ("", "empty", [], 0)


_DETERMINISM_SCRIPT = """
import hashlib, json, sys
from pathlib import Path
from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
fixtures = Path(sys.argv[1])
cases = json.loads((fixtures / "cases.json").read_text())
case = cases["orgs"][0]
pages = []
for p in case["pages"]:
    page = mc.page_text((fixtures / (p["file"] + ".md")).read_text())
    pages.append(mc.OrgPage(p["subpath"], p["is_home"], page.verdict, page.text))
print(hashlib.sha256(mc.org_text(case["source"], pages).text.encode()).hexdigest())
"""


def test_combined_text_is_identical_across_hash_seeds():
    """set() order changes with PYTHONHASHSEED; the text must not."""
    digests = set()
    for seed in ("0", "1", "4242"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        out = subprocess.run([sys.executable, "-c", _DETERMINISM_SCRIPT, str(FIXTURES)],
                             env=env, capture_output=True, text=True, check=True)
        digests.add(out.stdout.strip().splitlines()[-1])
    assert len(digests) == 1


# ---------------------------------------------------------------------------
# Golden files
# ---------------------------------------------------------------------------

def _cases() -> dict:
    return json.loads((FIXTURES / "cases.json").read_text())


def _golden_outputs(cases: dict) -> dict[str, tuple[str, str]]:
    """name -> (verdict, text) for every page case and org case."""
    out = {}
    for case in cases["pages"]:
        page = mc.page_text((FIXTURES / f"{case['name']}.md").read_text())
        out[case["name"]] = (page.verdict, page.text)
    for case in cases["orgs"]:
        pages = []
        for p in case["pages"]:
            page = mc.page_text((FIXTURES / f"{p['file']}.md").read_text())
            pages.append(mc.OrgPage(p["subpath"], p["is_home"], page.verdict, page.text))
        org = mc.org_text(case["source"], pages)
        out[f"org_{case['name']}"] = (org.verdict, org.text)
    return out


def _regenerate(cases: dict, outputs: dict) -> None:
    EXPECTED.mkdir(exist_ok=True)
    changed = [name for name, (_, text) in outputs.items()
               if (EXPECTED / f"{name}.txt").exists() and (EXPECTED / f"{name}.txt").read_text() != text]
    if changed and mc.CLEANER_VERSION == cases["cleaner_version"]:
        pytest.fail(f"outputs changed for {changed} under CLEANER_VERSION {mc.CLEANER_VERSION}; bump it first")
    for name, (_, text) in outputs.items():
        (EXPECTED / f"{name}.txt").write_text(text)
    cases["cleaner_version"] = mc.CLEANER_VERSION
    (FIXTURES / "cases.json").write_text(json.dumps(cases, indent=2) + "\n")


def test_golden_pages_and_org():
    cases = _cases()
    outputs = _golden_outputs(cases)
    if os.environ.get("REGENERATE_GOLDEN") == "1":
        _regenerate(cases, outputs)
        pytest.skip("golden files regenerated")
    assert mc.CLEANER_VERSION == cases["cleaner_version"], "CLEANER_VERSION changed: regenerate the golden files"
    expected_verdicts = {c["name"]: c["verdict"] for c in cases["pages"]}
    expected_verdicts.update({f"org_{c['name']}": c["verdict"] for c in cases["orgs"]})
    for name, (verdict, text) in outputs.items():
        assert verdict == expected_verdicts[name], name
        assert text == (EXPECTED / f"{name}.txt").read_text(), name


def test_golden_cleaning_halves_nav_heavy_pages():
    # Raw pilot markdown is about 3x the cleaned text at the median; these two
    # fixtures are mostly navigation and repeated headers.
    for name in ("rcdmonterey_nav_home", "intersectag_duplicated_header"):
        raw = (FIXTURES / f"{name}.md").read_text()
        assert len(mc.clean_markdown(raw)) < len(raw) / 2, name


def test_golden_org_dedupe_removes_repeated_chrome():
    cases = _cases()
    case = cases["orgs"][0]
    texts = [mc.page_text((FIXTURES / f"{p['file']}.md").read_text()).text
             for p in sorted(case["pages"], key=lambda p: (not p["is_home"], len(p["subpath"])))]
    lines = [mc._line_key(line) for t in mc.dedupe_lines(texts) for line in t.split("\n")]
    assert len(lines) == len(set(lines))
    assert sum(len(t) for t in mc.dedupe_lines(texts)) < sum(len(t) for t in texts)

