"""clean_tags / clean_tag_list: dedupe without reordering.

Regression context: both deduped with set(), whose iteration order for strings
changes with every Python process (PYTHONHASHSEED). prepare_candid joins each
org's mission and program texts with " // " and runs them through clean_tags,
so every reprocessing of Candid reshuffled those segments. The text hash
changed, every text-keyed cache missed (relevance, adaptation, summary of
summaries), and in the Oct 2026 CFT run 14,861 Candid orgs were re-classified
for a reorder alone; 216 of them flipped their relevance verdict.
"""
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.common_functions import clean_tag_list, clean_tags

REPO_ROOT = Path(__file__).resolve().parents[3]
SEGMENTS = [
    "To enable others to do the most good",
    "Our mission is to ensure that no community is left behind",
    "Streetscape",
    "A peer-led program dedicated to ending homelessness",
]


def test_clean_tags_keeps_first_seen_order_and_dedupes():
    df = pd.DataFrame({"Description": [" // ".join(SEGMENTS + [SEGMENTS[0]])]})
    assert clean_tags(df, "Description", delimeter=" // ")[0] == " // ".join(SEGMENTS)


def test_clean_tag_list_keeps_first_seen_order_and_dedupes():
    df = pd.DataFrame({"Geo_Tags": [["Ohio", "United States", "Ohio", " ", "Cleveland"]]})
    assert clean_tag_list(df, "Geo_Tags")[0] == ["Ohio", "United States", "Cleveland"]


def test_clean_tags_output_is_the_same_under_any_hash_seed():
    code = (
        "import pandas as pd\n"
        "from vdl_tools.shared_tools.common_functions import clean_tags\n"
        f"df = pd.DataFrame({{'d': [{' // '.join(SEGMENTS)!r}]}})\n"
        "print(clean_tags(df, 'd', delimeter=' // ')[0])\n"
    )
    outputs = set()
    for seed in ("1", "2", "3", "4"):
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(REPO_ROOT)}
        run = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True)
        outputs.add(run.stdout.strip())
    assert outputs == {" // ".join(SEGMENTS)}
