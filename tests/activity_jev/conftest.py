import json

import pandas as pd
import pytest

from vdl_tools.shared_tools.activity_extraction.general import GeneralActivityProfile
from vdl_tools.shared_tools.activity_extraction.pipeline import extract_frame
from vdl_tools.shared_tools.taxonomy_mapping import drawdown, one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import map_subjects, score_subjects
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.exports import export_extraction
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.subjects import build_activity_subjects

PACKAGES = {"one_earth": one_earth, "drawdown": drawdown}
# The Drawdown wording names these two nodes; a fixture workbook must contain them.
STEWARDSHIP_SECTOR = "Nature-Based Carbon Removal"
STEWARDSHIP_CLUSTER = "Protect, Manage, & Restore Ecosystems"


@pytest.fixture(autouse=True)
def offline_config(tmp_path, monkeypatch):
    config = tmp_path / "offline.ini"
    config.write_text(
        "[postgres]\nhost=localhost\nport=1\nuser=offline\npassword=offline\ndatabase=offline\n"
    )
    monkeypatch.setenv("VDL_GLOBAL_CONFIG_PATH", str(config))


def profile(activities=2):
    return GeneralActivityProfile(
        summary="A solar organization installs panels and trains installers.",
        activities=[
            {
                "activity": f"solar work {i}",
                "what_happens": "Installs rooftop solar panels.",
                "who_carries_it_out": "Own crews",
                "aim": "Clean energy",
                "sector": "electricity",
                "methods": "rooftop solar",
                "geography": "not stated",
                "evidence": "Installs rooftop solar panels",
                "certainty": "high",
                "prominence": "core" if i == 0 else "minor",
                "recurrence": "ongoing",
            }
            for i in range(activities)
        ],
        geographic_orientation_evidence="not stated",
        site_matches_organization="yes",
        site_match_evidence="The organization runs this site.",
        coverage="adequate",
    )


@pytest.fixture
def organizations():
    return pd.DataFrame(
        [
            {"id": "1", "Organization": "Solar", "Website": "https://solar.example", "Sector": "CB"},
            {"id": "2", "Organization": "Missing", "Website": None, "Sector": "CB"},
            {"id": "3", "Organization": "Failed", "Website": "https://failed.example", "Sector": "CB"},
            {"id": "4", "Organization": "Empty", "Website": "https://empty.example", "Sector": "CB"},
        ]
    )


@pytest.fixture
def extraction(organizations):
    texts = {
        url: "URL: " + url + "/\nTEXT: Our own crews install rooftop solar panels on homes. We also train solar installers."
        for url in organizations.Website.dropna()
    }
    return extract_frame(
        organizations,
        run_id="fixture",
        website_texts=texts,
        runner=lambda *a, **kw: {"1": profile(), "4": profile(0)},
    )


@pytest.fixture
def taxonomy(tmp_path):
    def make(name="one_earth", duplicate_names=False, excluded=False):
        package = PACKAGES[name]
        parents = ("Energy", "Nature" if name == "one_earth" else STEWARDSHIP_SECTOR)

        def cluster(parent):
            if duplicate_names:
                return "Repeated"
            return STEWARDSHIP_CLUSTER if parent == STEWARDSHIP_SECTOR else f"{parent} cluster"

        path = tmp_path / f"{name}.xlsx"
        with pd.ExcelWriter(path) as writer:
            for depth, sheet in enumerate(package.SHEETS):
                rows = []
                for parent in parents:
                    row = {
                        package.COLUMNS[0]: parent,
                        "Definition": parent + " work.",
                        "Exclude": int(excluded and parent == parents[1]),
                    }
                    if depth >= 1:
                        row[package.COLUMNS[1]] = cluster(parent)
                    if depth >= 2:
                        row[package.COLUMNS[2]] = parent + " solution"
                        row.update(
                            {"CATEGORY": "Avoid", "CLIMATE POLLUTANTS": "CO2", "SPEED OF ACTION": "Fast"}
                        )
                    rows.append(row)
                pd.DataFrame(rows).to_excel(writer, sheet_name=sheet, index=False)
        return package.load_taxonomy(path)

    return make


def fake_jev(probabilities=None):
    """Replies keyed by cache ID, respecting the exact questions in each request."""

    def run(questions, pairs, **kwargs):
        return {
            key: {
                "response_text": json.dumps(
                    {node: {"noul": (probabilities or {}).get(node, 0.8)} for node in questions}
                ),
                "response_full": {"model": "fixture-jev", "usage": {"input_tokens": 10}},
            }
            for key, _ in pairs
        }

    return run


def subjects_of(extraction):
    """An extraction's activities as the subject frame the engine takes."""
    return build_activity_subjects(extraction.activities)


def score_activities(extraction, tax, **options):
    return score_subjects(subjects_of(extraction), tax, **options)


def map_and_export(organizations, extraction, tax, **options):
    """What a Climate Landscape job does: map the activities, then run the tracker export."""
    mapped = map_subjects(
        subjects_of(extraction), tax, policy=PACKAGES[tax.name].DEFAULT_POLICY, **options
    )
    return export_extraction(organizations, extraction, tax, mapped, mapping_name=f"{tax.name}_category")
