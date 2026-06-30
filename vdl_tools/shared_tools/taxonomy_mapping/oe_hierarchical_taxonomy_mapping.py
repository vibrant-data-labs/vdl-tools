"""
One Earth hierarchical taxonomy mapping — library
==================================================

Reusable classifier that maps any DataFrame of entities (companies,
organizations, projects — anything with a name and a description) onto
the four-level One Earth Climate Solutions Framework taxonomy:

    0. Pillar      (5 nodes: Energy Transition, Nature Conservation,
                    Regenerative Agriculture, Cross-Cutting, Geo-Engineering)
    1. Sub-Pillar  (~17 nodes; children of a Pillar)
    2. Solution    (~120 nodes; children of a Sub-Pillar)
    3. Sub-Term    (~700+ nodes; children of a Solution; absent for the
                    Cross-Cutting pillar — walk leafs at Solution there)

This is the data + prompt half. The CFT-specific driver (loading the
CFT JSON, sampling, writing the xlsx triplet, comparing to the prior
``One Earth *`` columns) lives in ``run_oe_hierarchical_mapping.py``.

The taxonomy xlsx location is caller-supplied: pass ``taxonomy_path``
(an xlsx) or ``taxonomy_dir`` (searched for the latest
``OE Solutions Terms *VDL.xlsx``) to ``map_to_oneearth``. There is no
built-in default path.

High-level usage
----------------
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from oe_hierarchical_taxonomy_mapping import map_to_oneearth

    with get_session() as session:
        per_row_df, collapsed_df = map_to_oneearth(
            entities=my_dataframe,
            id_col="uid",
            name_col="Name",
            text_col="Description",
            session=session,
        )

The library re-exports ``build_system_prompt`` and ``classify_entities``
from the generic engine for callers that want to drive the walk
themselves with custom level specs / prompts. All OpenAI calls flow
through the SQL prompt/response cache — see
``hierarchical_taxonomy_mapping``'s "Caching" docstring section.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

import vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping as _htm
from vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping import (
    build_system_prompt,
    classify_entities,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session


# ---------------------------------------------------------------------------
# Per-project match schema
# ---------------------------------------------------------------------------
# OE uses three modes (direct / enabling tech / indirect) for both the
# organization and research prompt variants. The Literal constrains the
# model to those three values under strict-mode structured output; the
# Field descriptions on the per-variant Match subclasses carry the
# domain-specific definitions, and the response-class docstrings carry
# the selection behavior. ``InstructorPRC`` appends the schema JSON to
# the prompt so the model sees all three layers (Literal constraint,
# Field descriptions, response-class docstring) without any prose
# scaffolding in ``build_system_prompt``.
#
# Pass the right response class to ``classify_entities(match_schema=...)``
# via ``oneearth_match_schema(...)``.

# Mode definitions — kept as plain dicts so the Literal and the
# Field descriptions derive from one source. Org and research share mode
# NAMES but have different definitions.

_OE_MODE_DEFINITIONS: dict[str, str] = {
    "direct": (
        "the entity itself deploys, operates, produces, implements, "
        "or performs the solution activity (e.g. builds solar farms, "
        "runs regenerative agriculture, manufactures low-carbon "
        "cement, restores wetlands)."
    ),
    "enabling tech": (
        "the entity develops or supplies technology, hardware, "
        "software, tools, materials, financing, or services that "
        "make the solution possible for others to deploy, but does "
        "not deploy it itself at scale (e.g. sells sensors to wind "
        "farms, builds software for grid operators, provides capital "
        "to project developers)."
    ),
    "indirect": (
        "the entity works on public policy, advocacy, awareness, "
        "education, standards, research without deployment, "
        "convening, or other non-deployment levers of change that "
        "shape whether or how the solution is adopted."
    ),
}

_OE_RESEARCH_MODE_DEFINITIONS: dict[str, str] = {
    "direct": (
        "the project itself implements, deploys, restores, "
        "sequesters, manufactures, or runs a field demonstration "
        "of the named solution at real-world scale — e.g. installs "
        "a microgrid, restores wetlands at a specific site, runs a "
        "methane-leak monitoring deployment, plants cover crops on "
        "working farms, field-trials a geoengineering intervention. "
        "This is the rarest mode in research-grant data; most "
        "research is upstream of deployment."
    ),
    "enabling tech": (
        "the project develops, validates, characterizes, "
        "prototypes, or investigates the mechanism behind a "
        "specific named technology, material, process, or practice "
        "that maps to a taxonomy node. This covers BOTH applied "
        "R&D (synthesizing battery electrolytes, designing a "
        "perovskite PV cell, building a methane-detection sensor) "
        "AND mechanism / foundational research where the abstract "
        "names both the mechanism AND a specific solution domain "
        "it informs (e.g. plant nutrient-sensing for low-fertilizer "
        "crops, soil-microbe interactions for carbon stabilization, "
        "computational methods explicitly for clean-energy materials). "
        "Most research-grant abstracts that match the taxonomy "
        "fall in this mode."
    ),
    "indirect": (
        "the project produces field-building infrastructure or "
        "soft levers for the climate field — climate-science "
        "instrumentation that supports many solutions, satellite "
        "monitoring, lifecycle assessments, multi-pillar policy / "
        "regulatory analysis, capacity-building research, "
        "education / outreach research. Typically Cross-Cutting; "
        "use this mode when the research output is a tool or "
        "framework rather than the development of a specific "
        "named solution."
    ),
}

# Both variants share the same Literal — keys must agree across the
# two definition dicts. (3.11+: Literal[*dict.keys()] unpacking.)
assert tuple(_OE_MODE_DEFINITIONS) == tuple(_OE_RESEARCH_MODE_DEFINITIONS), (
    "OE org and research mode dicts must share key order"
)
_OE_MODE = Literal[*_OE_MODE_DEFINITIONS]


def _mode_field(definitions: dict[str, str]):
    """Build the Field for ``mode_of_operation`` with definitions inline."""
    return Field(
        description=(
            "How the entity relates to the matched candidate. Pick the "
            "mode that best fits the entity's primary relationship.\n"
            + "\n".join(f"- {k}: {v}" for k, v in definitions.items())
        ),
    )


_INDEX_FIELD = Field(
    description=(
        "The 1-based position of the matched candidate in the numbered "
        "candidate list shown in the user prompt. Never return 0 or an "
        "out-of-range index — to indicate no match, return `matches: []`."
    ),
)
_EVIDENCE_FIELD = Field(
    default="",
    description=(
        "Phrase(s) from the entity description that support the match. "
        "Quote or closely paraphrase the supporting language. If no "
        "language in the description supports the candidate, omit the "
        "match — do not invent evidence."
    ),
)
_REASON_FIELD = Field(
    default="",
    description=(
        "One sentence explaining how the evidence maps to the candidate's "
        "definition."
    ),
)


class OneEarthMatch(BaseModel):
    """Per-match shape for the organization OE prompt (no confidence)."""

    index: int = _INDEX_FIELD
    mode_of_operation: _OE_MODE = _mode_field(_OE_MODE_DEFINITIONS)
    evidence: str = _EVIDENCE_FIELD
    reason: str = _REASON_FIELD


class OneEarthMatchesResponse(BaseModel):
    """Default OE selection behavior: emit only the matches the
    description clearly supports — self-filter weak candidates. A match
    requires a specific phrase from the description that names the
    candidate's activity."""

    matches: list[OneEarthMatch] = []


class OneEarthMatchWithConfidence(OneEarthMatch):
    """Per-match shape with confidence enabled (used with
    ``confidence_threshold``)."""

    confidence: float = Field(
        description=(
            "Confidence in [0, 1] that the description supports this "
            "match. 1 = the description names the activity explicitly; "
            "lower values reflect weaker or more inferential support."
        ),
    )


class OneEarthMatchesWithConfidenceResponse(BaseModel):
    """Confidence-enabled OE selection behavior: surface every candidate
    the description could plausibly support — including weak ones, with
    low confidence — rather than self-filtering. A downstream threshold
    decides which to keep. Do not omit a plausible candidate just because
    it is uncertain; emit it with a low confidence value instead."""

    matches: list[OneEarthMatchWithConfidence] = []


class OneEarthResearchMatch(BaseModel):
    """Per-match shape for the research-project OE prompt."""

    index: int = _INDEX_FIELD
    mode_of_operation: _OE_MODE = _mode_field(_OE_RESEARCH_MODE_DEFINITIONS)
    evidence: str = _EVIDENCE_FIELD
    reason: str = _REASON_FIELD


class OneEarthResearchMatchesResponse(BaseModel):
    """Research-prompt selection behavior: a match requires the abstract
    to name a research activity (development, validation, mechanism
    investigation, etc.) targeting the candidate's domain — not just
    passing climate vocabulary in a broader-impacts statement."""

    matches: list[OneEarthResearchMatch] = []


def oneearth_match_schema(
    *,
    include_confidence: bool = False,
    research: bool = False,
) -> type[BaseModel]:
    """Return the OE match-response Pydantic class for a given variant.

    ``include_confidence`` enables the confidence field (only meaningful
    for the organization prompt). ``research=True`` returns the
    research-project variant, which uses different mode definitions and
    does not support confidence.
    """
    if research:
        if include_confidence:
            raise ValueError(
                "research variant does not support include_confidence yet"
            )
        return OneEarthResearchMatchesResponse
    return (
        OneEarthMatchesWithConfidenceResponse if include_confidence
        else OneEarthMatchesResponse
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL = "gpt-5.4-nano"
DESCENT_FANOUT_CAP = 3
DEFAULT_WORKERS = 32  # I/O-bound on the OpenAI API; large pools are fine.

# Per-pillar sheets that carry Sub-Term-level rows. The Cross-Cutting
# pillar has no per-pillar sheet, so its Solutions naturally have no
# Sub-Terms — the walk leafs at Solution there.
PILLAR_DETAIL_SHEETS = (
    "Energy",
    "Regenerative Ag",
    "Nature Conservation",
    "Geo-Engineering",
)


# ---------------------------------------------------------------------------
# Latest-version discovery
# ---------------------------------------------------------------------------

def find_latest_taxonomy(
    taxonomy_dir: Path,
) -> Path:
    """Return the latest VDL-edited OE Solutions Terms xlsx in ``taxonomy_dir``.

    Picks files matching ``OE Solutions Terms *VDL.xlsx`` and chooses the
    one with the largest 8-digit YYYYMMDD prefix in the trailing token.
    Files with no date (e.g. ``OE Solutions Terms VDL.xlsx``) sort
    earliest so any dated file beats them.
    """
    candidates = list(taxonomy_dir.glob("OE Solutions Terms *VDL.xlsx"))
    if not candidates:
        raise FileNotFoundError(
            f"No OE Solutions Terms *VDL.xlsx file found in {taxonomy_dir}"
        )

    def date_key(p: Path) -> str:
        # The dated names look like 'OE Solutions Terms 20250502_expanded_VDL.xlsx';
        # the bare 'OE Solutions Terms VDL.xlsx' has no date and should sort
        # earliest. Pull the leftmost 8-digit run from the stem.
        m = re.search(r"\d{8}", p.stem)
        return m.group(0) if m else "00000000"

    return max(candidates, key=date_key)


# ---------------------------------------------------------------------------
# One Earth level spec
# ---------------------------------------------------------------------------
# A 4-level walk: Pillar -> Sub-Pillar -> Solution -> Sub-Term.
#
# The default child filters work because each level's parent-side column
# in the source data uses the same name as the parent's key_col:
#   Sub-Pillar rows carry "Pillar"
#   Solution rows carry "Pillar" + "Sub-Pillar"
#   Sub-Term rows (synthesized below) carry "Pillar" + "Sub-Pillar" + "Solution"
# So no child_filter_col / child_filter_value_col overrides are needed.
#
# The level-3 sheet name is a placeholder; ``load_taxonomy`` below
# builds the Sub-Term frame in memory rather than reading a single sheet.

ONEEARTH_LEVELS = [
    {
        "idx": 0, "name": "Pillar",
        "sheet": "Pillars", "key_col": "Pillar",
        "output_col": "Pillar",
        "parent_filters": [],
    },
    {
        "idx": 1, "name": "SubPillar",
        "sheet": "SubPillars", "key_col": "Sub-Pillar",
        "output_col": "Sub-Pillar",
        "parent_filters": ["Pillar"],
    },
    {
        "idx": 2, "name": "Solution",
        "sheet": "Solutions", "key_col": "Solution",
        "output_col": "Solution",
        "parent_filters": ["Pillar", "Sub-Pillar"],
    },
    {
        "idx": 3, "name": "SubTerm",
        "sheet": "_SubTerms_synthetic", "key_col": "Sub-Term",
        "output_col": "Sub-Term",
        "parent_filters": ["Pillar", "Sub-Pillar", "Solution"],
    },
]


# ---------------------------------------------------------------------------
# One Earth system prompt — assembled from generic skeleton + OE overrides
# ---------------------------------------------------------------------------
# The prose prompt has two customization layers: a domain intro and rule
# overrides (``evidence_only`` and ``multiple_matches`` use the engine's
# defaults). The mode-of-operation definitions and the per-match response
# shape live on the Pydantic ``OneEarthMatch`` classes above, NOT in this
# prose — see ``oneearth_match_schema``.

ONEEARTH_DOMAIN_INTRO = (
    "You are classifying an organization against the One Earth Climate "
    "Solutions Framework, a hierarchical taxonomy of climate-action "
    "solutions for keeping global warming below 1.5°C. The taxonomy has "
    "four levels — Pillar, Sub-Pillar, Solution, Sub-Term — nested under "
    "five top-level pillars:\n"
    "- Energy Transition: clean renewable power, heat, transport, "
    "electrification, energy storage, EV infrastructure, building energy "
    "systems, and industrial decarbonization.\n"
    "- Nature Conservation: protecting, restoring, and connecting "
    "forests, grasslands, wetlands, peatlands, and oceans.\n"
    "- Regenerative Agriculture: soil-restoring farming, reduced "
    "chemical inputs, and food-system transformation, including food "
    "waste reduction.\n"
    "- Geo-Engineering: intentional large-scale interventions in Earth "
    "systems such as carbon dioxide removal or solar radiation "
    "management.\n"
    "- Cross-Cutting: field-building and enabling work (tools, data, "
    "monitoring, policy, finance, legal, education, advocacy) that "
    "spans two or more of the other four pillars. It is a leaf with no "
    "sub-pillars or solutions of its own, not a fallback bucket.\n"
    "Return an empty match list when the organization's work falls "
    "outside all five pillars — e.g. general social services, "
    "non-climate health, sports, consumer products with no climate "
    "angle, or generic economic development."
)

# NOTE: mode-of-operation definitions live in ``_OE_MODE_DEFINITIONS`` at
# the top of this file (consumed by the ``OneEarthMatch.mode_of_operation``
# Field description). No standalone ``ONEEARTH_MODES`` constant.

ONEEARTH_RULE_OVERRIDES = {
    "domain_relevance": (
        "Climate-action relevance. Match a candidate when the description "
        "names activities aligned with the candidate's definition. The "
        "taxonomy already establishes the climate mechanism for each "
        "candidate — decide whether the description names the activity, "
        "not whether the activity helps the climate. An organization's "
        "products, services, or stated mission are sufficient evidence at "
        "the Pillar / Sub-Pillar level even when the climate mechanism is "
        "left implicit.\n"
        "The activity classes below are IN SCOPE and map as shown. These "
        "inclusions grant entry at the Pillar / Sub-Pillar level only; "
        "selecting a specific Solution or Sub-Term still requires the "
        "description to name that specific activity (see the specificity "
        "rule):\n"
        "- Low-carbon or circular materials, recycling, and material "
        "substitution (bioplastics, recycled or bio-based fibers, "
        "low-carbon cement / chemicals, battery or e-waste recycling) -> "
        "Energy Transition (industrial decarbonization).\n"
        "- Food-system work: regenerative, urban, or indoor / controlled-"
        "environment farming, agroecology, local / organic food systems, "
        "alternative proteins, food-waste reduction / rescue / upcycling, "
        "composting, and sustainable fibers or textile recycling "
        "(fibersheds) -> Regenerative Agriculture.\n"
        "- Ecosystem protection, restoration, or connection (forests, "
        "grasslands, wetlands, peatlands, oceans, coastal habitats, urban "
        "green infrastructure) and conservation-enabling work "
        "(biodiversity data, conservation finance, monitoring, policy, "
        "education) -> Nature Conservation.\n"
        "- Carbon dioxide removal or capture (direct air capture, "
        "mineralization or carbon-storing materials, enhanced weathering, "
        "ocean or biogenic CDR) and carbon-credit / MRV infrastructure "
        "for capture -> Geo-Engineering.\n"
        "- Climate field-building that enables others (data platforms, "
        "monitoring, finance, policy, legal, education, advocacy, "
        "incubators) -> placed by scope; see the cross-pillar rule.\n"
        "Out of scope: fossil-fuel extraction, processing, or transport; "
        "conventional chemical-input agribusiness; consumer goods carrying "
        "only a 'sustainable' / 'green' label with no climate mechanism; "
        "general social services, recreation, emergency services, "
        "non-environmental health or education, and generic economic "
        "development. When the description is too thin to commit even to a "
        "Pillar, return `matches: []`.\n"
        "At the Pillar level, bias plausible but uncertain matches toward "
        "inclusion. The specificity and qualifier cautions in the other "
        "rules govern the choice of a Solution or Sub-Term — they do not "
        "justify withholding a Pillar because the description names no "
        "specific technology, practice, or qualifier."
    ),
    "cross_sector": (
        "No cross-pillar inference. Assign a pillar only for an activity the "
        "entity itself performs, supplies, or advances. An entity that "
        "supplies a low-carbon input or service to another pillar stays in "
        "its own pillar unless the description says it also operates in that "
        "pillar."
    ),
    "prominence": (
        "Prominence. Match a Pillar or Sub-Pillar for any activity that is "
        "the entity's own work — its products, services, or stated mission "
        "— even if described in a single phrase or sentence; do not require "
        "multiple sentences or \"primary focus\" framing at these levels. "
        "What fails this bar is an activity that is not the entity's own "
        "(e.g. \"our clients include solar firms\") or a throwaway mention "
        "of something otherwise absent from the description. At the Solution "
        "/ Sub-Term levels, a specifically named technology or practice is "
        "likewise sufficient even if briefly mentioned."
    ),
}

# Cross-pillar routing — disambiguation for activities that could plausibly
# fit more than one pillar. Definitions hold scope; the prompt holds routing.
# Appended as its own labeled section (build_system_prompt only emits the
# fixed PROMPT_RULE_KEYS). Mirrored 1:1 in the judge's scope intro.
ONEEARTH_CROSS_PILLAR_ROUTING = (
    "Cross-pillar routing. When work could plausibly fit more than one "
    "pillar, route it as follows:\n"
    "- Field-builders / ecosystem-builders whose own work is enabling tools "
    "(data, monitoring, finance, science) or enabling conditions (policy, "
    "legal, education, advocacy, incubation): bounded to ONE primary pillar "
    "-> that pillar's `Cross-Cutting <Pillar>` sub-pillar; spanning TWO OR "
    "MORE primary pillars -> the top-level Cross-Cutting pillar. Cross-Cutting "
    "is never a fallback for a poor fit — an entity that builds, deploys, or "
    "supplies a SPECIFIC solution belongs to that solution's pillar.\n"
    "- Carbon capture, transport, storage, or removal — including "
    "point-source capture at fossil or industrial facilities — is "
    "Geo-Engineering / Engineered CDR, not Energy Transition, whenever that "
    "is the primary climate mechanism.\n"
    "- Classify by product or activity, not feedstock: bio-based, "
    "biodegradable, or recycled plastics and organic / green chemicals are "
    "Energy Transition (Organic Chemicals & Plastics) even when made from "
    "agricultural or organic-waste feedstock; fibers, textiles, and clothing "
    "are Regenerative Agriculture (circular fibersheds); food and farming "
    "products are Regenerative Agriculture; materials whose primary purpose "
    "is capturing or sequestering CO2 are Geo-Engineering."
)


def build_oneearth_system_prompt(include_confidence: bool = False) -> str:
    """Assemble the canonical One Earth organization system prompt (prose only).

    Generic prose skeleton + OE rule overrides + the standalone
    cross-pillar routing section. The mode definitions, the confidence
    semantics, and the selection-vs-self-filter behavior all live on the
    Pydantic response class (selected by
    ``oneearth_match_schema(include_confidence=...)``) — pass that same
    class to ``classify_entities(match_schema=...)`` so the model sees
    the schema's Field descriptions + response-class docstring via
    InstructorPRC's schema append.

    ``include_confidence`` no longer affects the prose (confidence is a
    schema-only concern now); the org prompt is identical either way. The
    parameter is retained so existing call sites don't break — pick the
    confidence-enabled SCHEMA via ``oneearth_match_schema`` instead.
    """
    prompt = build_system_prompt(
        levels=ONEEARTH_LEVELS,
        domain_intro=ONEEARTH_DOMAIN_INTRO,
        rules=ONEEARTH_RULE_OVERRIDES,
    )
    return prompt + "\n\n" + ONEEARTH_CROSS_PILLAR_ROUTING


ONEEARTH_SYSTEM_PROMPT = build_oneearth_system_prompt()


# ---------------------------------------------------------------------------
# Research-project prompt variant
# ---------------------------------------------------------------------------
# The default prompt above is calibrated for organizations (companies,
# NGOs, land trusts) — its mode definitions describe what an "entity"
# does and its rule overrides are full of organization-shaped examples
# ("land trust", "Audubon-style organization", "regenerative-farming
# co-op"). Research-grant abstracts (NSF, NIH, USAspending) describe
# investigations rather than operations, and the org-tuned framing
# causes the LLM to miss applied / mechanism research that legitimately
# maps to deployment-shaped taxonomy nodes.
#
# The research variant moves as a coordinated triple: the domain intro
# and rule overrides below (prose) plus the ``OneEarthResearchMatch``
# schema above (whose ``mode_of_operation`` Field reworded the mode
# definitions around research roles, keeping the canonical mode names).
# Callers select the variant via ``map_to_oneearth(prompt_mode=
# "research")``, which pairs ``ONEEARTH_RESEARCH_SYSTEM_PROMPT`` with
# ``oneearth_match_schema(research=True)``. Mixing the org prose with the
# research schema (or vice versa) produces incoherent prompts.

ONEEARTH_RESEARCH_DOMAIN_INTRO = (
    "You are classifying a description of a scientific research project "
    "(e.g. an NSF, NIH, or USAspending grant abstract) against the One "
    "Earth Climate Solutions Framework — a hierarchical taxonomy of "
    "climate-action solutions designed to keep global warming below "
    "1.5°C. The taxonomy is organized into five pillars: Energy "
    "Transition (clean renewable power, heat, transport, "
    "electrification, batteries / energy storage, EV infrastructure, "
    "building energy systems, and industrial decarbonization), Nature "
    "Conservation (protecting, restoring, and connecting forests, "
    "grasslands, wetlands, peatlands, and oceans), Regenerative "
    "Agriculture (soil-restoring farming, reduced chemical inputs, and "
    "food-system transformation), Cross-Cutting (research that spans "
    "multiple primary pillars OR provides field-building tools / "
    "enabling conditions across the climate field — typically climate "
    "science, monitoring, modeling, policy, finance, or education), "
    "and Geo-Engineering (intentional large-scale interventions in "
    "Earth systems such as carbon dioxide removal or solar radiation "
    "management).\n\n"
    "Your task: identify which taxonomy nodes the research project "
    "investigates, develops, validates, demonstrates, or otherwise "
    "advances. The project does NOT have to deploy the named solution "
    "to count as a match — research that develops methodology, "
    "validates a component, characterizes a material, models a system, "
    "or investigates the underlying mechanism of a recognized solution "
    "all match. Match by what the project's research activity "
    "actually IS, not by speculative downstream applications. Every "
    "taxonomy node describes a specific climate-action solution; the "
    "match must be to a named research target, not to general "
    "sustainability framing in a broader-impacts statement."
)

# Research-shaped mode definitions, but using the engine's three
# canonical mode names (direct / enabling tech / indirect) so the
# engine's hardcoded validation accepts the values without warning and
# downstream mode-aware logic (e.g. indirect-fanout stop) keeps working.
# Research-flavored mode definitions live in
# ``_OE_RESEARCH_MODE_DEFINITIONS`` at the top of this file (consumed by
# the ``OneEarthResearchMatch.mode_of_operation`` Field description).

ONEEARTH_RESEARCH_RULE_OVERRIDES = {
    "domain_relevance": (
        "Climate-action relevance for research. Match a candidate when "
        "the abstract names a research activity (development, "
        "validation, characterization, modeling, mechanism "
        "investigation, field demonstration) on the candidate's "
        "domain. The taxonomy already establishes the climate "
        "mechanism for each candidate — your job is to decide whether "
        "the abstract names the research activity that maps to the "
        "candidate, not to re-derive whether the underlying solution "
        "helps stabilize the climate.\n"
        "Important: inclusions below grant entry at the Pillar / "
        "Sub-Pillar level only. Selecting a specific Solution or "
        "Sub-Term still requires the abstract to name that specific "
        "research target — see the specificity rule.\n"
        "- Research on ecosystem protection or restoration (forests, "
        "grasslands, wetlands, peatlands, mangroves, coastal habitats, "
        "watersheds, coral reefs, urban green infrastructure) counts "
        "as a Nature Conservation match even when the abstract does "
        "not say 'carbon' or 'GHG' — the carbon-sink mechanism is "
        "established by the taxonomy itself.\n"
        "- Research on climate-data, analytics, risk-modeling, "
        "monitoring, satellite observation, and geospatial tools "
        "counts as field_building / Cross-Cutting when the work "
        "supports many solutions rather than one.\n"
        "- Research on local food systems, agroecology, or food-system "
        "transformation counts as Regenerative Agriculture at the "
        "Pillar / Sub-Pillar level when sustainable practices are "
        "named as the research target.\n"
        "- Mechanism research counts as a match ONLY when the abstract "
        "names BOTH (a) the mechanism / method, AND (b) a specific "
        "solution-domain that depends on it. Plant biology that names "
        "'breeding low-fertilizer crops' as the application is a "
        "match; plant biology with no named application is NOT a "
        "match.\n"
        "Out of scope: research that mentions climate vocabulary in "
        "passing without making it the project's research target; "
        "biology / chemistry / medicine that uses 'carbon' or 'CO2' in "
        "non-atmospheric contexts (medical CO2 removal, organic "
        "carbon chemistry unrelated to atmospheric carbon, biological "
        "respiration); pure curiosity-driven science with no named "
        "solution; basic-impact statements that name 'sustainability' "
        "as a downstream possibility without making it a research "
        "target; conferences, symposia, training grants, workforce "
        "programs; thin USAspending stub records that describe "
        "funding mechanisms (IRA / IIJA / 5339(C)) without naming a "
        "research activity. When the abstract is too thin to commit "
        "to even a Pillar, return `matches: []`."
    ),
    "cross_sector": (
        "No cross-pillar inference, and no Cross-Cutting fallback. "
        "Assign a Pillar only if the project itself investigates, "
        "develops, or validates the climate activity in that Pillar. "
        "A research project that develops a low-emission INPUT used by "
        "another sector stays in its own primary Pillar — it does not "
        "get pulled into the destination sector's Pillar — unless the "
        "abstract says the project also investigates that destination.\n"
        "TWO DISTINCT SENSES of 'cross-cutting' exist in this taxonomy "
        "— do not conflate them:\n"
        "  (A) Top-level Cross-Cutting Pillar — for projects whose work "
        "      genuinely spans multiple PRIMARY PILLARS (Energy "
        "      Transition × Nature Conservation × Regenerative "
        "      Agriculture × Geo-Engineering). Examples: integrated "
        "      land-energy system modeling that combines forest carbon "
        "      with renewable siting; a climate finance platform that "
        "      funds both Reg Ag and Energy Transition work; satellite "
        "      monitoring that serves both Nature Conservation AND "
        "      Energy Transition use cases. The defining test: list "
        "      the primary pillars the project's stated work touches. "
        "      If that list has >= 2 entries, top-level Cross-Cutting "
        "      may fit.\n"
        "  (B) Cross-Cutting Sub-Pillars inside one primary pillar "
        "      (Cross-Cutting Energy / Cross-Cutting Nature / Cross-"
        "      Cutting Regen Ag) — for projects that span multiple "
        "      SOLUTIONS within ONE primary pillar, or that build the "
        "      field for that pillar. Per the taxonomy definitions:\n"
        "      • Cross-Cutting Regen Ag spans multiple Regen Ag "
        "        Solutions (regenerative croplands, sustainable "
        "        rangelands, food waste reduction, circular "
        "        fibersheds) — including supply-chain tools, food-"
        "        system data platforms, ag-policy analysis, farmer "
        "        education / extension that touches several practices, "
        "        food-system AI institutes, planetarian-diet research, "
        "        meal-planning / food-waste apps.\n"
        "      • Cross-Cutting Energy spans multiple energy Solutions "
        "        (renewable power + renewable heat + renewable transport "
        "        + energy efficiency) — including grid-modeling that "
        "        crosses generation modes, cleantech accelerators "
        "        funding multiple energy solutions, multi-mode "
        "        decarbonization planning tools.\n"
        "      • Cross-Cutting Nature spans multiple nature Solutions "
        "        (land conservation + ocean conservation + ecosystem "
        "        restoration + wildlife connectivity) — including "
        "        multi-ecosystem monitoring, multi-habitat policy "
        "        frameworks, conservation-finance platforms.\n"
        "DECISION RULE: Before picking top-level Cross-Cutting Pillar, "
        "identify the primary pillar(s) the project's stated work "
        "lives in. If the project's scope sits entirely within ONE "
        "primary pillar's domain (e.g. 'the food system' = Regen Ag; "
        "'the energy transition' = Energy Transition; 'biodiversity / "
        "ecosystems' = Nature Conservation), descend into THAT pillar "
        "and pick its Cross-Cutting Sub-Pillar, NOT the top-level "
        "Cross-Cutting Pillar. Use top-level Cross-Cutting Pillar only "
        "when the work genuinely crosses pillar boundaries (e.g., "
        "energy + nature, energy + agriculture, all-pillar climate "
        "science).\n"
        "WITHIN-PILLAR SUB-PILLAR RULE: When a project is a horizontal "
        "platform within ONE primary pillar — i.e., an institute / "
        "research center / data platform / AI or ML system / MRV or "
        "monitoring system / policy framework / financing mechanism / "
        "supply-chain or systems-level effort — and its stated mission "
        "targets MULTIPLE Sub-Pillars or multiple Solutions within "
        "that pillar, route to the pillar's Cross-Cutting Sub-Pillar "
        "(Cross-Cutting Energy / Cross-Cutting Nature / Cross-Cutting "
        "Regen Ag) rather than to whichever narrow Sub-Pillar has the "
        "strongest literal-text match in the abstract. Signals that "
        "trigger this routing: mission statements that name 2+ "
        "downstream activities ('breeding AND production AND supply "
        "chain AND consumer'; 'generation AND transmission AND end-"
        "use efficiency'; 'multiple ecosystems'); explicit framings "
        "like 'transform US food systems', 'decarbonize the energy "
        "system', 'transform biodiversity monitoring'; institute / "
        "center / consortium scope language; horizontal enabling "
        "technologies (AI, sensors, digital twins, satellite, finance, "
        "policy) applied across several levers within the pillar. Do "
        "NOT collapse such a project onto its narrowest literal match "
        "(e.g. picking Food Waste Reduction just because the abstract "
        "lists 'eliminating food waste' as one of four mission goals). "
        "Conversely, when a project genuinely targets ONLY one named "
        "lever (one specific solution, one ecosystem, one technology), "
        "stay at that narrow Sub-Pillar / Solution — do not promote it "
        "to Cross-Cutting.\n"
        "A research project on a SPECIFIC named solution belongs to "
        "that solution's primary Pillar:\n"
        "- Battery chemistry, EV powertrain, heat-pump, smart-grid, "
        "or industrial-decarbonization research → Energy Transition "
        "(NOT Cross-Cutting).\n"
        "- Wetland-restoration ecology, marine conservation science, "
        "or land-trust research → Nature Conservation (NOT Cross-"
        "Cutting).\n"
        "- Soil microbiome, cover-crop, regenerative-grazing, "
        "alternative-protein research, food-waste R&D, planetarian-"
        "diet studies → Regenerative Agriculture (NOT top-level "
        "Cross-Cutting).\n"
        "- Low-carbon materials, recycling, or industrial chemistry "
        "research stays in its primary Pillar (typically Energy "
        "Transition for industrial decarb), or returns no Pillar "
        "match if no clean home — it does NOT become top-level "
        "Cross-Cutting by default.\n"
        "Pick top-level Cross-Cutting Pillar only when the abstract "
        "names genuine cross-pillar integration or all-field enabling "
        "tools. When in doubt between top-level Cross-Cutting and a "
        "within-pillar Cross-Cutting Sub-Pillar, prefer the within-"
        "pillar Sub-Pillar."
    ),
    "specificity": (
        "Specificity must match the level. A candidate is selectable "
        "only when the abstract names a research target specific "
        "enough to support it. Broad research framing ('renewable "
        "energy systems', 'climate-resilient agriculture', "
        "'natural climate solutions', 'sustainable materials', "
        "'low-carbon chemistry') can support a Pillar but is NOT "
        "sufficient for a narrower Sub-Pillar, Solution, or Sub-Term "
        "unless the abstract also names the specific technology, "
        "ecosystem, organism, or practice the child covers.\n"
        "Research projects often name their methodology specifically "
        "(e.g. 'metalloprotein-inspired catalysts for CO2 reduction'). "
        "The methodology name supports the Solution / Sub-Term level "
        "when it maps cleanly. When the abstract describes basic "
        "mechanism research without naming a specific downstream "
        "solution, leaf at the Pillar / Sub-Pillar — do NOT descend "
        "without specific evidence.\n"
        "Energy Transition example: 'renewable energy materials' "
        "supports the Pillar; selecting Solar PV requires "
        "solar/photovoltaic/silicon/perovskite or other PV-specific "
        "terms in the abstract.\n"
        "Nature Conservation example: 'habitat restoration' or "
        "'biodiversity conservation' supports the Nature Conservation "
        "Pillar (and often a broad Sub-Pillar like Land Conservation), "
        "but does NOT support a specific Solution or Sub-Term. "
        "Selecting Wetland Restoration requires the abstract to name "
        "wetlands / salt marsh / mangrove / peatland AND the "
        "restoration activity. Selecting Species Rewilding requires "
        "'reintroduces extirpated species'. Selecting Invasive "
        "Species Management requires 'removes / controls invasive "
        "species'.\n"
        "Regenerative Agriculture example: 'sustainable farming' or "
        "'soil health' supports the Pillar; selecting Cover Crops "
        "requires 'cover crops' or 'planting cover between cash "
        "crops'; No-till requires 'no-till' or 'reduced tillage'; "
        "Agroforestry requires 'integrating trees with crops or "
        "livestock'.\n"
        "When in doubt, return `matches: []` and let the walk stop "
        "one level higher."
    ),
    "qualifier_lock": (
        "Qualifier lock. Qualifiers in a candidate's name are "
        "mandatory constraints, not flavor — Utility-Scale, "
        "Distributed, Residential, Onshore, Offshore, Coastal, "
        "Tropical, Temperate, Boreal, etc. The abstract must support "
        "the qualifier. If the abstract is silent on the qualifier, "
        "do NOT select the candidate. Research on 'solar PV' without "
        "naming utility-scale or distributed deployment supports the "
        "Solar Solution but neither Sub-Term."
    ),
    "prominence": (
        "Prominence at Pillar and Sub-Pillar levels. The research "
        "activity must be a substantial focus of the project, not a "
        "passing mention in the broader-impacts statement. Look for "
        "the project's stated research aims, methods, and "
        "deliverables — those should name the candidate's domain. A "
        "single broader-impacts phrase like 'this work could enable "
        "sustainable agriculture' or 'broader impacts include "
        "renewable energy applications' is NOT enough at the Pillar "
        "level — the project's primary research target must be in "
        "the candidate's domain.\n"
        "At deeper levels (Solution / Sub-Term), a specifically "
        "named research target is sufficient even if briefly "
        "mentioned, because specificity itself is the signal."
    ),
    "advocacy_depth": (
        "Match depth must reflect the research's scope. A project "
        "investigating molecular mechanism of nitrogen fixation "
        "matches at the Sub-Pillar level (Regenerative Croplands) "
        "unless the abstract names a specific fertilizer-reduction "
        "practice the work supports. A project on solar-cell "
        "materials matches at the Solar Solution level unless the "
        "abstract names utility-scale or distributed deployment. The "
        "narrower the candidate, the more explicit the abstract "
        "evidence must be."
    ),
}

ONEEARTH_RESEARCH_SYSTEM_PROMPT = build_system_prompt(
    levels=ONEEARTH_LEVELS,
    domain_intro=ONEEARTH_RESEARCH_DOMAIN_INTRO,
    rules=ONEEARTH_RESEARCH_RULE_OVERRIDES,
)


# ---------------------------------------------------------------------------
# Taxonomy loader (custom — handles OE's per-pillar Sub-Term sheets)
# ---------------------------------------------------------------------------

def load_taxonomy(path: Path) -> dict[int, pd.DataFrame]:
    """Load the One Earth taxonomy at ``path`` as a level-keyed dict.

    The default ``_htm.load_taxonomy`` reads one sheet per level, but
    Sub-Terms are split across four per-pillar sheets in the source
    file. So levels 0–2 are read directly from their named sheets, and
    level 3 is composed in memory: each per-pillar sheet contributes
    its rows where ``Sub-Term`` is non-blank, with ``Sub-Term Definition``
    promoted to ``Definition``. The Cross-Cutting pillar has no per-pillar
    sheet, so its Solutions appear in level 2 with no level-3 children
    and the walk leafs at Solution there.

    Rows missing the level's ``key_col`` or ``Definition`` are dropped to
    keep the prompt clean — same contract as the engine's loader.
    """
    tables: dict[int, pd.DataFrame] = {}

    # Levels 0–2: direct read from named sheets.
    for lvl in ONEEARTH_LEVELS[:3]:
        df = pd.read_excel(path, sheet_name=lvl["sheet"])
        df = df.dropna(subset=[lvl["key_col"], "Definition"]).reset_index(drop=True)
        tables[lvl["idx"]] = df

    # Level 3: concatenate per-pillar sheets and promote Sub-Term Definition.
    sub_term_frames: list[pd.DataFrame] = []
    for sheet in PILLAR_DETAIL_SHEETS:
        sdf = pd.read_excel(path, sheet_name=sheet)
        # Drop rows with no Sub-Term value.
        sdf = sdf.dropna(subset=["Sub-Term"]).copy()
        sdf = sdf[sdf["Sub-Term"].astype(str).str.strip().ne("")]
        # Drop the solution-level "Definition" so we can promote the
        # sub-term-level definition under that name.
        if "Definition" in sdf.columns:
            sdf = sdf.drop(columns=["Definition"])
        sdf = sdf.rename(columns={"Sub-Term Definition": "Definition"})
        # Keep only the columns the walk needs.
        keep = ["Pillar", "Sub-Pillar", "Solution", "Sub-Term", "Definition"]
        sdf = sdf[[c for c in keep if c in sdf.columns]]
        sub_term_frames.append(sdf)

    sub_terms = pd.concat(sub_term_frames, ignore_index=True)
    sub_terms = sub_terms.dropna(
        subset=["Pillar", "Sub-Pillar", "Solution", "Sub-Term", "Definition"]
    )
    # Dedupe on the (Pillar, Sub-Pillar, Solution, Sub-Term) path so a
    # Sub-Term that appears in multiple pillar sheets only contributes one
    # candidate to its parent Solution.
    sub_terms = sub_terms.drop_duplicates(
        subset=["Pillar", "Sub-Pillar", "Solution", "Sub-Term"]
    ).reset_index(drop=True)

    tables[3] = sub_terms
    return tables


def collapse_to_one_row_per_uid(
    df: pd.DataFrame,
    id_col: str = "uid",
) -> pd.DataFrame:
    """Collapse a per-row OE classification frame to one row per id.

    Thin wrapper over the engine's ``collapse_to_one_row_per_uid`` bound
    to ``ONEEARTH_LEVELS``. ``id_col`` defaults to ``"uid"`` to match the
    CFT convention but accepts any column name.
    """
    return _htm.collapse_to_one_row_per_uid(df, ONEEARTH_LEVELS, id_col=id_col)


# ---------------------------------------------------------------------------
# High-level mapping entry point
# ---------------------------------------------------------------------------

_PROMPT_BY_MODE: dict[str, str] = {
    "organization": ONEEARTH_SYSTEM_PROMPT,
    "research": ONEEARTH_RESEARCH_SYSTEM_PROMPT,
}

def build_recovery_scope_prompt(taxonomy_path: Path | None = None) -> str:
    """Build the second-stage recovery's scope system prompt.

    Reuses the canonical mapping scope (``ONEEARTH_DOMAIN_INTRO`` +
    ``ONEEARTH_CROSS_PILLAR_ROUTING``) so the recovery and the classifier
    share one scope source, plus a yes/no instruction. Validated on the
    seed-42 empties at ~88% accuracy separating in-scope-but-refused from
    genuinely out-of-scope; the bare-definition scope under-recalls on
    framing-masked in-scope orgs, so the richer scope is used. ``taxonomy_path``
    is accepted for signature compatibility but unused (scope is text-based).
    """
    return (
        ONEEARTH_DOMAIN_INTRO + "\n\n" + ONEEARTH_CROSS_PILLAR_ROUTING + "\n\n"
        + "Decide whether the organization's OWN work is in scope for any "
        "pillar above (an implicit climate mechanism is acceptable; an "
        "incidental co-benefit of otherwise out-of-scope work is not). "
        "Return JSON: {\"in_scope\": true|false, \"category\": \"<exact "
        "pillar name from the list above, or null if not in scope>\", "
        "\"reason\": \"<one sentence>\"}."
    )


def map_to_oneearth(
    entities: pd.DataFrame,
    *,
    id_col: str,
    name_col: str,
    text_col: str,
    session=None,
    model: str = MODEL,
    max_workers: int = DEFAULT_WORKERS,
    taxonomy_path: Path | None = None,
    taxonomy_dir: Path | None = None,
    descent_fanout_cap: int = DESCENT_FANOUT_CAP,
    prompt_mode: str = "organization",
    system_prompt: str | None = None,
    confidence_threshold: float | None = None,
    emit_per_level: bool = False,
    recover_unmatched: bool = False,
    recovery_model: str = "gpt-4.1-mini",
    recovery_scope_prompt: str | None = None,
    walk_recovered: bool = False,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Classify a DataFrame of entities against the One Earth taxonomy.

    Top-level convenience for the common case: one DataFrame in,
    per-row + collapsed DataFrames out. Loads the latest taxonomy,
    runs the hierarchical walk, and returns both views.

    Parameters
    ----------
    entities
        DataFrame whose rows are the entities to classify. Must contain
        ``id_col``, ``name_col``, and ``text_col``. Any other columns
        are carried through to the output DataFrames untouched.
    id_col
        Column with a stable per-entity id. Used to group leaf rows in
        the collapsed view.
    name_col
        Column with the entity's name. Shown to the LLM but not used as
        evidence (see the engine's ``evidence_only`` rule).
    text_col
        Column with the entity description. The LLM matches against the
        text in this column.
    session
        SQLAlchemy session used by the SQL prompt/response cache. Build
        via ``vdl_tools.shared_tools.database_cache.database_utils.
        get_session``; the caller's ``with get_session() as session:``
        block scopes the transaction.
    model
        OpenAI model id. Default ``MODEL`` (currently ``gpt-5.4-nano``).
    max_workers
        Concurrency cap for the cache's API worker pool, applied per
        level batch. The hierarchical walk is I/O-bound on the OpenAI
        API; large pools (16, 32, 64) are fine.
    read_from_cache
        When False, bypass cached rows and re-issue every API call
        (force-refresh). Default True.
    write_to_cache
        When False, leave the cache untouched (read-only / passthrough).
        Default True.
    taxonomy_path
        Path to the taxonomy xlsx. If omitted, ``taxonomy_dir`` must be
        provided and the latest ``OE Solutions Terms *VDL.xlsx`` found in
        it (via ``find_latest_taxonomy``) is used.
    taxonomy_dir
        Directory searched for the latest ``OE Solutions Terms *VDL.xlsx``
        when ``taxonomy_path`` is not given. One of ``taxonomy_path`` /
        ``taxonomy_dir`` is required — there is no built-in default path.
    descent_fanout_cap
        Maximum number of children to descend into when a level returns
        multiple matches. Default 3.
    prompt_mode
        ``"organization"`` (default): the standard OE system prompt,
        calibrated for organizations (companies, NGOs, land trusts).
        ``"research"``: a research-project variant — framed for grant
        abstracts (NSF, NIH, USAspending). Selecting a mode pairs the
        matching prose prompt with the matching ``match_schema`` (org vs
        ``OneEarthResearchMatch``); both move together. Same taxonomy and
        walk, different framing. Ignored when ``system_prompt`` is set
        (in which case the org schema is used).
    system_prompt
        Escape hatch for callers that want to assemble a fully custom
        system prompt via ``build_system_prompt``. When provided, it
        overrides ``prompt_mode`` entirely (and the organization match
        schema is used).
    confidence_threshold
        Precision/recall knob in [0, 1]. When set (organization mode
        only), the confidence-enabled schema
        (``OneEarthMatchesWithConfidenceResponse``) is selected so the
        model emits a per-match confidence, and matches below the
        threshold are dropped. LOWER keeps more (weaker) matches — fewer
        no-mappings, more false positives; HIGHER keeps only strong
        matches. ``None`` (default) applies no confidence scoring or
        filtering.
    emit_per_level
        When True, the per-row frame gains ``<level> evidence`` /
        ``<level> reason`` / ``<level> confidence`` columns for every
        assignment along each path, not just the deepest match.
    walk_recovered
        When True (requires ``recover_unmatched=True``), entities the walk
        left empty but the recovery marked in scope get a second walk
        seeded at the recovered pillar, descending into Sub-Pillar /
        Solution / Sub-Term where the description supports it. Their
        placeholder rows are replaced by the seeded leaf rows; the
        ``recovery_*`` columns carry through, so the rows remain flagged
        as recovery-sourced.

    Returns
    -------
    (per_row_df, collapsed_df)
        ``per_row_df`` has one row per (entity, leaf) pair, with columns
        ``id_col``, ``name_col``, ``text_col``, all the entity's other
        columns, then ``Pillar`` / ``Sub-Pillar`` / ``Solution`` /
        ``Sub-Term``, ``deepest_match``, ``leaf_definition``,
        ``mode_of_operation``, ``evidence``, ``reason``.

        ``collapsed_df`` has one row per id, with each level rendered as
        a repr-encoded list of unique values per entity, plus the
        deepest non-empty level name in ``deepest_match`` (or
        ``"NoMatch"``). Non-classification columns from the input are
        carried through using each id's first non-null value.
    """
    if walk_recovered and not recover_unmatched:
        raise ValueError("walk_recovered=True requires recover_unmatched=True")

    if taxonomy_path is None:
        if taxonomy_dir is None:
            raise ValueError(
                "map_to_oneearth: pass taxonomy_path=<xlsx> or taxonomy_dir="
                "<dir searched via find_latest_taxonomy>. There is no "
                "built-in default taxonomy location."
            )
        taxonomy_path = find_latest_taxonomy(taxonomy_dir)
    tables = load_taxonomy(taxonomy_path)

    # Pick the per-match Pydantic class to match the system_prompt
    # variant. Confidence is only meaningful under the organization
    # prompt; research uses its own Match class with research-flavored
    # mode definitions.
    confidence_on = (
        confidence_threshold is not None and prompt_mode == "organization"
    )
    match_schema = oneearth_match_schema(
        include_confidence=confidence_on,
        research=(prompt_mode == "research"),
    )

    if system_prompt is None:
        if confidence_on:
            # The confidence knob needs the confidence-enabled schema.
            system_prompt = build_oneearth_system_prompt(include_confidence=True)
        elif prompt_mode not in _PROMPT_BY_MODE:
            raise ValueError(
                f"Unknown prompt_mode {prompt_mode!r}; expected one of "
                f"{sorted(_PROMPT_BY_MODE)} or pass system_prompt= explicitly."
            )
        else:
            system_prompt = _PROMPT_BY_MODE[prompt_mode]

    with get_session(session=session) as session:
        per_row_df = classify_entities(
            session=session,
            tables=tables,
            levels=ONEEARTH_LEVELS,
            system_prompt=system_prompt,
            entities=entities,
            id_col=id_col,
            name_col=name_col,
            text_col=text_col,
            model=model,
            descent_fanout_cap=descent_fanout_cap,
            max_workers=max_workers,
            confidence_threshold=confidence_threshold,
            emit_per_level=emit_per_level,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            match_schema=match_schema,
        )
        if recover_unmatched:
            # Default top-level column + category choices from the level spec +
            # tables; override only the scope prompt with the OE-specific rich one.
            per_row_df = _htm.recover_unmatched(
                per_row_df,
                session=session,
                model=recovery_model,
                id_col=id_col,
                name_col=name_col,
                text_col=text_col,
                levels=ONEEARTH_LEVELS,
                tables=tables,
                scope_prompt=recovery_scope_prompt or build_recovery_scope_prompt(
                    taxonomy_path),
                max_workers=max_workers,
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
            )

        if walk_recovered:
            per_row_df = _walk_recovered_entities(
                per_row_df, session=session, tables=tables, system_prompt=system_prompt,
                id_col=id_col, name_col=name_col, text_col=text_col, model=model,
                descent_fanout_cap=descent_fanout_cap, max_workers=max_workers,
                confidence_threshold=confidence_threshold, emit_per_level=emit_per_level,
                read_from_cache=read_from_cache, write_to_cache=write_to_cache,
                match_schema=match_schema,
            )

    collapsed_df = collapse_to_one_row_per_uid(per_row_df, id_col=id_col)
    return per_row_df, collapsed_df


def _walk_recovered_entities(
    per_row_df: pd.DataFrame, *, session, tables, system_prompt, id_col, name_col,
    text_col, model, descent_fanout_cap, max_workers, confidence_threshold,
    emit_per_level, read_from_cache=True, write_to_cache=True,
    match_schema: type[BaseModel] | None = None,
) -> pd.DataFrame:
    """Seed the walk at each recovery-recovered entity's pillar and descend.

    Entities the walk left empty but the recovery marked in-scope (with a
    pillar) get a second walk seeded at that pillar, picking up Sub-Pillar /
    Solution / Sub-Term where the description supports it. Their placeholder
    (no-match) rows are replaced by the seeded leaf rows; the recovery
    columns carry through, so these rows stay flagged as recovery-sourced.
    Entities that stay pillar-only after the descent keep the assigned pillar.
    """
    top_col = ONEEARTH_LEVELS[0]["output_col"]
    recovered_col = f"recovered_{top_col}"
    if recovered_col not in per_row_df.columns:
        return per_row_df

    is_recovered = (
        per_row_df[top_col].isna()
        & (per_row_df["recovered_in_scope"] == True)  # noqa: E712
        & per_row_df[recovered_col].notna()
    )
    rec_ids = per_row_df.loc[is_recovered, id_col].unique()
    if len(rec_ids) == 0:
        return per_row_df

    # One entity row per recovered id; strip the classification columns so the
    # seeded walk regenerates them (carry cols incl. recovery_* are kept).
    class_cols = (
        [lvl["output_col"] for lvl in ONEEARTH_LEVELS]
        + ["deepest_match", "leaf_definition", "mode_of_operation",
           "evidence", "reason", "confidence"]
    )
    for lvl in ONEEARTH_LEVELS:
        oc = lvl["output_col"]
        class_cols += [f"{oc} evidence", f"{oc} reason", f"{oc} confidence"]
    seed_input = (per_row_df[per_row_df[id_col].isin(rec_ids)]
                  .drop_duplicates(id_col)
                  .drop(columns=[c for c in class_cols if c in per_row_df.columns]))

    seeded = classify_entities(
        session=session, tables=tables, levels=ONEEARTH_LEVELS,
        system_prompt=system_prompt, entities=seed_input, id_col=id_col,
        name_col=name_col, text_col=text_col, model=model,
        descent_fanout_cap=descent_fanout_cap, max_workers=max_workers,
        confidence_threshold=confidence_threshold, emit_per_level=emit_per_level,
        seed_col=recovered_col,
        read_from_cache=read_from_cache, write_to_cache=write_to_cache,
        match_schema=match_schema,
    )

    kept = per_row_df[~per_row_df[id_col].isin(rec_ids)]
    merged = pd.concat([kept, seeded.reindex(columns=per_row_df.columns)],
                       ignore_index=True)
    return merged


# Re-exports for callers that want to drive the engine directly.
__all__ = [
    "MODEL",
    "DESCENT_FANOUT_CAP",
    "DEFAULT_WORKERS",
    "PILLAR_DETAIL_SHEETS",
    "ONEEARTH_LEVELS",
    # Organization-tuned prompt (default).
    "ONEEARTH_DOMAIN_INTRO",
    "ONEEARTH_RULE_OVERRIDES",
    "ONEEARTH_SYSTEM_PROMPT",
    # Research-project-tuned prompt (prompt_mode="research").
    "ONEEARTH_RESEARCH_DOMAIN_INTRO",
    "ONEEARTH_RESEARCH_RULE_OVERRIDES",
    "ONEEARTH_RESEARCH_SYSTEM_PROMPT",
    # Per-project Pydantic match schemas (pass to classify_entities).
    "OneEarthMatch",
    "OneEarthMatchesResponse",
    "OneEarthMatchWithConfidence",
    "OneEarthMatchesWithConfidenceResponse",
    "OneEarthResearchMatch",
    "OneEarthResearchMatchesResponse",
    "oneearth_match_schema",
    "find_latest_taxonomy",
    "load_taxonomy",
    "collapse_to_one_row_per_uid",
    "map_to_oneearth",
    # Engine re-exports
    "build_system_prompt",
    "classify_entities",
]


if __name__ == "__main__":
    entities = pd.DataFrame([
        {"id": 1, "name": "CCS", "description": "CCS is a project developer focusing on building carbon capture and storage facilities. They also make fertilizer from captured carbon dioxide."},
        {"id": 2, "name": "Electric Semis", "description": "Electric Semis is an auto manufacturer focusing on building electric semi-trucks. They also build battery storage systems."},
        {"id": 3, "name": "Forest Defense Fund", "description": "Forest Defense Fund is a non-profit organization focusing on protecting forests and wildlife."},
    ])
    results = map_to_oneearth(
        entities=entities,
        taxonomy_dir=Path("../shared-data-clean/data/taxonomies/oneearth"),
        id_col="id",
        name_col="name",
        text_col="description",
        read_from_cache=True,
        write_to_cache=True,
    )
    print(results)
