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

Eventual home: ``vdl_tools/shared_tools/taxonomy_mapping/``. Once moved
there, ``SHARED_TAXONOMY_DIR`` should be relativized via
``vdl_tools.shared_tools.project_config`` rather than the absolute path
used here for now.

High-level usage
----------------
    from openai import OpenAI
    from oe_hierarchical_taxonomy_mapping import map_to_oneearth

    client = OpenAI(api_key=...)
    per_row_df, collapsed_df = map_to_oneearth(
        entities=my_dataframe,
        id_col="uid",
        name_col="Name",
        text_col="Description",
        client=client,
    )

The library re-exports ``build_system_prompt``, ``classify_entities``,
``classify_entity`` from the generic engine for callers that want to
drive the walk themselves with custom level specs / prompts.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from openai import OpenAI

import vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping as _htm
from vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping import (
    build_system_prompt,
    classify_entities,
    classify_entity,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# TODO: relativize SHARED_TAXONOMY_DIR via vdl_tools.shared_tools.project_config
# when this file moves to vdl_tools/shared_tools/taxonomy_mapping/.
SHARED_TAXONOMY_DIR = Path(
    "/Users/rjw/Dropbox/VDL/shared-data/data/taxonomies/oneearth"
)

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
    taxonomy_dir: Path = SHARED_TAXONOMY_DIR,
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
# Same three-layer customization the Drawdown driver uses: domain intro,
# modes-of-operation (copied verbatim — definitions are domain-agnostic),
# and rule overrides. ``evidence_only`` and ``multiple_matches`` use the
# engine's defaults.

ONEEARTH_DOMAIN_INTRO = (
    "You are classifying an organization against the One Earth Climate "
    "Solutions Framework — a hierarchical taxonomy of climate-action "
    "solutions designed to keep global warming below 1.5°C. The "
    "taxonomy is organized into five pillars: Energy Transition (clean "
    "renewable power, heat, transport, electrification, batteries / "
    "energy storage, EV infrastructure, building energy systems, and "
    "industrial decarbonization), Nature Conservation (protecting, "
    "restoring, and connecting forests, grasslands, wetlands, "
    "peatlands, and oceans), Regenerative Agriculture (soil-restoring "
    "farming, reduced chemical inputs, and food-system transformation, "
    "INCLUDING food waste reduction and food recovery / rescue), "
    "Geo-Engineering (intentional large-scale interventions in Earth "
    "systems such as carbon dioxide removal or solar radiation "
    "management), and Cross-Cutting. "
    ""
    "Field-builders / ecosystem-builders — entities whose work is "
    "enabling tools (data platforms, monitoring, science, financial "
    "tools) or enabling conditions (policy, legal, educational, "
    "cultural, advocacy, or community-action work) rather than direct "
    "deployment — are placed by scope of impact: a field-builder whose "
    "work is bounded to a single primary pillar belongs in that "
    "pillar's `Cross-Cutting <Pillar>` sub-pillar; a field-builder "
    "whose work spans two or more of the four primary pillars (Energy "
    "Transition, Nature Conservation, Regenerative Agriculture, "
    "Geo-Engineering) belongs in the top-level Cross-Cutting pillar. "
    "The Cross-Cutting pillar is a leaf with no sub-pillars or "
    "solutions of its own; it is NOT a fallback bucket for entities "
    "that don't fit cleanly elsewhere. "
    ""
    "Concrete examples of single-pillar field-builders (these go in "
    "`Cross-Cutting <Pillar>`, NOT no-match): "
    "(a) a natural-history museum, nature center, or zoo society "
    "promoting conservation education and species awareness → Nature "
    "Conservation → Cross-Cutting Nature → Nature Education & "
    "Communication; "
    "(b) a coastal-stewardship nonprofit running outreach and advocacy "
    "for marine/coastal protection → Nature Conservation → "
    "Cross-Cutting Nature → Nature Community Action or Education & "
    "Communication; "
    "(c) a sustainable-transit / bike / walkability advocacy coalition "
    "pushing mode-shift policy → Energy Transition → Cross-Cutting "
    "Energy → Energy Policy & Governance or Community Action; "
    "(d) an energy-policy think tank → Energy Transition → "
    "Cross-Cutting Energy → Energy Policy & Governance; "
    "(e) an agricultural-business support organization helping "
    "regenerative or local food producers → Regenerative Agriculture "
    "→ Cross-Cutting Regen Ag. "
    ""
    "Note also that some org types map to DIRECT pillar solutions, not "
    "to `Cross-Cutting <Pillar>`: electric micromobility / electric "
    "vehicles / e-bikes / EV charging → Energy Transition → Renewable "
    "Transport; food-waste-reduction and food-rescue nonprofits → "
    "Regenerative Agriculture → Food Waste Reduction (a primary "
    "sub-pillar). "
    ""
    "Refuse to assign a pillar (empty result) ONLY when the "
    "organization's work is genuinely outside all five pillars — e.g., "
    "general social services, consumer products with no climate angle, "
    "sports, non-climate health, generic economic development, or "
    "individual-animal welfare (which is distinct from ecosystem-scale "
    "nature conservation). Every taxonomy node describes a specific "
    "climate-action solution, not general sustainability, social "
    "services, or economic development."
)

# Copied verbatim from the Drawdown driver — the definitions are
# domain-agnostic and transfer cleanly.
ONEEARTH_MODES = [
    {
        "name": "direct",
        "definition": (
            "the entity itself deploys, operates, produces, implements, "
            "or performs the solution activity (e.g. builds solar farms, "
            "runs regenerative agriculture, manufactures low-carbon "
            "cement, restores wetlands)."
        ),
    },
    {
        "name": "enabling tech",
        "definition": (
            "the entity develops or supplies technology, hardware, "
            "software, tools, materials, financing, or services that "
            "make the solution possible for others to deploy, but does "
            "not deploy it itself at scale (e.g. sells sensors to wind "
            "farms, builds software for grid operators, provides capital "
            "to project developers)."
        ),
    },
    {
        "name": "indirect",
        "definition": (
            "the entity works on public policy, advocacy, awareness, "
            "education, standards, research without deployment, "
            "convening, or other non-deployment levers of change that "
            "shape whether or how the solution is adopted."
        ),
    },
]

ONEEARTH_RULE_OVERRIDES = {
    "domain_relevance": (
        "Climate-action relevance. Match a candidate when the "
        "description names activities aligned with the candidate's "
        "definition. The taxonomy already establishes the climate "
        "mechanism for each candidate — your job is to decide whether "
        "the description names the activity, not to re-derive whether "
        "the activity helps stabilize the climate.\n"
        "Important: the inclusions below grant entry at the Pillar / "
        "Sub-Pillar level only. Selecting a specific Solution or "
        "Sub-Term still requires the description to name that specific "
        "activity — see the specificity rule for the depth-of-evidence "
        "constraint that always applies.\n"
        "- Ecosystem protection or restoration (forests, grasslands, "
        "wetlands, peatlands, mangroves, coastal habitats, watersheds, "
        "coral reefs, urban green infrastructure) counts as a Nature "
        "Conservation match even when the description does not "
        "explicitly say 'carbon' or 'GHG' — the carbon-sink mechanism "
        "is already established by the taxonomy itself.\n"
        "- Habitat / biodiversity / wildlife / land-trust / Audubon-"
        "style organizations count as Nature Conservation matches at "
        "the Pillar / Sub-Pillar level when their work involves "
        "managing intact ecosystems or rebuilding degraded ones, even "
        "when the framing is conservation rather than carbon.\n"
        "- Climate-data, analytics, risk-modeling, monitoring, and "
        "geospatial tools count as 'enabling tech' matches for the "
        "climate decisions they inform (e.g. siting renewables, "
        "monitoring forests, tracking emissions, climate-risk "
        "disclosure).\n"
        "- Local food systems, farmers markets, agroecology, and "
        "community food infrastructure count as Regenerative "
        "Agriculture matches at the Pillar / Sub-Pillar level when "
        "sustainable practices or food-system transformation are named "
        "as goals.\n"
        "Out of scope: activities clearly unrelated to climate — pure "
        "social welfare, general recreation, drinking-water utility "
        "services, education unrelated to environment, fundraising or "
        "networking without programs, generic 'clean' or 'green' "
        "branding without specific activity. When the description is "
        "too thin to commit to even a Pillar, return `matches: []`."
    ),
    "cross_sector": (
        "No cross-pillar inference, and no Cross-Cutting fallback. "
        "Assign a Pillar only if the entity itself performs the "
        "climate activity in that Pillar. An entity that supplies a "
        "lower-emission input to another sector stays in its own "
        "primary Pillar — it does not get pulled into the destination "
        "Pillar — unless the description says the entity also operates "
        "in the destination Pillar.\n"
        "Critically, Cross-Cutting is NOT a default for entities that "
        "aren't a clean fit elsewhere. Cross-Cutting is reserved for "
        "(a) entities that genuinely work across multiple primary "
        "pillars (e.g. an organization whose programs span both "
        "Renewable Power and Forest Conservation), or (b) field-"
        "builders / ecosystem-builders that provide enabling tools "
        "(data platforms, monitoring, climate science, financial tools) "
        "or enabling conditions (policy, legal, education, culture) "
        "for the climate field broadly. A company that builds, "
        "operates, deploys, or supplies a SPECIFIC climate solution "
        "belongs to that solution's primary Pillar:\n"
        "- A battery-storage company, EV-charging operator, building-"
        "HVAC vendor, heat-pump installer, smart-grid software vendor, "
        "or industrial-decarbonization technology provider is Energy "
        "Transition (NOT Cross-Cutting).\n"
        "- A land trust, wildlife group, marine conservation org, or "
        "ecosystem-restoration company is Nature Conservation (NOT "
        "Cross-Cutting).\n"
        "- A regenerative-farming co-op, cover-crop seed supplier, or "
        "alternative-protein producer is Regenerative Agriculture "
        "(NOT Cross-Cutting).\n"
        "- A low-carbon materials, recycling, or chemicals company "
        "stays in whichever primary Pillar its specific activity sits "
        "in (typically Energy Transition for industrial decarb), or "
        "returns no Pillar match if it has no clear home — it does "
        "not become Cross-Cutting by default.\n"
        "Pick Cross-Cutting only when the description names "
        "field-building, advocacy across pillars, or enabling tools / "
        "enabling conditions as the entity's own work. When in doubt, "
        "prefer no match over a Cross-Cutting fallback."
    ),
    "specificity": (
        "Specificity must match the level. A candidate is selectable "
        "only when the description names activity specific enough to "
        "support it. Broad themes ('sustainability', 'cleantech', "
        "'clean energy', 'energy efficiency', 'renewables', 'nature-"
        "based solutions', 'regenerative practices', 'low-carbon') can "
        "support a Pillar but are not sufficient for a narrower "
        "Sub-Pillar, Solution, or Sub-Term unless the description also "
        "names the specific technology, process, material, ecosystem, "
        "or practice the child covers.\n"
        "Energy Transition example: 'renewable energy' alone supports "
        "the Pillar and a Renewable Power Sub-Pillar, but does NOT "
        "support solar-, wind-, geothermal-, or hydro-specific "
        "Solutions or Sub-Terms — those require the description to "
        "name the technology ('solar', 'wind', 'geothermal'). Sub-Terms "
        "in particular often add a narrower qualifier (utility-scale, "
        "distributed, offshore, tropical) that must be explicitly "
        "supported.\n"
        "Nature Conservation example: generic phrases like 'habitat "
        "protection', 'wildlife conservation', 'land conservation', "
        "'watershed protection', 'biodiversity', 'environmental "
        "stewardship', 'protecting nature', or 'preserving open space' "
        "support the Nature Conservation Pillar (and often a broad "
        "Sub-Pillar like Land Conservation), but they do NOT by "
        "themselves support any specific Solution or Sub-Term in that "
        "Pillar.\n"
        "Categorical rule for Ecosystem Restoration Sub-Pillar: a "
        "specific Solution or Sub-Term under Ecosystem Restoration "
        "REQUIRES the description to name BOTH (a) the specific "
        "ecosystem being restored — e.g. wetlands, salt marshes, "
        "mangroves, peatlands, rivers / streams, riparian areas, "
        "estuaries, forests, grasslands, prairies, coral reefs, kelp "
        "forests, oyster reefs, dunes — AND (b) the restoration / "
        "management activity. Generic 'habitat restoration', 'habitat "
        "enhancement', 'ecological restoration', 'ecosystem "
        "rehabilitation', or 'restoration projects' language supports "
        "the Sub-Pillar but NOT a specific child. Same for adjacent "
        "named activities: Species Rewilding requires 'reintroduces "
        "extirpated species'; Hydrological Restoration requires "
        "'restores natural water flow / hydrology'; Invasive Species "
        "Management / Vegetation Management requires 'removes / "
        "controls invasive species' or 'manages vegetation'; Erosion "
        "Control requires explicit erosion or sediment language. A "
        "wildlife refuge, conservation trust, garden club, or fishery "
        "enhancement group whose description only says 'restores "
        "habitat' or 'enhances habitat' should leaf at the Sub-Pillar.\n"
        "Categorical rule for Land Conservation Sub-Pillar: Land Trust "
        "requires 'acquires land or holds easements'; Protected Lands "
        "requires 'manages a designated reserve / national park / "
        "formally protected area'; Land Corridors / Wildlife "
        "Connectivity requires explicit corridor or connectivity "
        "language; Indigenous Tenure requires explicit Indigenous "
        "land-rights language.\n"
        "Regenerative Agriculture example: 'sustainable farming', "
        "'regenerative practices', or 'soil health' support the "
        "Pillar / Sub-Pillar but NOT a specific child.\n"
        "Categorical rule for Regenerative Croplands Sub-Pillar: a "
        "specific Solution or Sub-Term REQUIRES the description to "
        "name the specific practice — Cover Crops requires 'cover "
        "crops' or 'planting cover between cash crops'; No-till / "
        "Reduced Tillage requires 'no-till', 'reduced tillage', or "
        "'minimum disturbance'; Microbial Inoculants requires "
        "'microbial inoculants' or 'beneficial microbes'; Agroforestry "
        "requires 'integrating trees with crops or livestock'; "
        "Polyculture / Silvopasture / Multi-strata each require their "
        "specific cropping system named; Dryland Irrigation requires "
        "explicit irrigation language; Abandoned Farmland Restoration "
        "requires 'abandoned' or 'degraded' farmland language; "
        "Perennial Crops & Superfoods requires explicit perennial-crop "
        "language. A community garden, school program, soil-health "
        "education org, or general agricultural-services org that does "
        "not name a specific practice should leaf at the Sub-Pillar.\n"
        "When in doubt, return `matches: []` and let the walk stop "
        "one level higher."
    ),
    "qualifier_lock": (
        "Qualifier lock. Qualifiers in a candidate's name are "
        "mandatory constraints, not flavor. They include:\n"
        "- Scale / deployment type: Utility-Scale, Distributed, "
        "Residential, Commercial, Industrial, Small Modular.\n"
        "- Geographic scope: Onshore, Offshore, Coastal, Terrestrial, "
        "Marine.\n"
        "- Climatic / biome terms: Temperate, Tropical, Boreal, Arctic, "
        "Polar, Tundra.\n"
        "- Ecosystem type — a Solution or Sub-Term named for a specific "
        "ecosystem REQUIRES that ecosystem to appear in the "
        "description: Wetland(s), Salt Marsh, Mangrove, Peatland, "
        "Bog, Riparian, River, Stream, Estuary, Coral Reef, Kelp "
        "Forest, Oyster Reef, Seagrass, Dune, Forest, Grassland, "
        "Prairie, Savanna, Tundra. 'Habitat restoration' or 'ecosystem "
        "restoration' alone is NOT enough — the specific ecosystem "
        "must be named.\n"
        "- Practice type — a Solution or Sub-Term named for a specific "
        "agricultural / land-management practice REQUIRES that "
        "practice to appear in the description: Cover Crop, No-Till, "
        "Reduced Tillage, Microbial Inoculant, Biochar, Compost, "
        "Agroforestry, Silvopasture, Polyculture, Multi-strata, "
        "Perennial Crop, Crop Rotation, Managed Grazing, Holistic "
        "Grazing, Erosion Control, Conservation Tillage. 'Regenerative "
        "practices' or 'soil health' alone is NOT enough.\n"
        "- Feedstock / phase: First-generation, Advanced, etc.\n"
        "If the description identifies a different qualifier, do NOT "
        "select that candidate. If the description is silent on the "
        "qualifier, do NOT select — the candidate's qualifier must be "
        "supported by the description. When no sibling's qualifier "
        "matches the description, return an empty list at this level."
    ),
    "prominence": (
        "Prominence at Pillar and Sub-Pillar levels. At these two "
        "levels, select only activities that are a core line of "
        "business — a distinct area with multiple sentences, listed "
        "among the entity's main offerings, or described as a primary "
        "focus. A single incidental phrase ('also supports X', "
        "'including X', 'in addition to Y') about an activity "
        "otherwise absent from the description is not enough at these "
        "levels. At the Solution and Sub-Term levels, this threshold "
        "does not apply for entities directly performing or enabling "
        "the technology — a specifically named technology or practice "
        "is sufficient even if briefly mentioned. For entities whose "
        "primary mode is 'indirect' (advocacy / education / policy / "
        "awareness), the prominence threshold applies at EVERY level: "
        "a single passing phrase about a candidate's domain is not "
        "enough — the description must establish that domain as a "
        "primary focus of the entity's advocacy or programs."
    ),
    "advocacy_depth": (
        "Advocacy depth lock. When the entity's primary mode of "
        "operation toward a candidate is 'indirect' (advocacy, public "
        "policy, education, awareness, organizing, legal challenges, "
        "convening, standards-setting, research without deployment), "
        "the match must sit at the level that matches the SCOPE of "
        "the advocacy named in the description, not at deeper levels "
        "the entity does not itself perform. Concretely: an advocacy "
        "organization that promotes 'renewable energy' in general "
        "matches at the Energy Transition Pillar or a Renewable Power "
        "Sub-Pillar — NOT at solar/wind Solutions or Sub-Terms — "
        "unless the description names the specific Solution or "
        "Sub-Term as the focus of that advocacy (e.g. 'campaigns "
        "specifically to expand rooftop solar incentives'). Generic "
        "phrases like 'promotes renewables', 'opposes coal', "
        "'protects forests', or 'supports clean energy policies' name "
        "the Pillar or Sub-Pillar, not a Solution. The narrower the "
        "candidate, the more explicit the advocacy focus must be."
    ),
}

ONEEARTH_SYSTEM_PROMPT = build_system_prompt(
    levels=ONEEARTH_LEVELS,
    domain_intro=ONEEARTH_DOMAIN_INTRO,
    modes=ONEEARTH_MODES,
    rules=ONEEARTH_RULE_OVERRIDES,
)


# ---------------------------------------------------------------------------
# Research-project prompt variant
# ---------------------------------------------------------------------------
# The default prompt above is calibrated for organizations (companies,
# NGOs, land trusts) — its modes describe what an "entity" does and its
# rule overrides are full of organization-shaped examples ("land trust",
# "Audubon-style organization", "regenerative-farming co-op"). Research-
# grant abstracts (NSF, NIH, USAspending) describe investigations rather
# than operations, and the org-tuned framing causes the LLM to miss
# applied / mechanism research that legitimately maps to deployment-
# shaped taxonomy nodes.
#
# The constants below provide a coordinated alternative — domain intro
# framed for research projects, modes-of-operation reworded around
# research roles (while keeping the engine's canonical mode names), and
# rule overrides with research-shaped examples and exclusions. Callers
# select the variant via ``map_to_oneearth(prompt_mode="research")``.
# All three pieces move as a triple; mixing the org domain_intro with
# research modes (or vice versa) produces incoherent prompts.

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
ONEEARTH_RESEARCH_MODES = [
    {
        "name": "direct",
        "definition": (
            "the project itself implements, deploys, restores, "
            "sequesters, manufactures, or runs a field demonstration "
            "of the named solution at real-world scale — e.g. installs "
            "a microgrid, restores wetlands at a specific site, runs a "
            "methane-leak monitoring deployment, plants cover crops on "
            "working farms, field-trials a geoengineering intervention. "
            "This is the rarest mode in research-grant data; most "
            "research is upstream of deployment."
        ),
    },
    {
        "name": "enabling tech",
        "definition": (
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
    },
    {
        "name": "indirect",
        "definition": (
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
    },
]

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
    modes=ONEEARTH_RESEARCH_MODES,
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


def map_to_oneearth(
    entities: pd.DataFrame,
    *,
    id_col: str,
    name_col: str,
    text_col: str,
    client: OpenAI,
    model: str = MODEL,
    max_workers: int = DEFAULT_WORKERS,
    taxonomy_path: Path | None = None,
    descent_fanout_cap: int = DESCENT_FANOUT_CAP,
    prompt_mode: str = "organization",
    system_prompt: str | None = None,
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
    client
        An ``openai.OpenAI`` client. Caller supplies it so this library
        does not depend on a project-specific config-file location.
    model
        OpenAI model id. Default ``MODEL`` (currently ``gpt-5.4-nano``).
    max_workers
        Thread pool size for parallel classification. The hierarchical
        walk is I/O-bound on the OpenAI API; large pools (16, 32, 64)
        are fine. Use 1 for a single-threaded debug path.
    taxonomy_path
        Optional override for the taxonomy xlsx. Defaults to
        ``find_latest_taxonomy()`` which picks the latest VDL-edited
        ``OE Solutions Terms *VDL.xlsx`` from ``SHARED_TAXONOMY_DIR``.
    descent_fanout_cap
        Maximum number of children to descend into when a level returns
        multiple matches. Default 3.
    prompt_mode
        ``"organization"`` (default): the standard OE system prompt,
        calibrated for organizations (companies, NGOs, land trusts).
        ``"research"``: a research-project variant that swaps domain
        intro, modes-of-operation, and rule overrides as a coordinated
        triple — framed for grant abstracts (NSF, NIH, USAspending).
        Same taxonomy and walk, different framing. The three pieces
        move together; mixing org and research framing produces
        incoherent prompts. Ignored when ``system_prompt`` is set.
    system_prompt
        Escape hatch for callers that want to assemble a fully custom
        system prompt via ``build_system_prompt``. When provided, it
        overrides ``prompt_mode`` entirely.

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
    if taxonomy_path is None:
        taxonomy_path = find_latest_taxonomy()
    tables = load_taxonomy(taxonomy_path)

    if system_prompt is None:
        if prompt_mode not in _PROMPT_BY_MODE:
            raise ValueError(
                f"Unknown prompt_mode {prompt_mode!r}; expected one of "
                f"{sorted(_PROMPT_BY_MODE)} or pass system_prompt= explicitly."
            )
        system_prompt = _PROMPT_BY_MODE[prompt_mode]

    per_row_df = classify_entities(
        client=client,
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
    )
    collapsed_df = collapse_to_one_row_per_uid(per_row_df, id_col=id_col)
    return per_row_df, collapsed_df


# Re-exports for callers that want to drive the engine directly.
__all__ = [
    "MODEL",
    "DESCENT_FANOUT_CAP",
    "DEFAULT_WORKERS",
    "SHARED_TAXONOMY_DIR",
    "PILLAR_DETAIL_SHEETS",
    "ONEEARTH_LEVELS",
    # Organization-tuned prompt (default).
    "ONEEARTH_DOMAIN_INTRO",
    "ONEEARTH_MODES",
    "ONEEARTH_RULE_OVERRIDES",
    "ONEEARTH_SYSTEM_PROMPT",
    # Research-project-tuned prompt (prompt_mode="research").
    "ONEEARTH_RESEARCH_DOMAIN_INTRO",
    "ONEEARTH_RESEARCH_MODES",
    "ONEEARTH_RESEARCH_RULE_OVERRIDES",
    "ONEEARTH_RESEARCH_SYSTEM_PROMPT",
    "find_latest_taxonomy",
    "load_taxonomy",
    "collapse_to_one_row_per_uid",
    "map_to_oneearth",
    # Engine re-exports
    "build_system_prompt",
    "classify_entities",
    "classify_entity",
]
