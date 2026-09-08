"""
Drawdown hierarchical taxonomy mapping — library
=================================================

Reusable classifier that maps any DataFrame of entities (companies,
organizations, projects — anything with a name and a description) onto
the four-level Drawdown climate-mitigation taxonomy:

    0. Sector                (13 nodes)
    1. SectorCluster         (35 nodes; children of a Sector)
    2. Solution              (166 nodes; children of a SectorCluster)
    3. Activity              (~1800 nodes; children of a Solution)

This is the data + prompt half, promoted from the drawdown project repo
(``drawdown/taxonomy_mapping/drawdown_hierarchical_taxonomy_mapping.py``,
which remains the CFT-network driver with its own entity loaders and
output conventions). The hierarchical-walk algorithm itself lives in
``hierarchical_taxonomy_mapping.py``.

The taxonomy xlsx location is caller-supplied: pass ``taxonomy_path``
(an xlsx with Sectors / SectorClusters / Solutions / Activities sheets)
to ``add_drawdown_hierarchical_taxonomy`` or ``load_taxonomy``. There is
no built-in default path — pin the vintage in the calling project's
paths config.

High-level usage
----------------
    from vdl_tools.shared_tools.taxonomy_mapping.drawdown_hierarchical_taxonomy_mapping import (
        add_drawdown_hierarchical_taxonomy,
    )

    df, funding_df = add_drawdown_hierarchical_taxonomy(
        df,
        id_col="id",
        text_col="text_for_one_earth",
        taxonomy_path=paths["drawdown_hierarchical_taxonomy"],
    )

All OpenAI calls flow through the SQL prompt/response cache — see
``hierarchical_taxonomy_mapping``'s "Caching" docstring section. The
system prompt below is byte-identical to the drawdown repo driver so
cached matches are shared wherever the entity text also matches.
"""

from __future__ import annotations

import contextlib
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
from vdl_tools.shared_tools.json_cache import write_json
from vdl_tools.shared_tools.tools.logger import logger


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODEL = "gpt-5.4-nano"
RECOVERY_MODEL = "gpt-4.1-mini"

# When a level returns multiple matches, descend into at most this many
# children to avoid combinatorial blow-up for multi-activity entities.
DESCENT_FANOUT_CAP = 3

# Forwarded to reasoning models (ignored by gpt-4.1-class ones). Part of the
# cache key — keep in sync with the drawdown repo driver to share its cache.
DEFAULT_LLM_API_KWARGS = {"reasoning": {"effort": "low"}}


# ---------------------------------------------------------------------------
# Drawdown level spec
# ---------------------------------------------------------------------------
# Note the SectorCluster row: its taxonomy sheet identifies nodes by the
# combined "SectorCluster" name (used in prompts), but children at the
# Solution and Activity levels filter by the bare "Cluster" column. So
# this level overrides ``child_filter_col`` and ``child_filter_value_col``
# to "Cluster". All other levels use the default (== key_col).

DRAWDOWN_LEVELS = [
    {
        "idx": 0, "name": "Sector",
        "sheet": "Sectors", "key_col": "Sector",
        "output_col": "Sector",
        "parent_filters": [],
    },
    {
        "idx": 1, "name": "SectorCluster",
        "sheet": "SectorClusters", "key_col": "SectorCluster",
        "output_col": "Cluster",
        "parent_filters": ["Sector"],
        "child_filter_col": "Cluster",
        "child_filter_value_col": "Cluster",
    },
    {
        "idx": 2, "name": "Solution",
        "sheet": "Solutions", "key_col": "Solution",
        "output_col": "Solution",
        "parent_filters": ["Sector", "Cluster"],
    },
    {
        "idx": 3, "name": "Activity",
        "sheet": "Activities", "key_col": "Activity",
        "output_col": "Activity",
        "parent_filters": ["Sector", "Cluster", "Solution"],
    },
]


# ---------------------------------------------------------------------------
# Drawdown system prompt — assembled from generic skeleton + Drawdown overrides
# ---------------------------------------------------------------------------
# The prompt is built by ``build_system_prompt`` in
# ``hierarchical_taxonomy_mapping``; only the climate-mitigation framing,
# the three modes-of-operation, and rules whose wording leans on Drawdown
# concepts (sectors, qualifier lists, indirect-mode advocacy, etc.) live
# here. Rules 2 and 5 use the generic defaults — their wording transfers.

DRAWDOWN_DOMAIN_INTRO = (
    "You are classifying an organization against a hierarchical "
    "taxonomy of CLIMATE MITIGATION solutions — approaches that "
    "reduce, prevent, or offset greenhouse gas (GHG) emissions. Every "
    "taxonomy node describes a specific climate-mitigation solution, "
    "not general sustainability, environmental cleanup, social "
    "services, or economic development."
)

DRAWDOWN_MODES = [
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


# ---------------------------------------------------------------------------
# Per-match response schema (mirrors the One Earth pattern). The engine's
# per-project ``match_schema`` carries the mode-of-operation enum; the mode
# DEFINITIONS live in the prose ``DRAWDOWN_MODE_SECTION`` below (models weight a
# prose directive more heavily than schema-field metadata), so the schema field
# holds only a pointer.
# ---------------------------------------------------------------------------

_DD_MODE_DEFINITIONS: dict[str, str] = {m["name"]: m["definition"] for m in DRAWDOWN_MODES}
_DD_MODE = Literal[*_DD_MODE_DEFINITIONS]


def _mode_field():
    return Field(
        description=(
            "How the entity relates to the matched candidate. See the "
            "'Mode of operation' section in the instructions for the mode "
            "definitions and pick the best fit."
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


class DrawdownMatch(BaseModel):
    """Per-match shape for the Drawdown classifier."""

    index: int = _INDEX_FIELD
    mode_of_operation: _DD_MODE = _mode_field()
    evidence: str = _EVIDENCE_FIELD
    reason: str = _REASON_FIELD


class DrawdownMatchesResponse(BaseModel):
    """Response shape for the Drawdown prompt: one entry per selected
    candidate. Selection, match depth, and mode of operation are governed by
    the matching rules and the 'Mode of operation' section in the system
    prompt — this class adds no selection disposition of its own."""

    matches: list[DrawdownMatch] = []


# Mode-of-operation section — kept in the PROSE prompt (not just the schema
# field), because it is an imperative classification instruction and the model
# weights a prose directive more heavily than a schema field description.
DRAWDOWN_MODE_SECTION = (
    "Mode of operation — for each selected candidate, classify HOW the entity "
    "relates to the matched candidate:\n"
    + "\n".join(f"- '{k}': {v}" for k, v in _DD_MODE_DEFINITIONS.items())
    + "\nPick the mode that best fits the entity's primary relationship to the "
    "matched candidate."
)

DRAWDOWN_RULE_OVERRIDES = {
    "domain_relevance": (
        "Climate-mitigation relevance. Match a candidate when the "
        "description names activities aligned with the candidate's "
        "definition. The taxonomy already establishes the climate-"
        "mitigation mechanism for each candidate — your job is to "
        "decide whether the description names the activity, not to "
        "re-derive whether the activity reduces emissions. "
        "Specifically:\n"
        "- Ecosystem protection or restoration (forests, grasslands, "
        "wetlands, peatlands, coastal habitats, watersheds, urban "
        "green infrastructure) counts as a match for the relevant "
        "taxonomy node even when the description does not explicitly "
        "say 'carbon' or 'GHG' — the carbon-sink mechanism is already "
        "established by the taxonomy itself.\n"
        "- Habitat / biodiversity / wildlife / land-trust / Audubon-"
        "style organizations count as Protect or Restore matches when "
        "their work involves managing intact ecosystems or rebuilding "
        "degraded ones, even when the framing is conservation rather "
        "than carbon.\n"
        "- Climate-data, analytics, risk-modeling, monitoring, and "
        "geospatial tools count as 'enabling tech' matches for the "
        "climate-mitigation decisions they inform (e.g. siting "
        "renewables, monitoring forests, tracking emissions, climate-"
        "risk disclosure).\n"
        "- Local food systems, farmers markets, and community food "
        "infrastructure count as Food, Agriculture, Land & Ocean "
        "matches when sustainable practices or food-system "
        "relocalization are named as goals.\n"
        "- Vertical farming, controlled-environment agriculture, "
        "indoor agriculture, hydroponics, aquaponics, aeroponics, and "
        "the technology providers serving them count as Food, "
        "Agriculture, Land & Ocean matches even when the description "
        "frames the work as 'sustainable' or 'efficient' rather than "
        "explicitly low-carbon.\n"
        "- Ocean cleanup, water-quality monitoring, harbor / lake / "
        "waterway / port pollution remediation, and marine debris "
        "work count as Food, Agriculture, Land & Ocean matches under "
        "the Ocean and Waterway Health framing.\n"
        "- Public transit, rail (including streetcars, light rail, "
        "metro, freight rail), bus services, micromobility, EV "
        "charging infrastructure, EV marketplaces, EV component "
        "supply, and battery materials produced for vehicle "
        "electrification count as Transportation matches even when "
        "the description does not say 'low-carbon' or 'reduce "
        "emissions' explicitly. Electrified or zero-emission "
        "transportation is the climate-mitigation mechanism the "
        "taxonomy already establishes.\n"
        "- Battery cell manufacturing, cathode / anode / electrolyte "
        "materials production, and battery recycling count as "
        "Industry, Materials & Waste matches; the same entity may "
        "also match Transportation when the description names "
        "vehicle electrification or EV applications as the use case.\n"
        "- Parks, trails, wilderness, public-lands, watershed, "
        "conservancy, land-trust, 'friends of' / volunteer "
        "stewardship, urban greening, community green space or "
        "gardens, and habitat or waterway restoration (oyster reefs, "
        "streams, dunes, wetlands) organizations count as ecosystem "
        "Protect (FALO & Nature-Based Carbon Removal) or Restore "
        "(Nature-Based Carbon Removal) matches when the entity itself "
        "manages, stewards, protects, or restores land, habitat, or "
        "ecosystems — even when the framing is conservation, "
        "recreation access, or community greening rather than carbon. "
        "Protect (FALO & Nature-Based Carbon Removal) covers "
        "safeguarding intact land; Restore (Nature-Based Carbon "
        "Removal) covers rebuilding degraded land. Match the single "
        "Sector the work most centrally supports, selecting both only "
        "for two genuinely distinct lines of work (see the FALO/NBCR "
        "guidance below); do not return no match.\n"
        "- Packaging, single-use-plastic alternatives, reusable or "
        "refillable container systems, recycling and composting "
        "programs, bio-based or compostable materials made from "
        "agricultural / food / nutshell waste, and food-waste-"
        "reduction technologies count as Industry, Materials & Waste "
        "matches (or Food, Agriculture, Land & Ocean when the activity "
        "reduces food waste at the farm or production stage).\n"
        "Out of scope: activities clearly unrelated to climate — pure "
        "social welfare, general recreation, drinking-water utility "
        "services, education unrelated to environment, fundraising or "
        "networking without programs, generic 'clean' branding "
        "without specific activity. Organizations that only provide "
        "recreation, museum or interpretation, education, research or "
        "monitoring, fundraising, or facility access ABOUT nature — "
        "without themselves managing, protecting, or restoring land or "
        "ecosystems — remain out of scope. When the description is too "
        "thin to commit to even a Sector, return `matches: []`."
    ),
    "cross_sector": (
        "No cross-sector inference. Assign a sector only if the "
        "entity itself performs the mitigation activity in that "
        "sector. An entity that supplies a lower-emission input to "
        "another sector belongs to its own sector (e.g. a low-carbon "
        "fertilizer producer is industry/materials, not agriculture), "
        "unless the description says the entity also operates in the "
        "destination sector.\n"
        "FALO & Nature-Based Carbon Removal vs Nature-Based Carbon "
        "Removal — these two Sectors are distinguished by ACTIVITY: "
        "'FALO & Nature-Based Carbon Removal' covers PROTECTION of "
        "intact natural ecosystems and carbon-sequestering agriculture; "
        "'Nature-Based Carbon Removal' covers RESTORATION of degraded "
        "or converted land back to a vegetated, forested, or "
        "functioning state. They are not opposites, but DEFAULT TO THE "
        "SINGLE Sector the description most centrally supports: if it "
        "emphasizes 'protect', 'preserve', 'conserve', 'maintain', "
        "'safeguard', or operating intact natural areas, choose 'FALO & "
        "Nature-Based Carbon Removal'; if it emphasizes 'restore', "
        "'reforest', 'rewild', 'rehabilitate', 'replant', 'revegetate', "
        "or returning degraded land to natural cover, choose 'Nature-"
        "Based Carbon Removal'. Select BOTH only when the description "
        "names two genuinely distinct, prominent lines of work — "
        "protecting one named intact area AND restoring a different "
        "named degraded area, each described in its own right. Do not "
        "select both merely because an org works on nature or uses "
        "both words in passing.\n"
        "Other Energy lock. The 'Other Energy' Sector is RESERVED "
        "for upstream and midstream fossil-fuel work: oil, gas, coal, "
        "fossil-fuel extraction, processing, refining, pipeline "
        "transmission, storage, distribution, drilling, refineries, "
        "and fugitive methane emissions from coal mines or oil/gas "
        "processing. Do NOT select 'Other Energy' unless the "
        "description explicitly names one of these. Conservation, "
        "land-trust, watershed, wildlife, agriculture, advocacy, and "
        "education organizations do NOT match Other Energy. Generic "
        "'natural resources' or 'energy' framing is NOT enough.\n"
        "Component vs deployment lock (Activity level especially). "
        "An entity that manufactures a COMPONENT, MATERIAL, or "
        "upstream INPUT for a downstream activity does NOT match the "
        "downstream Activity. Match the Activity that names what the "
        "entity itself produces or operates, not the downstream "
        "Activity that uses the entity's output. Examples (NOT an "
        "exhaustive list — apply the principle generally):\n"
        "- A battery cell, cathode, or electrolyte manufacturer "
        "matches battery-manufacturing Activities. It does NOT match "
        "'Electric Freight Trucks', 'Purpose-Built EVs', 'Mobilize "
        "Electric Cars', 'Fleet Electrification', or 'Battery "
        "Recycling' Activities unless the description names the "
        "downstream activity as the entity's own work.\n"
        "- A solar PV manufacturer or solar project developer "
        "matches solar-deployment Activities. It does NOT match "
        "'Solar VPP Aggregation', 'Hybrid Offshore Storage', 'Wind + "
        "Battery Integration', 'Building-Integrated Solar', or "
        "'Solar-Battery Integration' Activities unless the "
        "description names the specific integration / aggregation / "
        "co-located system as the entity's own work.\n"
        "- An EV propulsion-system or EV-component manufacturer does "
        "NOT match 'Electric Freight Trucks' or 'Purpose-Built EVs' "
        "unless the description says the entity manufactures or "
        "deploys complete vehicles.\n"
        "- An electricity retailer or utility-scale solar developer "
        "does NOT match 'Distributed Solar PV', 'Home EV Charging', "
        "'Smart Thermostats', or 'Building-Integrated Solar' unless "
        "the description names the specific distributed / "
        "residential / building-side activity.\n"
        "- An aircraft, rotor, or propulsion-systems developer does "
        "NOT match 'Digital Flight Tools' or 'Smarter Flight "
        "Routing' unless the description names software, analytics, "
        "or routing as the entity's own product.\n"
        "If a candidate Activity describes a downstream use case of "
        "the entity's product rather than the product itself, return "
        "an empty list at this level."
    ),
    "specificity": (
        "Specificity must match the level. A candidate is selectable "
        "only when the description names activity specific enough to "
        "support it. Broad themes ('sustainability', 'cleantech', "
        "'clean energy', 'energy efficiency', 'reduce emissions', "
        "'renewable energy', 'renewables', 'zero-carbon', 'low-"
        "carbon') can support top-level sectors but are not "
        "sufficient for a narrower child unless the description also "
        "names the specific technology, process, material, or "
        "practice the child covers. Concretely: 'renewable energy' "
        "alone supports the Electricity sector and a general "
        "renewables cluster, but does NOT support solar-, wind-, "
        "geothermal-, or hydro-specific Solutions or Activities — "
        "those require the description to name the technology (e.g. "
        "'solar', 'wind', 'geothermal').\n"
        "Categorical rule for Coastal Wetlands Solutions and "
        "Activities: Solutions and Activities under Protect Coastal "
        "Wetlands or Restore Coastal Wetlands REQUIRE the description "
        "to explicitly name a coastal-wetland ecosystem. Specifically: "
        "'Salt Marsh' Solutions/Activities require 'salt marsh' or "
        "'tidal marsh' in the description; 'Mangrove' Solutions/"
        "Activities require 'mangrove'; 'Sea Grass' Solutions/"
        "Activities require 'seagrass' or 'sea grass'; 'Tidal "
        "Wetland' Activities require 'tidal' explicitly. Generic "
        "'coastal', 'ocean', 'marine', 'water', 'aquatic', 'wildlife "
        "habitat', or 'ecosystem restoration' language is NOT enough "
        "to select any Coastal Wetlands child node. Terrestrial "
        "conservation orgs (forests, prairies, monarch habitat, land "
        "trusts on dry land) and generic ocean-policy orgs do NOT "
        "match these nodes — leaf at the SectorCluster level instead.\n"
        "Categorical rule for climatic-zone qualifiers (Boreal, "
        "Temperate, Tropical, Subtropical, Arctic, Tundra, Polar): a "
        "Solution or Activity carrying one of these qualifiers "
        "REQUIRES the description to either (a) explicitly name the "
        "climatic zone or (b) name a country or region clearly "
        "belonging to that zone. North American grasslands, prairies, "
        "and savannas (Nebraska, Kansas, Texas, Iowa, California, "
        "etc.) are TEMPERATE — not Boreal. African and Latin American "
        "savannas are typically TROPICAL or SUBTROPICAL. Boreal "
        "applies to high-latitude conifer forests in Canada, Alaska, "
        "Russia, and Scandinavia. If the description does not name "
        "the zone or a region unambiguously in that zone, do not "
        "select the qualified child — leaf one level up.\n"
        "When in doubt, return `matches: []` and let the walk stop "
        "one level higher."
    ),
    "qualifier_lock": (
        "Qualifier lock. Qualifiers in a candidate's name are "
        "mandatory constraints, not flavor. These include climatic-"
        "zone terms (Temperate, Tropical, Boreal, Arctic, Tundra, "
        "Polar), geographic scope (Onshore, Offshore, Coastal, "
        "Terrestrial, Marine), scale or deployment type (Utility-"
        "Scale, Distributed, Residential, Commercial, Industrial, "
        "Small Modular), feedstock or phase (Multi-strata, "
        "Silvopasture, First-generation, Advanced), and any similar "
        "narrowing term. If the description identifies a different "
        "qualifier, do NOT select that candidate. If the description "
        "is silent on the qualifier, do NOT select — the candidate's "
        "qualifier must be supported by the description. When no "
        "sibling's qualifier matches the description, return an "
        "empty list at this level."
    ),
    "prominence": (
        "Prominence at Sector and Cluster levels. At these two "
        "levels, select only activities that are a core line of "
        "business — a distinct area with multiple sentences, listed "
        "among the entity's main offerings, or described as a "
        "primary focus. A single incidental phrase ('also supports "
        "X', 'including X', 'in addition to Y') about an activity "
        "otherwise absent from the description is not enough at "
        "these levels. At the Solution and Activity levels, this "
        "threshold does not apply for entities directly performing "
        "or enabling the technology — a specifically named "
        "technology or practice is sufficient even if briefly "
        "mentioned. For entities whose primary mode is 'indirect' "
        "(advocacy / education / policy / awareness), the prominence "
        "threshold applies at EVERY level: a single passing phrase "
        "about a candidate's domain is not enough — the description "
        "must establish that domain as a primary focus of the "
        "entity's advocacy or programs. An education organization "
        "that mentions 'green schools' once does not match a "
        "Buildings or Electricity candidate; a parks group that "
        "mentions 'composting events' once does not match a waste-"
        "management Solution."
    ),
    "advocacy_depth": (
        "Advocacy depth lock. When the entity's primary mode of "
        "operation toward a candidate is 'indirect' (advocacy, "
        "public policy, education, awareness, organizing, legal "
        "challenges, convening, standards-setting, research without "
        "deployment), the match must sit at the level that matches "
        "the SCOPE of the advocacy named in the description, not at "
        "deeper deployment levels the entity does not itself "
        "perform. Concretely: an advocacy organization that promotes "
        "'renewable energy' in general matches at the Electricity "
        "sector or a renewables Cluster — NOT at solar/wind "
        "Solutions or Activities — unless the description names the "
        "specific Solution or Activity as the focus of that advocacy "
        "(e.g. 'campaigns specifically to expand rooftop solar "
        "incentives'). Generic phrases like 'promotes renewables', "
        "'opposes coal', or 'supports clean energy policies' name "
        "the cluster, not a Solution. The narrower the candidate, "
        "the more explicit the advocacy focus must be."
    ),
}

DRAWDOWN_SYSTEM_PROMPT = build_system_prompt(
    levels=DRAWDOWN_LEVELS,
    domain_intro=DRAWDOWN_DOMAIN_INTRO,
    rules=DRAWDOWN_RULE_OVERRIDES,
) + "\n\n" + DRAWDOWN_MODE_SECTION


# ---------------------------------------------------------------------------
# Drawdown-bound helpers
# ---------------------------------------------------------------------------

def load_taxonomy(path) -> dict[int, pd.DataFrame]:
    """Load the Drawdown taxonomy at ``path`` keyed by level idx."""
    return _htm.load_taxonomy(path, DRAWDOWN_LEVELS)


def collapse_to_one_row_per_uid(df: pd.DataFrame, id_col: str = "uid") -> pd.DataFrame:
    """Collapse a per-row Drawdown classification frame to one row per id."""
    return _htm.collapse_to_one_row_per_uid(df, DRAWDOWN_LEVELS, id_col=id_col)


# ---------------------------------------------------------------------------
# Solution-level properties carried from the taxonomy into the mapping output
# ---------------------------------------------------------------------------
# The Solutions sheet carries three solution-level attributes. We surface them
# on every solution-level match (any per-row record with a Solution, including
# Activity leaves that belong to a solution) and, on the org frame, as real
# lists aligned 1:1 with the org's ``all_level2`` solution list.

SOLUTION_PROPERTY_COLS = ["CATEGORY", "CLIMATE POLLUTANTS", "SPEED OF ACTION"]

# Org-frame column names for each property. "CATEGORY" must not collapse to
# ``drawdown_category`` — that name is taken by the primary-category mapping
# column attach_mapping_columns emits.
SOLUTION_PROPERTY_ORG_COLS = {
    "CATEGORY": "drawdown_solution_category",
    "CLIMATE POLLUTANTS": "drawdown_climate_pollutants",
    "SPEED OF ACTION": "drawdown_speed_of_action",
}


def _solution_property_lookup(tables: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Return the Solutions frame indexed by Solution with the property columns
    that are actually present (robust to taxonomies lacking some of them)."""
    sol = tables[2]  # level idx 2 == Solutions sheet
    present = [c for c in SOLUTION_PROPERTY_COLS if c in sol.columns]
    return sol.drop_duplicates(subset="Solution").set_index("Solution")[present]


def attach_solution_properties(
    per_row_df: pd.DataFrame, tables: dict[int, pd.DataFrame]
) -> pd.DataFrame:
    """Left-merge the solution property columns onto the per-row frame by Solution.

    Rows with no Solution (Sector/SectorCluster-only matches) get NaN for each.
    """
    look = _solution_property_lookup(tables)
    if "Solution" not in per_row_df.columns or look.empty:
        return per_row_df
    # drop any pre-existing same-named columns so re-runs stay idempotent
    out = per_row_df.drop(columns=[c for c in look.columns if c in per_row_df.columns])
    return out.merge(look, left_on="Solution", right_index=True, how="left")


def attach_solution_property_columns(
    df: pd.DataFrame,
    tables: dict[int, pd.DataFrame],
    solutions_list_col: str,
) -> pd.DataFrame:
    """Add one real-list property column per solution property to the org frame.

    Each list is built by mapping the org's ``solutions_list_col`` (the native
    Python list ``attach_mapping_columns`` emits for the Solution level) through
    the Solutions-sheet lookup, so it is aligned 1:1 and in order with that
    column even when solutions share a value. Column names come from
    ``SOLUTION_PROPERTY_ORG_COLS``.
    """
    look = _solution_property_lookup(tables)
    cols = [c for c in look.columns if c in SOLUTION_PROPERTY_ORG_COLS]
    if not cols or solutions_list_col not in df.columns:
        return df

    def _lists_for(sol_cell) -> dict[str, list[str]]:
        sols = list(sol_cell) if isinstance(sol_cell, (list, tuple)) else []
        out = {c: [] for c in cols}
        for s in sols:
            in_index = s in look.index
            for c in cols:
                v = look.at[s, c] if in_index else None
                out[c].append("" if v is None or pd.isna(v) else str(v))
        return out

    built = df[solutions_list_col].map(_lists_for)
    for c in cols:
        df[SOLUTION_PROPERTY_ORG_COLS[c]] = built.map(lambda d, c=c: d[c])
    return df


# ---------------------------------------------------------------------------
# Extended algorithm: second-stage scope recovery + seeded re-walk
# ---------------------------------------------------------------------------
# The precision-tuned walk leaves obliquely-described in-scope entities
# unmatched (deepest_match == NoMatch). Mirroring the OneEarth extension, we
# run an independent scope recovery over the walk-empty entities (one cheap
# call each) to separate genuinely out-of-scope from in-scope-but-refused,
# then seed a fresh walk at each recovered entity's recovered Sector and
# descend its subtree. Reuses the generic library functions
# `recover_unmatched` and `classify_entities(seed_col=...)`.

def build_drawdown_scope_prompt(tables: dict[int, pd.DataFrame] | None = None) -> str:
    """Drawdown-specific scope prompt for the empties recovery (the override).

    Reuses the classifier's domain intro + cross-sector routing so the recovery
    and the walk share one scope source, plus a yes/no JSON instruction. This is
    the "improved" override; passing scope_prompt=None to recover_unmatched
    falls back to the generic build_default_scope_prompt (bare sector
    definitions).

    The structured-output schema the recovery enforces is ``ScopeDecision``,
    whose recovered-node field is named ``category`` — the JSON instruction
    must use that name, and ``cats.get`` exact-matches the returned value
    against the Sector names, so pass ``tables`` to enumerate them (without
    the list the model can only reliably produce the two sectors the
    cross-sector rule happens to spell out).
    """
    sector_list = ""
    if tables is not None:
        names = ", ".join(str(s) for s in tables[0]["Sector"].dropna().unique())
        sector_list = f"The Sectors are: {names}.\n\n"
    return (
        DRAWDOWN_DOMAIN_INTRO + "\n\n"
        + DRAWDOWN_RULE_OVERRIDES["cross_sector"] + "\n\n"
        + sector_list
        + "Decide whether the organization's OWN work is in scope for any Sector of the "
        "Drawdown climate-mitigation taxonomy (an implicit mitigation mechanism is "
        "acceptable; an incidental co-benefit of otherwise out-of-scope work is not). "
        "Return JSON: {\"in_scope\": true|false, \"category\": \"<exact Sector name "
        "from the list above, or null if not in scope>\", \"reason\": "
        "\"<one sentence>\"}."
    )


def _walk_recovered_entities(
    per_row_df: pd.DataFrame,
    *,
    session,
    tables: dict[int, pd.DataFrame],
    id_col: str,
    name_col: str,
    text_col: str,
    model: str,
    max_workers: int,
    descent_fanout_cap: int,
    match_schema: type[BaseModel] | None = None,
    llm_api_kwargs: dict | None = None,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
) -> pd.DataFrame:
    """Seed the walk at each recovered entity's Sector and descend.

    Entities the walk left empty but the recovery marked in-scope (with a
    Sector) get a second walk seeded at that Sector, picking up SectorCluster /
    Solution / Activity where the description supports it. Their placeholder
    (no-match) rows are replaced by the seeded leaf rows; the recovered_*
    columns carry through so the rows stay flagged.
    """
    top_col = DRAWDOWN_LEVELS[0]["output_col"]  # "Sector"
    rec_col = f"recovered_{top_col}"
    if rec_col not in per_row_df.columns:
        return per_row_df

    is_recovered = (
        per_row_df[top_col].isna()
        & (per_row_df["recovered_in_scope"] == True)  # noqa: E712
        & per_row_df[rec_col].notna()
    )
    rec_ids = per_row_df.loc[is_recovered, id_col].unique()
    if len(rec_ids) == 0:
        return per_row_df

    # One entity row per recovered id; strip the classification columns so the
    # seeded walk regenerates them (carry cols incl. recovered_* are kept).
    class_cols = (
        [lvl["output_col"] for lvl in DRAWDOWN_LEVELS]
        + ["deepest_match", "leaf_definition", "mode_of_operation",
           "evidence", "reason", "confidence"]
    )
    for lvl in DRAWDOWN_LEVELS:
        oc = lvl["output_col"]
        class_cols += [f"{oc} evidence", f"{oc} reason", f"{oc} confidence"]
    seed_input = (per_row_df[per_row_df[id_col].isin(rec_ids)]
                  .drop_duplicates(id_col)
                  .drop(columns=[c for c in class_cols if c in per_row_df.columns]))

    seeded = classify_entities(
        session=session, tables=tables, levels=DRAWDOWN_LEVELS,
        system_prompt=DRAWDOWN_SYSTEM_PROMPT, entities=seed_input, id_col=id_col,
        name_col=name_col, text_col=text_col, model=model,
        descent_fanout_cap=descent_fanout_cap, max_workers=max_workers,
        seed_col=rec_col, match_schema=match_schema, filter_by_model=True,
        llm_api_kwargs=llm_api_kwargs,
        read_from_cache=read_from_cache, write_to_cache=write_to_cache,
    )

    kept = per_row_df[~per_row_df[id_col].isin(rec_ids)]
    return pd.concat([kept, seeded.reindex(columns=per_row_df.columns)],
                     ignore_index=True)


def classify_entities_extended(
    tables: dict[int, pd.DataFrame],
    entities: pd.DataFrame,
    *,
    session=None,
    id_col: str = "uid",
    name_col: str = "Name",
    text_col: str = "Description",
    model: str = MODEL,
    recovery_model: str = RECOVERY_MODEL,
    max_workers: int = 8,
    descent_fanout_cap: int = DESCENT_FANOUT_CAP,
    recover_unmatched: bool = False,
    walk_recovered: bool = False,
    use_override_scope: bool = True,
    llm_api_kwargs: dict | None = None,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
) -> pd.DataFrame:
    """Run the base walk and, optionally, the extended recovery stages.

    With both flags off this is identical to a plain `classify_entities` call.
    With `recover_unmatched`, the walk-empty entities get an independent scope
    judgment (the Drawdown override scope prompt unless
    `use_override_scope=False`). With `walk_recovered` (requires
    `recover_unmatched`), recovered entities are re-walked seeded at their
    recovered Sector.

    All model calls route through the SQL prompt/response cache. ``session`` is
    a SQLAlchemy session; when ``None`` (default) one is opened and closed
    internally. ``llm_api_kwargs`` defaults to ``DEFAULT_LLM_API_KWARGS``
    (reasoning effort low); it is forwarded to reasoning models (ignored by
    ``gpt-4.1``-class ones, e.g. the recovery model) and is part of the cache
    key.
    """
    if walk_recovered and not recover_unmatched:
        raise ValueError("walk_recovered=True requires recover_unmatched=True")

    if llm_api_kwargs is None:
        llm_api_kwargs = DEFAULT_LLM_API_KWARGS

    owns_session = session is None
    session_cm = get_session() if owns_session else contextlib.nullcontext(session)
    with session_cm as session:
        per_row_df = classify_entities(
            session=session, tables=tables, levels=DRAWDOWN_LEVELS,
            system_prompt=DRAWDOWN_SYSTEM_PROMPT, entities=entities,
            id_col=id_col, name_col=name_col, text_col=text_col, model=model,
            descent_fanout_cap=descent_fanout_cap, max_workers=max_workers,
            match_schema=DrawdownMatchesResponse, filter_by_model=True,
            llm_api_kwargs=llm_api_kwargs,
            read_from_cache=read_from_cache, write_to_cache=write_to_cache,
        )

        if recover_unmatched:
            per_row_df = _htm.recover_unmatched(
                per_row_df, session=session, model=recovery_model,
                id_col=id_col, name_col=name_col, text_col=text_col,
                levels=DRAWDOWN_LEVELS, tables=tables,
                scope_prompt=build_drawdown_scope_prompt(tables) if use_override_scope else None,
                filter_by_model=True,
                max_workers=max_workers, llm_api_kwargs=llm_api_kwargs,
                read_from_cache=read_from_cache, write_to_cache=write_to_cache,
            )

        if walk_recovered:
            per_row_df = _walk_recovered_entities(
                per_row_df, session=session, tables=tables,
                id_col=id_col, name_col=name_col, text_col=text_col,
                model=model, max_workers=max_workers,
                descent_fanout_cap=descent_fanout_cap,
                match_schema=DrawdownMatchesResponse,
                llm_api_kwargs=llm_api_kwargs,
                read_from_cache=read_from_cache, write_to_cache=write_to_cache,
            )

    return per_row_df


def _seed_recovered_sectors_for_funding(per_row_df: pd.DataFrame) -> pd.DataFrame:
    """Funding-only view of ``per_row_df`` with recovered sectors filled in.

    With ``recover_unmatched=True`` and ``walk_recovered=False``, an in-scope
    recovery leaves its sector in ``recovered_Sector`` while the level columns
    stay empty, so ``distribute_funding_from_matches`` would drop the entity
    and its funding would silently vanish from the sector totals. Returns a
    copy with ``Sector`` seeded from ``recovered_Sector`` for those rows so
    they enter funding at sector depth. The caller's frame is untouched —
    the per-row results keep walk evidence and recovery guesses separate.
    (Mirror of the OE module's ``_seed_recovered_pillars_for_funding``.)
    """
    top_col = DRAWDOWN_LEVELS[0]["output_col"]
    recovered_col = f"recovered_{top_col}"
    if recovered_col not in per_row_df.columns:
        return per_row_df
    out = per_row_df.copy()
    sector = out[top_col]
    # Same emptiness test as recover_unmatched's own unmatched-id scan.
    empty = sector.isna() | sector.astype(str).str.strip().isin(["", "None", "nan"])
    mask = out["recovered_in_scope"].eq(True) & out[recovered_col].notna() & empty
    out.loc[mask, top_col] = out.loc[mask, recovered_col]
    return out


# ---------------------------------------------------------------------------
# Pipeline-facing wrapper (legacy column schema + persisted results)
# ---------------------------------------------------------------------------

def add_drawdown_hierarchical_taxonomy(
    df: pd.DataFrame,
    *,
    id_col: str,
    text_col: str,
    name_col: str = "Organization",
    taxonomy_path,
    mapping_name: str = "drawdown_category",
    results_path=None,
    distributed_funding_results_path=None,
    model: str = MODEL,
    recovery_model: str = RECOVERY_MODEL,
    recover_unmatched: bool = True,
    walk_recovered: bool = True,
    max_workers: int = 8,
    descent_fanout_cap: int = DESCENT_FANOUT_CAP,
    max_distr_funding_level: int = 2,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    llm_api_kwargs: dict | None = None,
    attach_property_cols: bool = True,
    session=None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map orgs to Drawdown and attach the legacy mapping-column schema.

    Drawdown sibling of ``oe_hierarchical_taxonomy_mapping.
    add_one_earth_hierarchical_taxonomy`` — same contract. One org-level
    DataFrame in, the same DataFrame back with the mapping columns attached
    (``drawdown_category`` / ``cat_level_{name}`` / ``level{i}_{name}`` /
    ``all_level{i}_{name}`` via ``attach_mapping_columns``), plus the
    distributed-funding frame. There are no ``pct``/``sim`` columns: the
    walk has no embedding scores.

    Parameters
    ----------
    df
        Org-level DataFrame. Must contain ``id_col``, ``name_col``,
        ``text_col``. Mapping columns are attached to it in place.
    id_col, text_col, name_col
        Column names for the org id, description text, and org name.
    taxonomy_path
        The pinned Drawdown taxonomy xlsx (required — pin the vintage in
        the calling project's paths config).
    mapping_name
        Suffix for the attached mapping columns. Default
        ``"drawdown_category"``.
    results_path
        When set, the per-row mapping results (classification columns
        only, keyed by ``id_col``) are written here as JSON (local or
        ``s3://``).
    distributed_funding_results_path
        When set, the distributed-funding frame is written here as JSON
        (local or ``s3://``).
    recover_unmatched, walk_recovered
        The extended recovery stages, both on by default. With
        ``recover_unmatched=True`` and ``walk_recovered=False``, the
        funding frame is rebuilt from a sector-seeded view so recovered
        entities still enter funding at sector depth (the mapping columns
        keep walk evidence only).
    attach_property_cols
        When True (default), also attach the solution property list
        columns (``drawdown_solution_category`` /
        ``drawdown_climate_pollutants`` / ``drawdown_speed_of_action``),
        aligned 1:1 with ``all_level2_{mapping_name}``.

    Returns
    -------
    (df, distributed_funding_df)
        ``df`` with the mapping columns attached, and the per-(org, path)
        ``FundingFrac`` frame (summing to 1.0 per org — see
        ``distribute_funding_from_matches``).
    """
    tables = load_taxonomy(taxonomy_path)

    # Classify on a slim frame: the walk's carry columns aren't needed (mapping
    # columns are joined back onto df by id), and a full org frame can collide
    # with the walk's output columns — the CFT meta carries a Crunchbase
    # "Sector" column that would shadow the taxonomy's Sector level.
    entities = df[[id_col, name_col, text_col]].copy()

    # The origin driver's entity loaders dropped description-less rows;
    # enforce the same precondition here so blank-text rows don't pay walk +
    # recovery LLM calls (the engine would embed a literal 'nan' as the
    # description). Skipped rows simply get no mapping columns, like any
    # unmatched org.
    text = entities[text_col]
    blank = text.isna() | (text.astype(str).str.strip() == "")
    if blank.any():
        logger.warning(
            "Skipping %d of %d entities with empty %r text; they get no "
            "Drawdown mapping", int(blank.sum()), len(entities), text_col,
        )
        entities = entities[~blank]

    per_row_df = classify_entities_extended(
        tables,
        entities,
        session=session,
        id_col=id_col,
        name_col=name_col,
        text_col=text_col,
        model=model,
        recovery_model=recovery_model,
        max_workers=max_workers,
        descent_fanout_cap=descent_fanout_cap,
        recover_unmatched=recover_unmatched,
        walk_recovered=walk_recovered,
        llm_api_kwargs=llm_api_kwargs,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
    )

    per_row_df = attach_solution_properties(per_row_df, tables)

    if results_path:
        # Persist every walk output column. Compute the set against the slim
        # entities frame, NOT the caller's df — a name collision (the CFT
        # meta's Crunchbase "Sector" column) would silently drop that
        # classification column from the artifact.
        new_columns = list(per_row_df.columns.difference([id_col, name_col, text_col]))
        if not str(results_path).startswith("s3://"):
            Path(results_path).parent.mkdir(parents=True, exist_ok=True)
        per_row_df[[id_col, name_col, text_col] + new_columns].to_json(
            results_path, orient="records"
        )
        logger.info("Wrote %s per-row Drawdown mapping rows to %s",
                    len(per_row_df), results_path)

    df, distributed_funding_df, _ = _htm.add_hierarchical_taxonomy_mapping(
        df=df,
        levels=DRAWDOWN_LEVELS,
        id_col=id_col,
        name_col=name_col,
        mapping_name=mapping_name,
        per_row_mapping_df=per_row_df,
        max_distr_funding_level=max_distr_funding_level,
    )

    if recover_unmatched and not walk_recovered:
        # Without the re-walk, recovered in-scope entities have empty level
        # columns and would be dropped from the funding frame; rebuild it from
        # a sector-seeded view so their dollars land at sector depth (the
        # mapping columns above keep walk evidence only, matching OE).
        distributed_funding_df = _htm.distribute_funding_from_matches(
            _seed_recovered_sectors_for_funding(per_row_df),
            DRAWDOWN_LEVELS,
            id_col,
            name_col,
            max_level=max_distr_funding_level,
        )

    if distributed_funding_df is not None and distributed_funding_results_path:
        if not str(distributed_funding_results_path).startswith("s3://"):
            Path(distributed_funding_results_path).parent.mkdir(
                parents=True, exist_ok=True
            )
        write_json(
            distributed_funding_df.to_dict(orient="records"),
            distributed_funding_results_path,
        )
        logger.info("Wrote %s distributed-funding rows to %s",
                    len(distributed_funding_df), distributed_funding_results_path)

    if attach_property_cols:
        df = attach_solution_property_columns(
            df, tables, solutions_list_col=f"all_level2_{mapping_name}"
        )

    return df, distributed_funding_df


# Re-exports for callers that want to drive the engine directly.
__all__ = [
    "MODEL",
    "RECOVERY_MODEL",
    "DESCENT_FANOUT_CAP",
    "DEFAULT_LLM_API_KWARGS",
    "DRAWDOWN_LEVELS",
    "DRAWDOWN_DOMAIN_INTRO",
    "DRAWDOWN_MODES",
    "DRAWDOWN_MODE_SECTION",
    "DRAWDOWN_RULE_OVERRIDES",
    "DRAWDOWN_SYSTEM_PROMPT",
    "DrawdownMatch",
    "DrawdownMatchesResponse",
    "SOLUTION_PROPERTY_COLS",
    "SOLUTION_PROPERTY_ORG_COLS",
    "load_taxonomy",
    "collapse_to_one_row_per_uid",
    "attach_solution_properties",
    "attach_solution_property_columns",
    "build_drawdown_scope_prompt",
    "classify_entities_extended",
    "add_drawdown_hierarchical_taxonomy",
    # Engine re-exports
    "build_system_prompt",
    "classify_entities",
]
