"""Exact question templates ported from oneearth; see SOURCE_REVISIONS.md."""

NODE_QUESTION = (
    "Does this organization's own work (its products, services, programs, or stated "
    'mission) fall under the {level} "{name}"{parent}? Definition of "{name}": {definition}'
)

QUESTION_VARIANTS = ("base", "cc_condition", "cc_criteria", "cc_routing")

CC_PREFIX = "Cross-Cutting"

CC_CONDITION = (
    " Answer yes only if the organization's work also qualifies for the Sub-Pillar "
    '"{subpillar}". Definition of "{subpillar}": {subpillar_definition}'
)

CC_CRITERIA_TRUE = (
    'The organization\'s work fits the definition of "{name}" and also qualifies for '
    'the Sub-Pillar "{subpillar}". Definition of "{subpillar}": {subpillar_definition}'
)

CC_CRITERIA_FALSE = (
    'The work does not fit the definition of "{name}", or it is that type of activity '
    'but does not qualify for the Sub-Pillar "{subpillar}".'
)

_RECYCLING_TRUE = (
    " This includes industrial decarbonization through low-carbon or circular materials, "
    "recycling, and material substitution (bioplastics, recycled or bio-based fibers, "
    "low-carbon cement or chemicals, battery or e-waste recycling)."
)

ROUTING_CRITERIA = {
    ("Pillar", "Energy Transition"): {
        "true": 'The organization\'s own work fits the definition of "Energy Transition".'
        + _RECYCLING_TRUE,
        "false": "The work fits none of this, or its primary climate mechanism is capturing, "
        "transporting, or storing CO2.",
    },
    ("Sub-Pillar", "Energy Efficiency"): {
        "true": 'The organization\'s own work fits the definition of "Energy Efficiency".'
        + _RECYCLING_TRUE,
        "false": "The work fits none of this.",
    },
    ("Pillar", "Cross-Cutting"): {
        "true": "The organization's own work is climate field-building that enables others "
        "(data platforms, monitoring, finance, policy, legal, education, advocacy, "
        "incubators), and it spans two or more of Energy Transition, Nature "
        "Conservation, Regenerative Agriculture, and Geo-Engineering, or addresses "
        "climate change as a whole rather than one of them.",
        "false": "The organization builds, deploys, or supplies a specific solution, or its "
        "field-building is bounded to one of those four pillars.",
    },
}

GATE_QUESTION = (
    "Does this organization's own work (its products, services, programs, or stated "
    "mission) contribute to climate-action solutions in at least one of these areas: "
    "clean energy and industrial decarbonization; protecting or restoring nature; "
    "regenerative agriculture and food systems; carbon dioxide removal or other "
    "geo-engineering; or field-building work (tools, data, policy, finance, education, "
    "advocacy) that enables these?"
)

GATE_CRITERIA = {
    "true": "The organization's own work fits at least one of the listed areas.",
    "false": (
        "The work falls outside all of them, for example general social services, "
        "non-climate health, sports, consumer products with no climate angle, or "
        "generic economic development."
    ),
}

UNITS = ("organization", "activity")

ACTIVITY_WORDING = (
    (
        "this organization's own work (its products, services, programs, or stated mission)",
        "this activity",
    ),
    ("The organization's own work", "The activity"),
    ("the organization's work", "the activity"),
    ("The organization's work", "The activity"),
    ("The organization builds", "The activity builds"),
)
