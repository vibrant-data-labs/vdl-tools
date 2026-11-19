"""One Earth question wording, ported from the oneearth prototype's ``cc_routing`` variant.

Templates address one activity (the prototype's organization wording with its own
``ACTIVITY_WORDING`` substitutions already applied). See docs/activity_jev_contract.md for
the source revision.
"""

NODE_QUESTION = (
    'Does this activity fall under the {level} "{name}"{parent}? '
    'Definition of "{name}": {definition}'
)

# Solutions under a Cross-Cutting Sub-Pillar must also qualify for that Sub-Pillar.
CC_PREFIX = "Cross-Cutting"
CC_CONDITION = (
    ' Answer yes only if the activity also qualifies for the Sub-Pillar "{subpillar}". '
    'Definition of "{subpillar}": {subpillar_definition}'
)

_RECYCLING_TRUE = (
    " This includes industrial decarbonization through low-carbon or circular materials, "
    "recycling, and material substitution (bioplastics, recycled or bio-based fibers, "
    "low-carbon cement or chemicals, battery or e-waste recycling)."
)

# Explicit true/false criteria for the three nodes the prototype routes specially.
ROUTING_CRITERIA = {
    ("Pillar", "Energy Transition"): {
        "true": 'The activity fits the definition of "Energy Transition".' + _RECYCLING_TRUE,
        "false": "The work fits none of this, or its primary climate mechanism is capturing, "
        "transporting, or storing CO2.",
    },
    ("Sub-Pillar", "Energy Efficiency"): {
        "true": 'The activity fits the definition of "Energy Efficiency".' + _RECYCLING_TRUE,
        "false": "The work fits none of this.",
    },
    ("Pillar", "Cross-Cutting"): {
        "true": "The activity is climate field-building that enables others (data platforms, "
        "monitoring, finance, policy, legal, education, advocacy, incubators), and it "
        "spans two or more of Energy Transition, Nature Conservation, Regenerative "
        "Agriculture, and Geo-Engineering, or addresses climate change as a whole rather "
        "than one of them.",
        "false": "The activity builds, deploys, or supplies a specific solution, or its "
        "field-building is bounded to one of those four pillars.",
    },
}
