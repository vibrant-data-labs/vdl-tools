"""Drawdown question wording, ported from the drawdown prototype's ``stewardship_yes`` variant.

See docs/activity_jev_contract.md for the source revision.
"""

NODE_QUESTION = (
    'Does this activity fall under the Drawdown {level} "{name}"{parent}? '
    'Definition of "{name}": {definition}'
)

_STEWARDSHIP_TRUE = (
    " This includes conservation, land-trust, park, trail, public-lands, watershed, "
    "wildlife and habitat organizations when the activity itself protects, manages, "
    "stewards, or restores land, habitat, waterways, or ecosystems, even when carbon or "
    "climate is not mentioned."
)

# Two nature nodes get explicit criteria so land and habitat stewardship counts as
# qualifying work. The "no" side only restates the definition, so enabling work (policy,
# financing, education) stays governed by the definitions, which include it.
STEWARDSHIP_CRITERIA = {
    ("Sector", "Nature-Based Carbon Removal"): {
        "true": 'The activity fits the definition of "Nature-Based Carbon Removal".'
        + _STEWARDSHIP_TRUE,
        "false": 'The activity fits none of the definition of "Nature-Based Carbon Removal".',
    },
    ("Cluster", "Protect, Manage, & Restore Ecosystems"): {
        "true": 'The activity fits the definition of "Protect, Manage, & Restore Ecosystems".'
        + _STEWARDSHIP_TRUE,
        "false": 'The activity fits none of the definition of "Protect, Manage, & Restore '
        'Ecosystems".',
    },
}
