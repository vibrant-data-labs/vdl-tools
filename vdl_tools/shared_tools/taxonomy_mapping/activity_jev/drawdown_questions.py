"""Exact question templates ported from drawdown; see SOURCE_REVISIONS.md."""

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

_STEWARDSHIP_FALSE = (
    " Conservation work does not qualify when the activity only educates, holds events, "
    "publishes, raises or grants money, or advocates, without itself protecting, "
    "managing, stewarding, or restoring land, habitat, or ecosystems."
)

QUESTION_VARIANTS: dict[str, dict[tuple[str, str], dict]] = {
    "base": {},
    "stewardship": {
        ("Sector", "Nature-Based Carbon Removal"): {
            "criteria": {
                "true": 'The activity fits the definition of "Nature-Based Carbon Removal".'
                + _STEWARDSHIP_TRUE,
                "false": 'The activity fits none of the definition of "Nature-Based Carbon '
                'Removal".' + _STEWARDSHIP_FALSE,
            }
        },
        ("Cluster", "Protect, Manage, & Restore Ecosystems"): {
            "criteria": {
                "true": 'The activity fits the definition of "Protect, Manage, & Restore '
                'Ecosystems".' + _STEWARDSHIP_TRUE,
                "false": 'The activity fits none of the definition of "Protect, Manage, & '
                'Restore Ecosystems".' + _STEWARDSHIP_FALSE,
            }
        },
    },
    # Same "yes" sentence; the "no" side only restates the definition, so enabling work
    # (policy, financing, education) stays governed by the definitions, which include it.
    "stewardship_yes": {
        ("Sector", "Nature-Based Carbon Removal"): {
            "criteria": {
                "true": 'The activity fits the definition of "Nature-Based Carbon Removal".'
                + _STEWARDSHIP_TRUE,
                "false": 'The activity fits none of the definition of "Nature-Based Carbon '
                'Removal".',
            }
        },
        ("Cluster", "Protect, Manage, & Restore Ecosystems"): {
            "criteria": {
                "true": 'The activity fits the definition of "Protect, Manage, & Restore '
                'Ecosystems".' + _STEWARDSHIP_TRUE,
                "false": 'The activity fits none of the definition of "Protect, Manage, & '
                'Restore Ecosystems".',
            }
        },
    },
}
