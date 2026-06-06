"""Single source of truth for NZI stage / round / exit / timing constants.

Every constant here defines part of the policy that the rest of the
``process_nzi`` package (and downstream analyses) uses to slice funding-round
data into stage buckets, classify round types as exits, and decide how long
without funding counts as "zombie / stale". If you need to change any of
these, change them here — everywhere else imports from this file.

Sections:
    1. Ordered stage sequence       (DISCLOSED_STAGES_ORDERED)
    2. Stage groupings / buckets    (UP_TO_A_TYPES, A_TO_B_TYPES, etc.)
    3. Exit and M&A round types     (EXIT_TYPES, M_AND_A_NAMES)
    4. Stage boundaries             (LATE_VC_CUTOFF, M_AND_A_SUCCESS_STAGE)
    5. Bucket ordering              (STAGE_ORDER)
    6. Split-strategy enums         (SPLIT_ON_FIRST_LATE_ROUND, ...)
    7. Timing thresholds            (TWO_YEARS_IN_DAYS)
    8. Survival-pipeline stages     (NZI_SURVIVAL_STAGES)
    9. Per-stage success/failure    (STAGE_FAILURE_MAP)
"""

# ---------------------------------------------------------------------------
# 1. Ordered stage sequence
# ---------------------------------------------------------------------------
# Canonical order of NZI ``round_type_nzi`` values from earliest to latest.
# Used to compare two stages ("is Series C later than Series B?") and to slice
# "everything at or after the graduation stage" in the failure logic.
# Note: "Early VC", "Late VC", and "Growth equity" are catch-all labels that
# are NOT in this ordering — they are handled as aliases at call sites
# (see _get_graduation_and_later_stages and _has_successful_manda).
DISCLOSED_STAGES_ORDERED = [
    "Accelerator/incubator",
    "Grant",
    "Pre-Seed",
    "Seed",
    "Early VC",
    "Series A",
    "Series B",
    "Series C",
    "Late VC",
    "Series D",
    "Series E",
    "Series F",
    "Series G",
    "Series H",
    "Series I",
    "Series J",
    "IPO",
    "SPAC",
    "Post IPO",
    "Post IPO - Equity",
]


# ---------------------------------------------------------------------------
# 2. Stage groupings / buckets
# ---------------------------------------------------------------------------
# Pre-Series-B funding is split into two finer buckets:
#   up_to_a = rounds before the first Series A or Early VC round
#   a_to_b  = rounds from first Series A / Early VC up to first Series B
# Series B is its own bucket (b_to_late). Anything from Series C / Late VC /
# Growth equity onward is late stage (late_to_exit). IPO / SPAC / M&A is exit.
UP_TO_A_TYPES = {
    "Pre-Seed",
    "Seed",
}

A_TO_B_TYPES = {
    "Series A",
    "Early VC",
}

MIDDLE_STAGE_TYPES = {
    "Series B",
    # "Late VC",  # moved to late stage - usually follows C or is synonymous with C
}

LATE_STAGE_TYPES = {
    "Late VC",
    "Series C",
    "Series D",
    "Series E",
    "Series F",
    "Series G",
    "Series H",
    "Series I",
    "Series J",
    "Growth equity",
}

# Cohort membership set for "seed-stage" classification. Broader than UP_TO_A_TYPES because it also counts
# pre-equity activity (accelerator, grant) as seed-level. Used both for ``STAGE_FAILURE_MAP["Seed"]`` (and
# ``Seed_Exit``) and for the survival-pipeline current-stage classifier.
SEED_COHORT_TYPES = {
    "Accelerator/incubator",
    "Grant",
    "Pre-Seed",
    "Seed",
}


# ---------------------------------------------------------------------------
# 3. Exit and M&A round types
# ---------------------------------------------------------------------------
# M&A names are also part of EXIT_TYPES — they're broken out as a list so
# downstream code can distinguish M&A specifically (e.g. early acqui-hire
# detection in _has_successful_manda).
M_AND_A_NAMES = [
    "Merger",
    "Acquisition",
    "Buyout",
]

EXIT_TYPES = {
    "IPO",
    "SPAC",
    "Post IPO",
    "Post IPO - Equity",
    "Merger",
    "Acquisition",
    "Buyout",
}


# ---------------------------------------------------------------------------
# 4. Stage boundaries
# ---------------------------------------------------------------------------
# Graduation-set inclusion floor used by ``_get_graduation_and_later_stages``
# in ``nzi_zombie_companies_fail.py``: if a cohort's ``graduation_stages``
# overlap the slice ``DISCLOSED_STAGES_ORDERED[idx(LATE_VC_CUTOFF):]``, the
# catch-all venture labels ``"Late VC"`` and ``"Growth equity"`` are appended
# to the graduation set. Set to ``"Series B"`` so that a Series A cohort
# (graduation_stages = ["Series B"]) sweeps in those catch-alls — a company
# with ``[Series A → Late VC]`` or ``[Series A → Growth equity]`` then
# correctly counts as graduated past Series A.
#
# NOTE: do NOT confuse this with ``MIDDLE_STAGE_TYPES = {"Series B"}`` (which
# answers a different question: what BUCKET does a Series B round land in?
# Answer: the middle / b_to_late bucket, not a late one). The two constants
# encode different decisions:
#
#   - ``MIDDLE_STAGE_TYPES`` is taxonomic: where does Series B sit in the
#     funding-round bucket ordering? Middle.
#   - ``LATE_VC_CUTOFF`` is a graduation-set trigger threshold: at which
#     graduation level should we start treating Late VC / Growth equity as
#     plausible graduation outcomes? Series B graduation → yes.
#
# Both are correct for what they're asking. Renaming has been discussed but
# left for a wider naming pass to avoid ripple through three modules; the
# docstring here is the authoritative description of the constant's role.
LATE_VC_CUTOFF = "Series B"

# Earliest stage at which an M&A event counts as a success rather than an
# early acqui-hire failure. (Default for `did_company_succeed` / `did_company_fail`.)
M_AND_A_SUCCESS_STAGE = "Series A"


# ---------------------------------------------------------------------------
# 5. Bucket ordering
# ---------------------------------------------------------------------------
# Ordinal positions of the five stage buckets, used to compare buckets
# (e.g. "is `b_to_late` at or above `a_to_b`?").
STAGE_ORDER = {"up_to_a": 0, "a_to_b": 1, "b_to_late": 2, "late_to_exit": 3, "exit": 4}


# ---------------------------------------------------------------------------
# 6. Split-strategy enums
# ---------------------------------------------------------------------------
# Two strategies for assigning rounds to the a_to_b bucket:
#   first_late_round       : a_to_b ends at the first Series B round.
#   after_last_early_round : a_to_b ends after the LAST Series A / Early VC
#                            round (so interleaved Bs stay in the a_to_b bucket).
SPLIT_ON_FIRST_LATE_ROUND = "first_late_round"
SPLIT_AFTER_LAST_EARLY_ROUND = "after_last_early_round"


# ---------------------------------------------------------------------------
# 7. Timing thresholds
# ---------------------------------------------------------------------------
# A company with no funding for this many days is flagged as a "zombie" (stale) failure by `did_company_fail`.
# Also used downstream in elemental-catalytic-capital as the OUTLIER_TIME cutoff.
TWO_YEARS_IN_DAYS = 365 * 2


# ---------------------------------------------------------------------------
# 8. Survival-pipeline stages
# ---------------------------------------------------------------------------
# Stages evaluated in the survival-rate pipeline (Seed → Series A → Series B
# funnel). Each stage uses STAGE_FAILURE_MAP[<stage>]["at_stage_round_types"]
# as the cohort gate — a company is only in the stage's cohort if it actually
# had a round of one of those types. Using "Series A" (rather than "Early VC")
# means only companies with a Series A OR Early VC round count as A-stage
# members; seed-only companies are excluded.
NZI_SURVIVAL_STAGES = ["Seed", "Series A", "Series B"]


# ---------------------------------------------------------------------------
# 9. Per-stage success / failure map
# ---------------------------------------------------------------------------
# When evaluating failure "at_stage", we need to know:
#   - which round types count as "having reached that stage"
#     (at_stage_round_types)
#   - which round types prove they graduated past it
#     (graduation_stages)
#
# Example: at_stage="Series A"
#   at_stage_round_types = ["Series A", "Early VC"]
#       → any of these mean the company was in the early stage
#   graduation_stages = ["Series B"]
#       → raising Series B means they succeeded past early stage
#
# Synthetic keys ("Late_Exit", "Seed_Exit") are NOT in DISCLOSED_STAGES_ORDERED
# and are used purely as STAGE_FAILURE_MAP keys for downstream reporting.
STAGE_FAILURE_MAP = {
    "Pre-Seed": {
        "at_stage_round_types": ["Accelerator/incubator", "Grant", "Pre-Seed"],
        "graduation_stages": ["Seed", "Series A", "Early VC"],
    },
    "Seed": {
        "at_stage_round_types": ["Accelerator/incubator", "Grant", "Pre-Seed", "Seed"],
        "graduation_stages": ["Series A", "Early VC"],
    },
    "Series A": {
        "at_stage_round_types": ["Series A", "Early VC"],
        "graduation_stages": ["Series B"],
    },
    "Early VC": {
        "at_stage_round_types": ["Series A", "Early VC", "Accelerator/incubator", "Grant", "Pre-Seed", "Seed"],
        "graduation_stages": ["Series B"],
    },
    "Series B": {
        "at_stage_round_types": ["Series B"],
        "graduation_stages": ["Late VC", "Series C"],
    },
    "Late_Exit": {
        "at_stage_round_types": list(LATE_STAGE_TYPES),
        # M&A names are handled separately by `_has_successful_manda` and are
        # not in DISCLOSED_STAGES_ORDERED, so exclude them here.
        "graduation_stages": sorted(EXIT_TYPES - set(M_AND_A_NAMES)),
    },
    "Seed_Exit": {
        "at_stage_round_types": ["Accelerator/incubator", "Grant", "Pre-Seed", "Seed"],
        # M&A names are handled separately by `_has_successful_manda` and are
        # not in DISCLOSED_STAGES_ORDERED, so exclude them here.
        "graduation_stages": sorted(EXIT_TYPES - set(M_AND_A_NAMES)),
    },
}
