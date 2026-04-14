"""Classify whether a venture-funded company succeeded or failed at a given stage.

Conceptual model:
    We pick a stage to evaluate (e.g. "Early VC") and ask two questions:

    1. Did the company SUCCEED past that stage?
       → They raised a later round (Series B+), IPO'd, or had a successful M&A (at or after Series A).

    2. Did the company FAIL at that stage?
       → They didn't succeed, AND one of:
         - ensemble_operating_status_classification is "Shut Down"
         - They had an M&A exit before the success stage (early acqui-hire = failure)
         - Their last funding was 2+ years ago (zombie/stale)

    If neither, the company is still in progress — not yet classifiable.

Key inputs:
    company_funding_rows: DataFrame of funding rounds (round_type_nzi, financing_type_nzi, round_date_nzi)
    company_row: dict of company details (must include ensemble_operating_status_classification)
"""

import datetime as dt

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    DISCLOSED_STAGES_ORDERED,
    LATE_VC_CUTOFF,
    M_AND_A_NAMES,
    M_AND_A_SUCCESS_STAGE,
    TWO_YEARS_IN_DAYS,
    raised_equity_round,
)


# ---------------------------------------------------------------------------
# Stage mapping for failure evaluation
# ---------------------------------------------------------------------------
# When evaluating failure "at_stage", we need to know:
#   - which round types count as "having reached that stage" (at_stage_round_types)
#   - which round types prove they graduated past it (graduation_stages)
#
# Example: at_stage="Early VC"
#   at_stage_round_types = ["Series A", "Early VC", "Grant", "Pre-Seed", "Seed"]
#       → any of these mean the company was in the early stage
#   graduation_stages = ["Series B"]
#       → raising Series B means they succeeded past early stage

STAGE_FAILURE_MAP = {
    "Grant": {
        "at_stage_round_types": ["Accelerator/incubator", "Grant", "Pre-Seed", "Seed"],
        "graduation_stages": ["Series A", "Early VC"],
    },
    "Pre-Seed": {
        "at_stage_round_types": ["Accelerator/incubator", "Grant", "Pre-Seed", "Seed"],
        "graduation_stages": ["Series A", "Early VC"],
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
        "at_stage_round_types": ["Series B", "Late VC"],
        "graduation_stages": ["Series C"],
    },
}


def _get_graduation_and_later_stages(graduation_stages, late_venture_cutoff):
    """Get all stages at or after the graduation threshold.

    Returns the graduation stages plus everything after them in the ordered
    stage list. Also includes "Late VC" or "Early VC" as aliases when the
    graduation threshold falls in their range.

    Example: graduation_stages=["Series B"], late_venture_cutoff="Series B"
        → ["Series B", "Late VC", "Series C", ..., "Post IPO - Equity", "Late VC"]
    """
    earliest_graduation_idx = min(
        DISCLOSED_STAGES_ORDERED.index(stage) for stage in graduation_stages
    )
    stages_at_or_after = DISCLOSED_STAGES_ORDERED[earliest_graduation_idx:]

    late_venture_stages = DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(late_venture_cutoff):
    ]

    # "Late VC" and "Early VC" are catch-all labels not in DISCLOSED_STAGES_ORDERED's
    # natural position — include them when the graduation threshold overlaps their range
    if set(graduation_stages).intersection(late_venture_stages):
        return stages_at_or_after + ["Late VC"]
    if set(graduation_stages).intersection(["Series A", "Early VC"]):
        return stages_at_or_after + ["Early VC"]

    return stages_at_or_after


def raised_stage_or_earlier(company_funding_rows, stages=["Series A", "Early VC"]):
    """Check if the company raised any round at or before the given stages.

    Used as a gate: if a company never raised at or before the threshold,
    they can't be evaluated for success/failure at that threshold.
    (e.g., a company that went straight to IPO with no early rounds)
    """
    round_types = set(company_funding_rows['round_type_nzi'].values)

    # Direct match — company raised one of the target stages
    if set(stages).intersection(round_types):
        return True

    # Check for any earlier stage
    earliest_idx = min(
        DISCLOSED_STAGES_ORDERED.index(stage) for stage in stages
    )
    earlier_stages = set(DISCLOSED_STAGES_ORDERED[:earliest_idx])
    return bool(earlier_stages.intersection(round_types))


def _has_successful_manda(round_types, m_and_a_success_stage):
    """Check if the company had an M&A event at or after the success stage.

    M&A before Series A (default) is considered a failure (early acqui-hire).
    M&A at Series A or later is considered a success.
    """
    if not round_types.intersection(M_AND_A_NAMES):
        return None  # No M&A event at all

    stages_where_manda_is_success = set(DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
    ])

    if stages_where_manda_is_success.intersection(round_types):
        return True   # M&A at a mature stage → success
    return False      # M&A at an early stage → failure


def did_company_succeed(
    company_funding_rows,
    company_classifier_status,
    graduation_stages=("Series B",),
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    """Did this company succeed past the given graduation threshold?

    A company succeeds if it:
      1. Raised equity (or grant) funding
      2. Raised at or before the graduation stage (wasn't a straight-to-IPO)
      3. AND any of:
         a. IPO'd
         b. Had a successful M&A (at or after m_and_a_success_stage)
         c. Raised a round at or after the graduation stage
         d. Was acquired per ensemble_operating_status_classification
            (covers companies with no M&A round type in funding data)
    """
    round_types = set(company_funding_rows['round_type_nzi'].values)

    if not raised_equity_round(company_funding_rows):
        return False

    # Gate: must have raised at or before the graduation stage
    if not raised_stage_or_earlier(company_funding_rows, list(graduation_stages)):
        return False

    if "IPO" in round_types:
        return True

    # M&A check — if they had M&A, its success depends on what stage they reached
    manda_result = _has_successful_manda(round_types, m_and_a_success_stage)
    if manda_result is not None:
        return manda_result

    # Did they raise a round at or after the graduation stage?
    stages_at_or_after = _get_graduation_and_later_stages(
        list(graduation_stages), late_venture_cutoff,
    )
    if set(stages_at_or_after).intersection(round_types):
        return True

    # Some companies were acquired but have no M&A round type in their funding data.
    # If they passed the stage gate above, treat the acquisition as success.
    if company_classifier_status == "Acquired / Merger":
        return True

    return False


def time_since_last_funding(company_funding_rows):
    return (dt.datetime.now() - company_funding_rows['round_date_nzi'].max()).days


def did_company_fail(
    company_funding_rows,
    company_classifier_status,
    at_stage="Early VC",
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    """Did this company fail at the given stage?

    A company fails if it:
      1. Raised equity (or grant) funding
      2. Reached the evaluation stage (has rounds matching at_stage_round_types)
      3. Did NOT succeed past the graduation threshold
      4. AND any of:
         a. ensemble_operating_status_classification is "Shut Down"
         b. Had an early M&A (before the success stage — acqui-hire)
         c. Last funding was 2+ years ago (zombie/stale)

    Returns False if the company is still in progress (not yet classifiable).

    Args:
        at_stage: The stage to evaluate failure at. Must be a key in STAGE_FAILURE_MAP
                  or a stage in DISCLOSED_STAGES_ORDERED.
                  Common values: "Seed", "Early VC", "Series A", "Series B"
    """
    if not raised_equity_round(company_funding_rows):
        return False

    # Gate: must have raised at or before this stage
    if not raised_stage_or_earlier(company_funding_rows, [at_stage]):
        return False

    # Look up the stage mapping
    if at_stage in STAGE_FAILURE_MAP:
        mapping = STAGE_FAILURE_MAP[at_stage]
        at_stage_round_types = mapping["at_stage_round_types"]
        graduation_stages = mapping["graduation_stages"]
    else:
        at_stage_round_types = [at_stage]
        stage_idx = DISCLOSED_STAGES_ORDERED.index(at_stage)
        graduation_stages = DISCLOSED_STAGES_ORDERED[stage_idx + 1:]

    # Must have actually reached the evaluation stage
    round_types = set(company_funding_rows['round_type_nzi'].values)
    if not set(at_stage_round_types).intersection(round_types):
        return False

    # Can't be a failure if they succeeded
    if did_company_succeed(
        company_funding_rows,
        company_classifier_status,
        graduation_stages=graduation_stages,
        late_venture_cutoff=late_venture_cutoff,
        m_and_a_success_stage=m_and_a_success_stage,
    ):
        return False

    # --- Failure signals (company didn't succeed, check why) ---

    if company_classifier_status == "Shut Down":
        return True

    # Early M&A = failure (acquired before reaching a mature stage)
    manda_result = _has_successful_manda(round_types, m_and_a_success_stage)
    if manda_result is False:
        return True

    # Zombie: no funding for 2+ years
    if time_since_last_funding(company_funding_rows) >= outlier_time:
        return True

    # Not yet classifiable — still in progress
    return False
