"""Classify whether a venture-funded company succeeded or failed at a given stage.

Conceptual model:
    We pick a stage to evaluate (e.g. "Early VC") and ask two questions:

    1. Did the company SUCCEED past that stage?
       → They raised a later round (Series B+), IPO'd, or had a successful M&A (at or after Series A).

    2. Did the company FAIL at that stage?
       → They didn't succeed, AND one of:
         - ensemble_operating_status_classification is "Shut Down" or "Restructured"
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
    LATE_STAGE_TYPES,
    EXIT_TYPES,
)


# ---------------------------------------------------------------------------
# Stage mapping for failure evaluation
# ---------------------------------------------------------------------------
# When evaluating failure "at_stage", we need to know:
#   - which round types count as "having reached that stage" (at_stage_round_types)
#   - which round types prove they graduated past it (graduation_stages)
#
# Example: at_stage="Series A"
#   at_stage_round_types = ["Series A", "Early VC"]
#       → any of these mean the company was in the early stage
#   graduation_stages = ["Series B"]
#       → raising Series B means they succeeded past early stage

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
        "at_stage_round_types": list(LATE_STAGE_TYPES) + ['Late VC'],
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


def _get_graduation_and_later_stages(graduation_stages, late_venture_cutoff):
    """Get all stage names at or after the graduation threshold.

    Given a list of graduation stages, returns those stages plus every stage
    that comes after them in ``DISCLOSED_STAGES_ORDERED``.  Also appends the
    catch-all labels ``"Late VC"`` or ``"Early VC"`` when the graduation
    threshold overlaps their range, since those labels are not part of the
    natural ordering but can appear in funding round data.

    Parameters
    ----------
    graduation_stages : list[str]
        Stage names that define the graduation threshold (e.g. ``["Series B"]``).
        Must all be present in ``DISCLOSED_STAGES_ORDERED``.
    late_venture_cutoff : str
        The stage at or after which rounds are considered "late venture".
        Used to decide whether the ``"Late VC"`` alias should be included.
        Comes from ``split_early_late_funding_rounds.LATE_VC_CUTOFF``.

    Returns
    -------
    list[str]
        All stage names at or after the earliest graduation stage, potentially
        with ``"Late VC"`` or ``"Early VC"`` appended.

    Examples
    --------
    >>> _get_graduation_and_later_stages(["Series B"], "Series B")
    ["Series B", "Series C", ..., "Post IPO - Equity", "Late VC"]
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
    they can't be evaluated for success/failure at that threshold
    (e.g. a company that went straight to IPO with no early rounds).

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding round rows for a single company. Must contain a
        ``"round_type_nzi"`` column with stage labels.
    stages : list[str], default ["Series A", "Early VC"]
        The stage(s) to use as the threshold.  The function checks whether
        the company has any round matching these stages or any stage earlier
        in ``DISCLOSED_STAGES_ORDERED``.

    Returns
    -------
    bool
        ``True`` if the company raised at least one round at or before the
        earliest of *stages*; ``False`` otherwise.
    """
    round_types = set(company_funding_rows['round_type_nzi'].values)

    # Direct match — company raised one of the target stages
    if set(stages).intersection(round_types):
        return True

    # Find the earliest position in DISCLOSED_STAGES_ORDERED among `stages`.
    # Ignore stages that aren't in the disclosed ordering (e.g. "Growth equity",
    # or synthetic STAGE_FAILURE_MAP keys like "Late_Exit") — those have no
    # position to compare against.
    ordered_indices = [
        DISCLOSED_STAGES_ORDERED.index(stage)
        for stage in stages
        if stage in DISCLOSED_STAGES_ORDERED
    ]
    if not ordered_indices:
        return False

    earliest_idx = min(ordered_indices)
    earlier_stages = set(DISCLOSED_STAGES_ORDERED[:earliest_idx])
    return bool(earlier_stages.intersection(round_types))


def _has_successful_manda(
    round_types,
    m_and_a_success_stage,
    company_classifier_status,
):
    """Check if the company had an M&A event and whether it counts as success.

    M&A signals come from two sources:
    - Round-level: Merger / Acquisition / Buyout in funding data.
    - Ensemble-level: company_classifier_status == "Acquired / Merger"
      catches acquisitions the LLM detected but that aren't recorded as M&A
      rounds in the funding data.

    Either source triggers the same stage-based determination:
    - At or after `m_and_a_success_stage` (default: Series A) → success.
    - Before that stage → failure (early acqui-hire).
    """
    has_manda_round    = bool(round_types.intersection(M_AND_A_NAMES))
    has_ensemble_manda = (company_classifier_status == "Acquired / Merger")

    if not has_manda_round and not has_ensemble_manda:
        return None  # no M&A signal at all

    stages_where_manda_is_success = set(DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
    ])
    # "Early VC" is a catch-all label for Series A-era rounds; treat it as
    # equivalent to Series A for M&A success purposes (parallels the existing
    # _get_graduation_and_later_stages logic).
    if m_and_a_success_stage in ("Series A", "Early VC"):
        stages_where_manda_is_success.update({"Early VC", "Series A"})

    if stages_where_manda_is_success.intersection(round_types):
        return True   # mature M&A → success
    return False      # early acqui-hire → failure


def did_company_succeed(
    company_funding_rows,
    company_classifier_status,
    graduation_stages=("Series B",),
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    """Determine whether a company succeeded past the given graduation stage.

    A company is classified as "succeeded" if **all** of the following hold:

    1. It raised equity (or grant) funding (via ``raised_equity_round``).
    2. It raised at or before the graduation stage — this excludes companies
       that skipped early stages entirely (e.g. straight-to-IPO).
    3. It meets **any one** of these success signals:
       a. IPO'd (has ``"IPO"`` in round types).
       b. Had a successful M&A at or after ``m_and_a_success_stage``.
       c. Raised a funding round at or after the graduation stage.
       d. Has ``ensemble_operating_status_classification == "Acquired / Merger"``
          (covers cases where the M&A round type is missing from funding data).

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        All funding round rows for a single company. Must contain columns
        ``"round_type_nzi"`` and ``"financing_type_nzi"``.
    company_classifier_status : str or None
        The company's ``ensemble_operating_status_classification`` value.
        Used as a fallback signal for acquisitions not captured in round data.
    graduation_stages : tuple[str, ...], default ("Series B",)
        The stage(s) the company must reach or surpass to be considered
        successful.  Looked up in ``STAGE_FAILURE_MAP`` by the caller.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold separating early from late venture rounds.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which an M&A event is considered a success
        rather than a failure (acqui-hire).

    Returns
    -------
    bool
        ``True`` if the company succeeded past the graduation threshold;
        ``False`` otherwise.
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
    manda_result = _has_successful_manda(
        round_types,
        m_and_a_success_stage,
        company_classifier_status,
    )
    if manda_result is not None:
        return manda_result

    # Did they raise a round at or after the graduation stage?
    stages_at_or_after = _get_graduation_and_later_stages(
        list(graduation_stages), late_venture_cutoff,
    )
    if set(stages_at_or_after).intersection(round_types):
        return True

    return False


def time_since_last_funding(company_funding_rows):
    """Compute the number of days since the company's most recent funding round.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding round rows for a single company. Must contain a
        ``"round_date_nzi"`` column with datetime values.

    Returns
    -------
    int
        Number of days between now and the most recent ``round_date_nzi``.
    """
    return (dt.datetime.now() - company_funding_rows['round_date_nzi'].max()).days


def did_company_fail(
    company_funding_rows,
    company_classifier_status,
    at_stage="Early VC",
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    """Determine whether a company failed at the given funding stage.

    A company is classified as "failed" if **all** of the following hold:

    1. It raised equity (or grant) funding (via ``raised_equity_round``).
    2. It raised at or before ``at_stage`` (via ``raised_stage_or_earlier``).
    3. It actually reached the evaluation stage (has rounds matching
       ``STAGE_FAILURE_MAP[at_stage]["at_stage_round_types"]``).
    4. It did **not** succeed past the graduation threshold (checked via
       ``did_company_succeed``).
    5. It exhibits **at least one** failure signal:
       a. ensemble_operating_status_classification is "Shut Down" or "Restructured".
       b. Had an early M&A before ``m_and_a_success_stage`` (acqui-hire).
       c. Last funding round was ``>= outlier_time`` days ago (zombie/stale).

    If conditions 1-4 hold but none of the failure signals (5a-c) are present,
    the company is considered **still in progress** and this returns ``False``.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        All funding round rows for a single company. Must contain columns
        ``"round_type_nzi"``, ``"financing_type_nzi"``, and ``"round_date_nzi"``.
    company_classifier_status : str or None
        The company's ensemble_operating_status_classification value.
    at_stage : str, default "Early VC"
        The stage to evaluate failure at. Must be a key in
        ``STAGE_FAILURE_MAP`` or a stage in ``DISCLOSED_STAGES_ORDERED``.
        Common values: ``"Seed"``, ``"Early VC"``, ``"Series A"``,
        ``"Series B"``.
    outlier_time : int, default TWO_YEARS_IN_DAYS (730)
        Number of days since last funding after which a non-succeeded company
        is considered a zombie/stale failure.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold separating early from late venture rounds.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which an M&A event is considered a success
        rather than a failure.

    Returns
    -------
    bool
        ``True`` if the company failed at the given stage; ``False`` if it
        succeeded, hasn't reached the stage, or is still in progress.
    """
    if not raised_equity_round(company_funding_rows):
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

    # Gate: must have raised at or before this stage.
    # Use at_stage_round_types (real stage names) rather than at_stage, since
    # at_stage may be a synthetic STAGE_FAILURE_MAP key (e.g. "Late_Exit")
    # that is not present in DISCLOSED_STAGES_ORDERED.
    if not raised_stage_or_earlier(company_funding_rows, at_stage_round_types):
        return False

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

    if company_classifier_status in {"Shut Down", "Restructured"}:
        return True

    # Early M&A = failure (acquired before reaching a mature stage)
    manda_result = _has_successful_manda(
        round_types,
        m_and_a_success_stage,
        company_classifier_status,
    )
    if manda_result is False:
        return True

    # Zombie: no funding for 2+ years
    if time_since_last_funding(company_funding_rows) >= outlier_time:
        return True

    # Not yet classifiable — still in progress
    return False
