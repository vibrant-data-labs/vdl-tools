import datetime as dt

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    DISCLOSED_STAGES_ORDERED,
    EARLY_VC_STAGES,
    LATE_VC_CUTOFF,
    M_AND_A_NAMES,
    M_AND_A_SUCCESS_STAGE,
    SPLIT_AFTER_LAST_EARLY_ROUND,
    SPLIT_ON_FIRST_LATE_ROUND,
    TWO_YEARS_IN_DAYS,
    divide_funding_rows,
    raised_equity_round,
)


def _get_subsequent_stages(success_threshold_stages, late_venture_cutoff):

    min_success_threshold_stage = min([
        DISCLOSED_STAGES_ORDERED.index(stage) for stage in success_threshold_stages
    ])

    subsequent_stages = DISCLOSED_STAGES_ORDERED[
        min_success_threshold_stage:
    ]

    late_venture_stages = DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(late_venture_cutoff):
    ]

    if set(success_threshold_stages).intersection(late_venture_stages):
        return subsequent_stages + ["Late VC"]

    if set(success_threshold_stages).intersection(["Series A", "Early VC"]):
        return subsequent_stages + ["Early VC"]


def raised_stage_or_earlier(company_funding_rows, stages=["Series A", "Early VC"]):
    all_funding_types = set(company_funding_rows['round_type_nzi'].values)
    if set(stages).intersection(all_funding_types):
        return True

    min_success_threshold_stage = min([
        DISCLOSED_STAGES_ORDERED.index(stage) for stage in stages
    ])

    earlier_stages = DISCLOSED_STAGES_ORDERED[:min_success_threshold_stage]
    if set(earlier_stages).intersection(all_funding_types):
        return True
    return False



def did_company_succeed(
    company_funding_rows,
    # If they made it to this stage, then they succeeded
    success_threshold_stage=["Series B"],
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,  # The first stage where M&A is considered successful
):
    all_funding_types = set(company_funding_rows['round_type_nzi'].values)
    if not raised_equity_round(company_funding_rows):
        return False

    # Sometimes someone goes straight to IPO -- we don't want to count these as successes
    if not raised_stage_or_earlier(company_funding_rows, success_threshold_stage):
        return False

    # If it IPO'd, then it succeeded (IPO isn't in the company funding types so need to check separately)
    if "IPO" in all_funding_types:
        return True

    subsequent_stages = _get_subsequent_stages(success_threshold_stage, late_venture_cutoff)

    # Did they have an M&A Event
    if all_funding_types.intersection(M_AND_A_NAMES):
        # Find all the stages where M&A is considered a success
        stage_names_where_m_and_a_success = set(DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
        ])

        # Check if any of the company's funding types are in the list of stages where M&A is considered a success
        # M&A is considered a success usually at series_a or longer (but can be changed with m_and_a_success_stage)
        if len(stage_names_where_m_and_a_success.intersection(all_funding_types)) > 0:
            return True
        else:
            return False

    return len(set(subsequent_stages).intersection(all_funding_types)) > 0


def time_since_last_funding(company_funding_rows):
    return (dt.datetime.now() - company_funding_rows['round_date_nzi'].max()).days


def did_company_fail(
    company_funding_rows,
    outlier_time=TWO_YEARS_IN_DAYS,
    success_threshold_stage_minus_one="Early VC",
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    # Have they raised a venture round before?
    if not raised_equity_round(company_funding_rows):
        return False

    # Sometimes someone goes straight to IPO -- we don't want to count these as failures
    if not raised_stage_or_earlier(company_funding_rows, [success_threshold_stage_minus_one]):
        return False

    if success_threshold_stage_minus_one in [ "Grant", "Pre-Seed", "Seed"]:
        focal_stage_names = ["Grant", "Pre-Seed", "Seed"]
        success_threshold_stage = ["Series A", "Early VC"]

    elif success_threshold_stage_minus_one == "Series A":
        focal_stage_names = ["Series A", "Early VC"]
        success_threshold_stage = ["Series B"]

    #     success_threshold_stage = "Series A"
    elif success_threshold_stage_minus_one == "Early VC":
        focal_stage_names = ["Series A", "Early VC", "Grant", "Pre-Seed", "Seed" ]
        success_threshold_stage = ["Series B"]

    elif success_threshold_stage_minus_one == "Series B":
        focal_stage_names = ["Series B", "Late VC"]
        success_threshold_stage = ["Series C"]
    else:
        focal_stage_names = [success_threshold_stage_minus_one]
        success_threshold_stage = DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(success_threshold_stage_minus_one) + 1:
        ]

    # Have they even made it to stage we are looking for?
    # If company never had this stage, then it can't fail past it
    all_funding_types = set(company_funding_rows['round_type_nzi'].values)
    if len(set(focal_stage_names).intersection(all_funding_types)) == 0:
        return False

    # Can't be a success
    if did_company_succeed(
        company_funding_rows=company_funding_rows,
        success_threshold_stage=success_threshold_stage,
        late_venture_cutoff=late_venture_cutoff,
        m_and_a_success_stage=m_and_a_success_stage,
    ):
        return False

    all_funding_types = set(company_funding_rows['round_type_nzi'].values.tolist())

    if all_funding_types.intersection(M_AND_A_NAMES):

        stage_names_where_m_and_a_success = set(DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
        ])

        # Check that the company doesn't have any funding stages of those that are considered a success
        # for M&A
        if len(stage_names_where_m_and_a_success.intersection(all_funding_types)) == 0:
            return True
        else:
            # Should never actually get here because would exited in the success check
            print('uh oh')
            print(company_funding_rows['client_id_nzi'].values[0])
            return False

    if time_since_last_funding(company_funding_rows) >= outlier_time:
        return True

    # Doesn't mean it succeeded! Just means it didn't fail
    return False
