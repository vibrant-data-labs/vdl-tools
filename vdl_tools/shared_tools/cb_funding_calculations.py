"""
Org-level funding classifications derived from an org's Crunchbase rounds.

The round-type vocabulary (slugs, display names, named groups) lives in
``cb_funding_types``; the group constants are re-exported here so older imports
such as ``from cb_funding_calculations import ROUND_TO_STAGE`` keep working.

Every predicate below works on raw slugs but passes its inputs through
``cb_funding_types.as_raw`` first, so it gives the same answer whether it is
handed the raw column (``Funding Types Raw``) or the display column
(``Funding Types``). Before this, ``raised_from_venture_rounds`` silently
returned False for every org inside the enrichment pipeline because it
compared raw slugs against display names.
"""
import pandas as pd

from vdl_tools.shared_tools.cb_funding_types import (  # noqa: F401  (re-exports)
    DISCLOSED_STAGES_ORDERED,
    EARLY_VENTURE_ROUNDS,
    FUNDING_STAGE_COL,
    FUNDING_STAGE_RAW_COL,
    FUNDING_TYPES_COL,
    FUNDING_TYPES_RAW_COL,
    IPO_STATES,
    LATE_VENTURE_ROUNDS,
    OTHER,
    POST_IPO,
    POST_IPO_TYPES,
    PRE_SEED_STAGES,
    ROUND_TO_STAGE,
    SEED_STAGES,
    UNDISCLOSED_STAGES,
    VENTURE_BACKED_ROUNDS,
    VENTURE_ROUNDS,
    as_raw,
)

# Kept for compatibility; no consumers in the VDL repos as of 2026-09.
EQUITY_MAPPING = {
    'equity_crowdfunding': 'pre_seed',
    'initial_coin_offering': 'pre_seed',
    'angel': 'pre_seed',
    'series_c': 'late_venture',
    'series_d': 'late_venture',
    'series_e': 'late_venture',
    'series_f': 'late_venture',
    'series_g': 'late_venture',
    'series_h': 'late_venture',
    'series_i': 'late_venture',
    'series_j': 'late_venture'
}


def _raw_types(company_row, funding_types_field):
    """The org's round types as a set of raw slugs (empty set when missing)."""
    value = company_row[funding_types_field]
    if isinstance(value, str):
        value = [value] if value else []
    if value is None or not isinstance(value, (list, tuple)):
        return set()
    return set(as_raw(list(value)))


def _raw_stage(company_row, funding_stage_field):
    """The org's stage as a raw label ('' when missing)."""
    value = company_row.get(funding_stage_field) if hasattr(company_row, 'get') else company_row[funding_stage_field]
    if value is None or value == '' or value != value:
        return ''
    return as_raw(value)


def complete_stage_from_type(company_row, funding_types_field=FUNDING_TYPES_RAW_COL):
    """Overall stage label for a company from its round types and Crunchbase's
    own ``funding_stage`` field (raw label; map with cb_funding_types.to_display).

    Rules, in order: private equity counts as late stage; non-profits are
    'Philanthropy'; a 'seed'-stage company with only pre-seed-type rounds is
    'pre_seed'; otherwise Crunchbase's stage is used when present; when it is
    blank the stage is inferred from the round types (post-IPO -> 'ipo', late
    venture, early venture, 'debt_only', 'grant_only', pre-seed, seed,
    'non_equity_assistance', 'unknown_venture_stage', else 'unknown').
    """
    company_funding_types = _raw_types(company_row, funding_types_field)
    funding_stage = _raw_stage(company_row, 'funding_stage')

    if funding_stage == 'private_equity':
        return 'late_stage_venture'
    elif company_row['company_type'] == 'non_profit':
        return "Philanthropy"
    elif funding_stage == 'seed' and company_funding_types.issubset(
            PRE_SEED_STAGES | OTHER) and not company_funding_types.issubset(OTHER):
        return 'pre_seed'
    # If there is already a `funding_stage` present, use it.
    elif funding_stage != '':
        return funding_stage

    else:
        if company_funding_types & POST_IPO:
            return 'ipo'
        elif company_funding_types & LATE_VENTURE_ROUNDS:
            return 'late_stage_venture'
        # all company_funding_types are one of pre_seed_stages/early venture and unknown or undisclosed
        elif company_funding_types & EARLY_VENTURE_ROUNDS:
            return 'early_stage_venture'
        elif company_funding_types == {'debt_financing'}:
            return 'debt_only'
        elif company_funding_types == {'grant'}:
            return 'grant_only'
        # all company_funding_types are one of pre_seed_stages
        elif company_funding_types.issubset(
                PRE_SEED_STAGES | OTHER | UNDISCLOSED_STAGES) and not company_funding_types.issubset(
            OTHER | UNDISCLOSED_STAGES):
            return 'pre_seed'
        elif company_funding_types & SEED_STAGES:
            return "seed"
        elif company_funding_types == {'non_equity_assistance'}:
            return 'non_equity_assistance'
        elif company_funding_types & (UNDISCLOSED_STAGES | {'product_crowdfunding'}):
            return 'unknown_venture_stage'
        else:
            return 'unknown'


def p_vs_venture(company_row, funding_stage_field=FUNDING_STAGE_COL):
    """'Philanthropy' / 'Venture' / 'Post-Venture' / 'Non-Equity' / 'Unknown'
    from the org type and the DISPLAY funding stage."""
    if company_row['Org Type'] in ['Nonprofit', 'Non Profit', 'non_profit']:
        return 'Philanthropy'
    stage = company_row[funding_stage_field]
    if stage == 'Philanthropy':
        return 'Philanthropy'
    elif stage == 'Late Venture':
        return 'Venture'
    elif stage in ['Pre-Seed', 'Seed', 'Early Venture']:
        return 'Venture'
    elif stage in ['IPO', 'M&A']:
        return 'Post-Venture'
    elif stage == 'Non-Equity':
        return 'Non-Equity'
    elif stage == 'Venture (Unknown Stage)':
        return 'Venture'
    return 'Unknown'


def grant_loan_flags(df, funding_types_field=FUNDING_TYPES_RAW_COL):
    """Add four booleans per org from the chronological round list: whether a
    grant / a loan (debt_financing) came before any seed round ('Before Seed
    Grant' / 'Before Seed Loan') or between seed and IPO ('Venture Grant' /
    'Venture Loan'). Rounds after the first post-IPO round are ignored.
    ``funding_types_field`` must be in chronological order."""
    grant_pre_seed = []
    loan_pre_seed = []
    grant_venture = []
    loan_venture = []

    for funding_list in df[funding_types_field]:
        if not isinstance(funding_list, (list, tuple)):
            funding_list = []
        funding_list = as_raw(list(funding_list))
        # Reset flags for each company
        grant_before_seed = False
        grant_between_seed_ipo = False
        loan_before_seed = False
        loan_between_seed_ipo = False

        # Flags to mark if Seed and Series A have been found
        found_seed = False
        found_early_venture = False
        found_otherFR = False

        for round_type in funding_list:
            if round_type in POST_IPO_TYPES:
                break
            elif round_type == 'seed':
                found_seed = True
            elif round_type == 'series_a':
                found_early_venture = True
            elif round_type == 'series_b':
                found_early_venture = True
            elif round_type in LATE_VENTURE_ROUNDS:
                found_otherFR = True
            elif round_type == 'private_equity':
                found_otherFR = True

            if round_type == 'grant':
                if not found_seed and not (found_early_venture or found_otherFR):
                    grant_before_seed = True
                elif found_early_venture:
                    grant_between_seed_ipo = True
                elif found_otherFR:
                    grant_between_seed_ipo = True
            if round_type == 'debt_financing':
                if not found_seed and not (found_early_venture or found_otherFR):
                    loan_before_seed = True
                elif found_early_venture:
                    loan_between_seed_ipo = True
                elif found_otherFR:
                    loan_between_seed_ipo = True
        # Append flags to lists
        grant_pre_seed.append(grant_before_seed)
        grant_venture.append(grant_between_seed_ipo)
        loan_pre_seed.append(loan_before_seed)
        loan_venture.append(loan_between_seed_ipo)

    # Add boolean columns to the DataFrame
    df['Before Seed Grant'] = grant_pre_seed
    df['Venture Grant'] = grant_venture
    df['Before Seed Loan'] = loan_pre_seed
    df['Venture Loan'] = loan_venture

    return df


# Round types that imply an org raises money like a for-profit: any venture,
# pre-seed, crowdfunding, private-equity or post-IPO round. Broader than
# VENTURE_BACKED_ROUNDS on purpose - this is used to infer org TYPE, not to
# decide whether an org is venture-backed.
FOR_PROFIT_ROUND_TYPES = VENTURE_ROUNDS | PRE_SEED_STAGES | {
    'equity_crowdfunding', 'initial_coin_offering', 'private_equity',
} | POST_IPO


def raised_from_venture_rounds(
    company_row,
    funding_types_field=FUNDING_TYPES_RAW_COL,
    funding_stage_field='funding_stage',
):
    """True if the org has any for-profit-style round (see
    FOR_PROFIT_ROUND_TYPES) or its stage is 'ipo'. Accepts raw or display
    values in either field."""
    company_funding_types = _raw_types(company_row, funding_types_field)
    if company_funding_types & FOR_PROFIT_ROUND_TYPES:
        return True
    elif _raw_stage(company_row, funding_stage_field) == 'ipo':
        return True
    return False


def deduce_org_type(company_row):
    """Crunchbase's company_type, except an org that raised for-profit-style
    rounds is always 'For Profit'."""
    if raised_from_venture_rounds(company_row):
        return "For Profit"
    return company_row['company_type']
