import pandas as pd

PRE_SEED_STAGES = {
    'angel', 'pre_seed', 'convertible_note'  # , 'non_equity_assistance' #,'product_crowdfunding'
}  # mention one of these plus 'other'
OTHER = {'grant', 'debt_financing', 'non_equity_assistance'}
SEED_STAGES = {'equity_crowdfunding', 'initial_coin_offering', 'seed'}
IPO_STATES = {
    'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary'
}
POST_IPO_TYPES = ['ipo', 'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary']
EQUITY_MAPPING = {
    'equity_crowdfunding': 'pre_seed',
    'initial_coin_offering': 'pre_seed',
    'angel': 'pre_seed',
    # 'private_equity': 'late_venture',
    'series_c': 'late_venture',
    'series_d': 'late_venture',
    'series_e': 'late_venture',
    'series_f': 'late_venture',
    'series_g': 'late_venture',
    'series_h': 'late_venture',
    'series_i': 'late_venture',
    'series_j': 'late_venture'
}

DISCLOSED_STAGES_ORDERED = [
    'grant', 'equity_crowdfunding', 'initial_coin_offering', 'angel', 'pre_seed', 'seed',
    'series_a', 'series_b', 'series_c', 'series_d', 'series_e', 'series_f',
    'series_g', 'series_h', 'series_i', 'series_j', 'corporate_round', 'secondary_market',
    # 'private_equity',
    'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary',
]
EARLY_VENTURE_ROUNDS = {
    'series_a', 'series_b'
}
LATE_VENTURE_ROUNDS = set(DISCLOSED_STAGES_ORDERED[
                          DISCLOSED_STAGES_ORDERED.index('series_c'):DISCLOSED_STAGES_ORDERED.index('post_ipo_equity')])
VENTURE_ROUNDS = LATE_VENTURE_ROUNDS | EARLY_VENTURE_ROUNDS | {'seed'}
POST_IPO = set(DISCLOSED_STAGES_ORDERED[DISCLOSED_STAGES_ORDERED.index('post_ipo_equity'):])
UNDISCLOSED_STAGES = {'undisclosed', 'series_unknown'}

ROUND_TO_STAGE = {}
for stage in PRE_SEED_STAGES:
    ROUND_TO_STAGE[stage] = 'pre_seed'
for stage in SEED_STAGES:
    ROUND_TO_STAGE[stage] = 'seed'
for stage in EARLY_VENTURE_ROUNDS:
    ROUND_TO_STAGE[stage] = 'early_stage_venture'
for stage in LATE_VENTURE_ROUNDS:
    ROUND_TO_STAGE[stage] = 'late_stage_venture'
for stage in IPO_STATES:
    ROUND_TO_STAGE[stage] = 'ipo'
for stage in OTHER:
    ROUND_TO_STAGE[stage] = stage
ROUND_TO_STAGE['private_equity'] = 'private_equity'


def complete_stage_from_type(company_row):
    """ complete the stage of a company
    private equity replaced with late stage
    blanks will be replaced with the latest known stage
    if company got only seed stages then it is a pre_seed
    if pre-seed and unknown or undisclosed then early_stage_venture
    if late venture rounds present then late_stage_venture
    if any post-ipo then is ipo
    based on the type of funding """

    company_funding_types = set(company_row["Funding Types"])

    if company_row["funding_stage"] == 'private_equity':
        return 'late_stage_venture'
    elif company_row['company_type'] == 'non_profit':
        return "Philanthropy"
    elif company_row['funding_stage'] == 'seed' and company_funding_types.issubset(
            PRE_SEED_STAGES | OTHER) and not company_funding_types.issubset(OTHER):
        return 'pre_seed'
    # If there is already a `funding_stage` present, use it.
    elif not pd.isna(company_row['funding_stage']) and company_row['funding_stage'] != '':
        return company_row['funding_stage']

    elif pd.isna(company_row['funding_stage']) or company_row['funding_stage'] == '':
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
            # print('uh oh')
            return 'unknown'


def p_vs_venture(company_row):
    """ return if company is a pre-seed, venture,  postventure or philantropic based on funding types"""

    if company_row['Org Type'] in ['Nonprofit', 'Non Profit', 'non_profit']:
        return 'Philanthropy'
    if company_row['Funding Stage'] == 'Philanthropy':
        return 'Philanthropy'
    elif company_row['Funding Stage'] == 'Late Venture':
        return 'Venture'
    elif company_row['Funding Stage'] in ['Pre-Seed', 'Seed', 'Early Venture']:
        return 'Venture'
    elif company_row['Funding Stage'] in ['IPO', 'M&A']:
        return 'Post-Venture'
    elif company_row['Funding Stage'] == 'Non-Equity':
        return 'Non-Equity'
    elif company_row['Funding Stage'] == 'Venture (Unknown Stage)':
        return 'Venture'
    return 'Unknown'


def grant_loan_flags(df):
    # Initialize lists to store the boolean flags for each company
    grant_pre_seed = []
    loan_pre_seed = []
    grant_venture = []
    loan_venture = []

    # Define the funding types of interest
    funding_interest = {'grant', 'debt_financing'}

    for funding_list in df['Funding Types']:
        # Reset flags for each company
        grant_before_seed = False
        grant_between_seed_ipo = False
        loan_before_seed = False
        loan_between_seed_ipo = False
        # after_seriesB = False

        # Flags to mark if Seed and Series A have been found
        found_seed = False
        found_early_venture = False
        # found_seriesB = False
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


def raised_from_venture_rounds(
    company_row,
    funding_types_field='Funding Types',
    funding_stage_field='funding_stage',
):
    company_funding_types = set(company_row[funding_types_field])
    target_funding_types = VENTURE_ROUNDS.union(
        {'equity_crowdfunding', 'initial_coin_offering', 'angel', 'pre_seed', 'product_crowdfunding',
         'series_unknown', 'private_equity', 'convertible_note'}
    ) | POST_IPO
    if company_funding_types & target_funding_types:
        return True
    elif company_row[funding_stage_field] == 'ipo':
        return True
    return False


def deduce_org_type(company_row):
    if raised_from_venture_rounds(company_row):
        return "For Profit"
    return company_row['company_type']
