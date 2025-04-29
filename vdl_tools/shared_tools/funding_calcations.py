import datetime as dt

import numpy as np
from plotly import express as px

import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used functions


SEED_STAGES = [
    'grant', 'equity_crowdfunding', 'initial_coin_offering','angel','pre_seed','seed', 'debt_financing', 'convertible_note', 'non_equity_assistance'
]


IPO_STATES = ['ipo', 'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary'] #    , 'm_and_a'] in in Funding Stage
OTHER_FR = [ 'series_c', 'series_d', 'series_e', 'series_f', 'series_g', 'series_h',
          'series_i', 'series_j', 'post_ipo_equity',  'post_ipo_debt', 'post_ipo_secondary','series_unknown', 'corporate_round']



POST_IPO_TYPES = ['ipo', 'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary']


EQUITY_ORDER = ['equity_crowdfunding','initial_coin_offering','angel','pre_seed','seed', 'series_a', 'series_b',
        'series_c', 'series_d', 'series_e', 'series_f', 'series_g', 'series_h',
        'series_i', 'series_j','undisclosed','series_unknown','private_equity' ,'corporate_round',
        'post_ipo_equity']
# make barcharts for each

# Mapping for combining categories
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


DISCLOSED_STAGES_ORDERED = [
    'grant', 'equity_crowdfunding','initial_coin_offering','angel','pre_seed','seed',
    'series_a','series_b', 'series_c', 'series_d', 'series_e', 'series_f',
    'series_g', 'series_h', 'series_i', 'series_j',
    # 'private_equity',
    'ipo', 'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary',
]

VENTURE_ROUNDS = DISCLOSED_STAGES_ORDERED[:DISCLOSED_STAGES_ORDERED.index('ipo')]

LATE_VENTURE_START = 'series_b'
M_AND_A_SUCCESS_STAGE = 'series_a'
LATE_STAGE_VENTURE_ROUNDS = VENTURE_ROUNDS[VENTURE_ROUNDS.index(LATE_VENTURE_START):]


def is_venture_company(company_funding_types):
    return any(stage in company_funding_types for stage in VENTURE_ROUNDS)


def did_company_succeed(
    company_row,
    subsequent_stages,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,  # The first stage where M&A is considered successful
):
    if not is_venture_company(company_row['Funding Types']):
        return False
    
    # If it IPO'd, then it succeeded (IPO isn't in the company funding types so need to check separately)
    if company_row['Funding Status'] == 'ipo':
        return True

    company_funding_types = set(company_row["Funding Types"])
    subsequent_stages = set(subsequent_stages)

    if company_row["Funding Status"] == 'm_and_a':
        # Find all the stages where M&A is considered a success
        stage_names_where_m_and_a_success = set(DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
        ])

        # Check if any of the company's funding types are in the list of stages where M&A is considered a success
        # M&A is considered a success usually at series_a or longer (but can be changed with m_and_a_success_stage)
        if len(stage_names_where_m_and_a_success.intersection(company_funding_types)) > 0:
            return True
        else:
            return False

    # Check if the company has any of the subsequent stages
    return len(subsequent_stages.intersection(company_funding_types)) > 0



def did_company_fail(
    company_row,
    focal_stage_names,
    subsequent_stages,
    outlier_time,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
):
    # Have they raised a venture round before?
    if not is_venture_company(company_row['Funding Types']):
        return False

    # Have they even made it to stage we are looking for?
    # If company never had this stage, then it can't fail past it
    if len(set(focal_stage_names).intersection(set(company_row['Funding Types']))) == 0:
        return False

    # Can't be a success
    if did_company_succeed(
        company_row,
        subsequent_stages=subsequent_stages,
        m_and_a_success_stage=m_and_a_success_stage,
    ):
        return False

    # Are they closed 
    if company_row['operating_status'] == 'closed':
        return True

    if company_row['Funding Status'] == 'm_and_a':

        stage_names_where_m_and_a_success = set(DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(m_and_a_success_stage):
        ])

        # Check that the company doesn't have any funding stages of those that are considered a success
        # for M&A
        if len(stage_names_where_m_and_a_success.intersection(set(company_row['Funding Types']))) == 0:
            return True
        else:
            # Should never actually get here because would exited in the success check
            print('uh oh')
            return False
    
    if time_since_last_funding(company_row['Funding Types Dates']) >= outlier_time:
        return True

    # Doesn't mean it succeeded! Just means it didn't fail
    return False



def calculate_failure_rate(
    subset,
    outlier_time,
    stage_name="series_a",
    late_venture_start=LATE_VENTURE_START,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    debug=False,
):
    """Calculates the rate of failure for companies that have not received funding past a certain stage."""

    if stage_name not in DISCLOSED_STAGES_ORDERED + ["late_venture"]:
        raise Exception("Invalid stage name")

    if stage_name == "late_venture":
        stage_order_idx = DISCLOSED_STAGES_ORDERED.index(late_venture_start)

        # Late Venture should be considered anything between late_venture_start and IPO
        focal_stage_names = DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(late_venture_start):
            DISCLOSED_STAGES_ORDERED.index("ipo")
        ]
        # Had to IPO to be successful
        subsequent_stages = DISCLOSED_STAGES_ORDERED[DISCLOSED_STAGES_ORDERED.index("ipo"):]
    elif stage_name == "seed":
        # Seed is seed and anything before it
        stage_order_idx = DISCLOSED_STAGES_ORDERED.index("seed")
        focal_stage_names = DISCLOSED_STAGES_ORDERED[:DISCLOSED_STAGES_ORDERED.index("seed")+1]
        subsequent_stages = DISCLOSED_STAGES_ORDERED[stage_order_idx + 1:]
    else:
        stage_order_idx = DISCLOSED_STAGES_ORDERED.index(stage_name)
        focal_stage_names = [stage_name]
        subsequent_stages = DISCLOSED_STAGES_ORDERED[stage_order_idx + 1:]

    df_success = subset[
        subset.apply(
            lambda x: did_company_succeed(
                company_row=x,
                subsequent_stages=subsequent_stages,
                m_and_a_success_stage=m_and_a_success_stage,
            ),
        axis=1)
    ].copy()

    df_failed = subset[
        subset.apply(
            lambda x: did_company_fail(
                company_row=x,
                focal_stage_names=focal_stage_names,
                subsequent_stages=subsequent_stages,
                outlier_time=outlier_time,
                m_and_a_success_stage=m_and_a_success_stage,
            ),
        axis=1)
    ].copy()

    failure_rate = df_failed.shape[0] / (df_failed.shape[0] + df_success.shape[0])

    if debug:
        return failure_rate, df_failed, df_success
    
    return failure_rate


def calculate_expected_survivals(
        base_df,
        alive_df,
        outlier_time,
        late_venture_start=LATE_VENTURE_START,
        m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    ):
    """Calculates the expected survival rates for companies at different stages of funding.

    Parameters
    ----------
    base_df : pd.DataFrame
        The base DataFrame to use for calculating survival and failure rates
    alive_df : pd.DataFrame
        The DataFrame to use for living companies
    outlier_time : int
        Number of days since last funding to be considered as an outlier
    """
    seed_failure_rate = calculate_failure_rate(
        base_df,
        outlier_time,
        "seed",
        late_venture_start=late_venture_start,
        m_and_a_success_stage=m_and_a_success_stage,
    )
    series_a_failure_rate = calculate_failure_rate(
        base_df,
        outlier_time,
        "series_a",
        late_venture_start=late_venture_start,
        m_and_a_success_stage=m_and_a_success_stage,
    )
    # series_b_failure_rate = calculate_failure_rate(
    #     base_df,
    #     outlier_time,
    #     "series_b",
    #     late_venture_start=late_venture_start,
    #     m_and_a_success_stage=m_and_a_success_stage,
    # )
    late_venture_failure_rate = calculate_failure_rate(
        base_df,
        outlier_time,
        "late_venture",
        late_venture_start=late_venture_start,
        m_and_a_success_stage=m_and_a_success_stage,
    )

    seed_survival_rate = 1 - seed_failure_rate
    series_a_survival_rate = 1 - series_a_failure_rate
    # series_b_survival_rate = 1 - series_b_failure_rate
    late_venture_survival_rate = 1 - late_venture_failure_rate

    n_seed_preseed = (alive_df['Funding Types'].apply(lambda x: all(round_name in SEED_STAGES for round_name in x))).sum()
    n_series_a = (alive_df['Equity Stage'] == 'series_a').sum()
    # n_series_b = (alive_df['Equity Stage'] == 'series_b').sum()

    expected_survived_seed = seed_survival_rate * n_seed_preseed
    expected_survived_a = series_a_survival_rate * (n_series_a + expected_survived_seed)
    # expected_survived_b = series_b_survival_rate * (n_series_b + expected_survived_a)
    expected_survived_late_venture = late_venture_survival_rate * (expected_survived_a)

    # n_total = n_seed_preseed + n_series_a + n_series_b
    n_total = n_seed_preseed + n_series_a
    overall_expected_survival_rate = expected_survived_late_venture / n_total

    return {
        "n_seed_preseed": int(n_seed_preseed),
        "n_series_a": int(n_series_a),
        # "n_series_b": int(n_series_b),
        "n_total": int(n_total),
        'expected_survived_seed': expected_survived_seed,
        'expected_survived_a': expected_survived_a,
        # 'expected_survived_b': expected_survived_b,
        'expected_survived_late_venture': expected_survived_late_venture,
        "overall_expected_survival_rate": overall_expected_survival_rate,
        'seed_failure_rate': seed_failure_rate,
        'series_a_failure_rate': series_a_failure_rate,
        # 'series_b_failure_rate': series_b_failure_rate,
        'late_venture_failure_rate': late_venture_failure_rate,
    }


def calculate_fr_stats(subset_df):
    # Extracting individual metrics from the stats dictionaries
    all_means = [stats['mean'] for stats in subset_df['date_diff_stats'] if stats is not None]
    all_medians = [stats['median'] for stats in subset_df['date_diff_stats'] if stats is not None]
    all_mins = [stats['min'] for stats in subset_df['date_diff_stats'] if stats is not None]
    all_maxes = [stats['max'] for stats in subset_df['date_diff_stats'] if stats is not None]
    all_std_devs = [stats['std_dev'] for stats in subset_df['date_diff_stats'] if stats is not None]

    # Calculate Interquartile Range (IQR)
    Q1 = np.percentile(all_means, 25)
    Q3 = np.percentile(all_means, 75)
    iqr_diff = Q3 - Q1

    # Calculating overall summary statistics
    overall_summary = {
    'mean_of_means': np.mean(all_means),
    'median_of_medians': np.median(all_medians),
    'min_of_mins': np.min(all_mins),
    'max_of_maxes': np.max(all_maxes),
    'mean_of_std_devs': np.mean(all_std_devs),
    #'Q1': Q1,
    'Q3': Q3,
    'IQR': iqr_diff
    }
    return overall_summary


def calculate_date_diff_stats(dates_list, funding_rounds, ipo_states=IPO_STATES):

    i = trim_funding_rounds(funding_rounds, ipo_states)
    if i < 3:  # Check if the list has fewer than 2 dates
        return None  # Not enough data to calculate differences

    # Convert string dates to datetime objects
    dates = [dt.datetime.strptime(date, '%Y-%m-%d') for date in dates_list[:i]]

    # Calculate differences between consecutive dates in days
    diffs = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]

    # Calculate statistics
    mean_diff = np.mean(diffs)
    median_diff = np.median(diffs)
    min_diff = min(diffs)
    max_diff = max(diffs)
    std_dev_diff = np.std(diffs, ddof=1)  # Use ddof=1 for sample standard deviation


    return {
        'mean': mean_diff,
        'median': median_diff,
        'min': min_diff,
        'max': max_diff,
        'std_dev': std_dev_diff,
    }


# trim secuence up to any of the ipo states
def trim_funding_rounds(funding_rounds, ipo_states):
    # Find the first index where a funding round matches an IPO state
    for i, round_type in enumerate(funding_rounds):
        if round_type in ipo_states:
            #print(funding_rounds[:i])
            # Return everything up to (but not including) the matched round
            return i
    # If no match is found, return the full list
    #print(funding_rounds[: len(funding_rounds)], len(funding_rounds))
    return len(funding_rounds)


# todo review the grant and loan flags function
def grant_loan_flags(df):
    # Initialize lists to store the boolean flags for each company
    before_seed_flags = []
    between_seed_seriesA_flags = []
    between_seriesA_seriesB_flags = []
    after_seriesB_flags = []

    # Define the funding types of interest
    funding_interest = {'grant', 'debt_financing'}

    for funding_list in df['Funding Types']:
        # Reset flags for each company
        before_seed = False
        between_seed_seriesA = False
        between_seriesA_seriesB = False
        after_seriesB = False

        # Flags to mark if Seed and Series A have been found
        found_seed = False
        found_seriesA = False
        found_seriesB = False
        found_otherFR = False

        for round_type in funding_list:
            if round_type in POST_IPO_TYPES:
                break
            elif round_type == 'seed':
                found_seed = True
            elif round_type == 'series_a':
                found_seriesA = True
            elif round_type == 'series_b':
                found_seriesB = True
            elif round_type in LATE_STAGE_VENTURE_ROUNDS:
                found_otherFR = True

            if round_type in funding_interest:
                if not found_seed and not (found_seriesA or found_seriesB or found_otherFR):
                    before_seed = True
                elif found_seed and not (found_seriesA or found_seriesB or found_otherFR):
                    between_seed_seriesA = True
                elif found_seriesA and not (found_seriesB or found_otherFR):
                    between_seriesA_seriesB = True
                elif found_otherFR and found_seriesB:
                    after_seriesB = True

        # Append flags to lists
        before_seed_flags.append(before_seed)
        between_seed_seriesA_flags.append(between_seed_seriesA)
        between_seriesA_seriesB_flags.append(between_seriesA_seriesB)
        after_seriesB_flags.append(after_seriesB)

    # Add boolean columns to the DataFrame
    df['Before Seed'] = before_seed_flags
    df['Between Seed and Series A'] = between_seed_seriesA_flags
    df['Between Series A and Series B'] = between_seriesA_seriesB_flags
    df['After Series B'] = after_seriesB_flags

    return df

# time since last funding
def time_since_last_funding(dates_list):
    #grab last date and calculate time since
    last_funding_at = dt.datetime.strptime(dates_list[-1], '%Y-%m-%d')
    time_since = (dt.datetime.now() - last_funding_at).days
    return time_since


def get_df_location(df, location='North America'):
    cf.string2list(df, ['location_identifiers'])
    df_location = df[df['location_identifiers'].apply(lambda x: location in x if isinstance(x, list) else False)].copy()
    return df_location


def calculate_outlier_time(df):
    df_stats = df[(df.operating_status=='active') & (df.date_diff_stats.notna())].copy()
    summary_non_closed = calculate_fr_stats(df_stats)

    print(summary_non_closed, 'stats operating is active')

    outlier_time = summary_non_closed['Q3'] + 1.5*summary_non_closed['IQR']
    return outlier_time


def permute_label(df, col):
    shuffled = np.random.permutation(df[col].tolist()).tolist()
    return shuffled # shuffled list of same length can be read as df column


def calculate_random_surival_round(df, comparison_column, outlier_time):
    # shuffle the column
    df = df.copy()
    df[comparison_column] = permute_label(df, comparison_column)

    # calculate the survival rates
    comparison_column_is_true = df[comparison_column]
    true_comparison_df = df[comparison_column_is_true]
    false_comparison_df = df[~comparison_column_is_true]
    true_column_survival_rate = calculate_expected_survivals(true_comparison_df, df, outlier_time)
    false_column_survival_rate = calculate_expected_survivals(false_comparison_df, df, outlier_time)
    return true_column_survival_rate, false_column_survival_rate


def run_compare_survival_rates_rounds(
    df,
    comparison_column,
    outlier_time,
    n_rounds=1000,
):
    comparison_column_is_true = df[comparison_column]
    true_comparison_df = df[comparison_column_is_true]

    observed_survival_rate_column = calculate_expected_survivals(true_comparison_df, df, outlier_time)
    observed_survival_rate_rest = calculate_expected_survivals(df[~comparison_column_is_true], df, outlier_time)

    random_survival_rates = []
    for i in range(n_rounds):
        true_column_survival_rate, false_column_survival_rate = calculate_random_surival_round(df, comparison_column, outlier_time)
        random_survival_rates.append((true_column_survival_rate, false_column_survival_rate))
        if i % 10 == 0:
            print(f"Round {i} complete")
    return observed_survival_rate_column, observed_survival_rate_rest, random_survival_rates


def compare_survival_rates(
    df,
    comparison_column,
    outlier_time,
    n_rounds=1000,
    plot=False,
    title=None,
    annotation_title=None,
    absolute_difference=True,
):
    observed_survival_rate_column, observed_survival_rate_rest, random_survival_rates = run_compare_survival_rates_rounds(df, comparison_column, outlier_time, n_rounds)
    if absolute_difference:
        observed_difference = observed_survival_rate_column['overall_expected_survival_rate'] - observed_survival_rate_rest['overall_expected_survival_rate']
        random_differences = [true_rate['overall_expected_survival_rate'] - false_rate['overall_expected_survival_rate'] for true_rate, false_rate in random_survival_rates]
    else:
        observed_difference = observed_survival_rate_column['overall_expected_survival_rate'] / observed_survival_rate_rest['overall_expected_survival_rate']
        random_differences = [true_rate['overall_expected_survival_rate'] / false_rate['overall_expected_survival_rate'] for true_rate, false_rate in random_survival_rates]

    title = title or f"Survival Rates vs Random {comparison_column}"
    if plot:
        fig = create_plot_get_metrics(
            differences=random_differences,
            observed_difference=observed_difference,
            title=title,
            annotation_title=annotation_title,
        )
        fig.show()

    return observed_difference, random_differences


def get_metrics(series, quantiles=(10, 90)):
    return {
        "mean": np.mean(series),
        "std_dev": np.std(series),
        "median": np.median(series),
        "iqr_quantiles": (
            np.percentile(series, quantiles[0]),
            np.percentile(series, quantiles[1]),
        ),
        "differences": series,
    }


def create_plot(
    differences,
    observed_difference,
    mean,
    std_dev,
    median,
    iqr_quantiles,
    quantiles=(10, 90),
    title="Random Differences in Survival Rates",
    annotation_title=None,
    add_std_dev_bars=False,
    add_iqr_bars=True,
):

    fig = px.histogram(
        differences,
        title=title,
        template="simple_white",
        color_discrete_sequence=["limegreen"],
        opacity=0.7,
    )
    annotation_title = annotation_title or f"Observed Difference: {observed_difference:.2f}"
    fig.add_vline(
        x=observed_difference,
        line_width=3,
        line_dash="solid", line_color="violet",
        annotation_text=annotation_title,
        annotation_position="top right",
    )


    if add_std_dev_bars:
        fig.add_vline(x=mean, line_width=3, line_dash="dash", line_color="red")
        fig.add_vline(
            x=mean + 2 * std_dev,
            line_width=3,
            line_dash="dash",
            line_color="red",
        )
        fig.add_vline(
            x=mean - 2 * std_dev,
            line_width=3,
            line_dash="dash",
            line_color="red",
        )

    if add_iqr_bars:
        fig.add_vline(
            x=median,
            line_width=3,
            line_dash="dash",
            line_color="blue",
            annotation_text=f"Median - {median:.2f}",
            annotation_position="top left",
            annotation_textangle=-90,
        )

        fig.add_vline(
            x=iqr_quantiles[1],
            line_width=3,
            line_dash="dash",
            line_color="blue",
            annotation_text=f"{quantiles[1]}th percentile - {iqr_quantiles[1]:.2f}",
            annotation_position="top left",
            annotation_textangle=-90,
        )
        fig.add_vline(
            x=iqr_quantiles[0],
            line_width=3,
            line_dash="dash",
            line_color="blue",
            annotation_text=f"{quantiles[0]}th percentile - {iqr_quantiles[0]:.2f}",
            annotation_position="top left",
            annotation_textangle=-90,
        )

    fig.update_layout(
        xaxis_title=title,
        yaxis_title="Frequency",
        showlegend=False,
    )
    return fig


def create_plot_get_metrics(
    differences,
    observed_difference,
    title="Random Differences in Survival Rates",
    annotation_title=None,
    quantiles=(10, 90),
    add_std_dev_bars=False,
    add_iqr_bars=True,
):
    metrics = get_metrics(
        differences,
        quantiles=quantiles,
    )

    differences = metrics["differences"]
    mean = metrics["mean"]
    std = metrics["std_dev"]
    median = metrics["median"]
    iqr_quantiles = metrics["iqr_quantiles"]
    return create_plot(
        differences,
        observed_difference,
        mean,
        std,
        median,
        iqr_quantiles,
        quantiles,
        title,
        annotation_title,
        add_std_dev_bars=add_std_dev_bars,
        add_iqr_bars=add_iqr_bars,
    )



def get_active_companies(df, outlier_time):
    return (
        df[(df['operating_status'] == 'active') &
            (
                    (df['Funding Types'].apply(lambda x: any(
                        ipo_type in x for ipo_type in POST_IPO_TYPES))) |  # include companies that have had an ipo funding round
                    (df['Funding Status'] == 'ipo') | # sometimes there's this but no ipo funding round
                    (
                            (df['Funding Types Dates'].apply(lambda x: time_since_last_funding(
                                x) < outlier_time)) &  # include companies that have had funding within the outlier time but not m and a
                            ((df['Funding Types'].apply(
                                lambda x: all(ipo_type not in x for ipo_type in POST_IPO_TYPES)) | (
                                        df['Funding Status'] != 'ipo')) & (
                                        df['Funding Status'] != 'm_and_a'))
                    )  # include companies that have had a grant or loan
                # include companies that have had a series B and then an M&A
            )
            ].copy()
    )


def get_late_commercial_companies(df, outlier_time):
    df_alive = get_active_companies(df, outlier_time)
    return df_alive[
        (df_alive['operating_status'] == 'active') &
        (
            (df_alive['Funding Types'].apply(lambda x: any(item in POST_IPO_TYPES for item in x))) |
            (df_alive['Funding Status'] == 'ipo')  |
            (
                (df_alive['Funding Types'].apply(lambda x: any(item in x for item in ['series_a','series_b'] + OTHER_FR[:8]))) &
                (df_alive['Funding Status'] == 'm_and_a')
            )
        )
    ].copy()


def get_late_venture_companie(df, outlier_time):
    df_alive = get_active_companies(df, outlier_time)
    return df_alive[
        (df_alive['operating_status'] == 'active') &
        (
            # include companies that have had funding within the outlier time but not m and a
            (df_alive['Funding Types Dates'].apply(lambda x: time_since_last_funding(x) < outlier_time)) &
            (
                df_alive['Funding Types'].apply(lambda x: any(item in LATE_STAGE_VENTURE_ROUNDS for item in x))
            )
        ) &
        (df_alive['Funding Status'] != 'ipo') &
        (df_alive['Funding Status'] != 'm_and_a')
    ].copy()
