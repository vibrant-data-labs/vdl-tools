"""Tests for shared_tools.cb_funding_types and the cb_funding_calculations predicates.

Covers the vocabulary contract (every grouped slug is a known round type, the
display mapping round-trips) and the bug that motivated it: predicates must
give the same answer for raw slugs and for display names.
"""

import pandas as pd
import pytest

from vdl_tools.shared_tools import cb_funding_calculations as fcalc
from vdl_tools.shared_tools import cb_funding_types as ft


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

def test_every_grouped_slug_is_a_known_round_type():
    grouped = (ft.PRE_SEED_STAGES | ft.SEED_STAGES | ft.OTHER | ft.IPO_STATES
               | ft.VENTURE_ROUNDS | ft.POST_IPO | ft.UNDISCLOSED_STAGES
               | set(ft.DISCLOSED_STAGES_ORDERED) | set(ft.ROUND_TO_STAGE))
    assert grouped <= set(ft.ROUND_TYPES), grouped - set(ft.ROUND_TYPES)


def test_display_names_match_the_old_xlsx():
    """Spot checks against the former funding_types_mapping.xlsx values."""
    assert ft.to_display('series_a') == 'Series A'
    assert ft.to_display('series_unknown') == 'Venture (Unknown Stage)'
    assert ft.to_display('undisclosed') == 'Venture (Unknown Stage)'
    assert ft.to_display('debt_financing') == 'Debt'
    assert ft.to_display('non_equity_assistance') == 'Non-Equity'
    assert ft.to_display('grant_only') == 'Non-Equity'       # old quirk, preserved
    assert ft.to_display('early_stage_venture') == 'Early Venture'
    assert ft.to_display('Philanthropy') == 'Philanthropy'
    assert ft.to_display(['seed', 'grant', 'seed']) == ['Seed', 'Grant', 'Seed']  # order + duplicates kept


def test_as_raw_accepts_both_vocabularies_and_rejects_unknown():
    assert ft.as_raw('series_a') == 'series_a'
    assert ft.as_raw('Series A') == 'series_a'
    assert ft.as_raw('Venture (Unknown Stage)') == 'series_unknown'   # first slug listed wins
    assert ft.as_raw('IPO') == 'ipo'
    assert ft.as_raw(['Seed', 'grant']) == ['seed', 'grant']
    assert ft.as_raw(None) is None
    assert ft.as_raw('') == ''
    with pytest.raises(ValueError):
        ft.as_raw('Series Z')


def test_round_trip_display_to_raw_to_display():
    for display in ft.DISPLAY_NAMES:
        assert ft.to_display(ft.as_raw(display)) == display


def test_unknown_slugs():
    assert ft.unknown_slugs(['seed', 'series_a', None, '']) == set()
    assert ft.unknown_slugs(['seed', 'series_z']) == {'series_z'}


# ---------------------------------------------------------------------------
# Predicates: raw and display inputs must agree
# ---------------------------------------------------------------------------

def _row(types, stage=None, company_type='for_profit', org_type='For Profit'):
    return pd.Series({
        'Funding Types Raw': types,
        'Funding Types': ft.to_display(types),
        'funding_stage': stage,
        'Funding Stage': ft.to_display(stage),
        'company_type': company_type,
        'Org Type': org_type,
    })


def test_raised_from_venture_rounds_on_raw_and_display():
    row = _row(['grant', 'series_a'])
    assert fcalc.raised_from_venture_rounds(row) is True
    assert fcalc.raised_from_venture_rounds(
        row, funding_types_field='Funding Types', funding_stage_field='Funding Stage') is True

    grant_only = _row(['grant'])
    assert fcalc.raised_from_venture_rounds(grant_only) is False
    assert fcalc.raised_from_venture_rounds(
        grant_only, funding_types_field='Funding Types', funding_stage_field='Funding Stage') is False

    # stage-only signal, display vocabulary ('IPO' used to be compared against 'ipo')
    public = _row([], stage='ipo')
    assert fcalc.raised_from_venture_rounds(
        public, funding_types_field='Funding Types', funding_stage_field='Funding Stage') is True


def test_deduce_org_type_uses_raw_column():
    assert fcalc.deduce_org_type(_row(['seed'], company_type='non_profit')) == 'For Profit'
    assert fcalc.deduce_org_type(_row(['grant'], company_type='non_profit')) == 'non_profit'


def test_complete_stage_from_type_examples():
    assert fcalc.complete_stage_from_type(_row(['grant'])) == 'grant_only'
    assert fcalc.complete_stage_from_type(_row(['debt_financing'])) == 'debt_only'
    assert fcalc.complete_stage_from_type(_row(['seed', 'series_c'])) == 'late_stage_venture'
    assert fcalc.complete_stage_from_type(_row(['seed'], stage='early_stage_venture')) == 'early_stage_venture'
    assert fcalc.complete_stage_from_type(_row(['angel'], stage='seed')) == 'pre_seed'
    assert fcalc.complete_stage_from_type(_row(['grant'], company_type='non_profit')) == 'Philanthropy'
    # same answer from the display column
    assert fcalc.complete_stage_from_type(_row(['seed', 'series_c']), funding_types_field='Funding Types') \
        == 'late_stage_venture'


def test_grant_loan_flags_raw_equals_display():
    raw = pd.DataFrame({'Funding Types Raw': [
        ['grant', 'seed', 'series_a', 'debt_financing'],
        ['debt_financing', 'grant'],
        ['seed', 'post_ipo_equity', 'grant'],   # grant after IPO is ignored
        [],
    ]})
    display = pd.DataFrame({'Funding Types': raw['Funding Types Raw'].apply(ft.to_display)})
    out_raw = fcalc.grant_loan_flags(raw.copy())
    out_display = fcalc.grant_loan_flags(display.copy(), funding_types_field='Funding Types')
    cols = ['Before Seed Grant', 'Venture Grant', 'Before Seed Loan', 'Venture Loan']
    assert out_raw[cols].equals(out_display[cols])
    assert out_raw[cols].values.tolist() == [
        [True, False, False, True],
        [True, False, True, False],
        [False, False, False, False],
        [False, False, False, False],
    ]


def test_p_vs_venture():
    assert fcalc.p_vs_venture(_row(['grant'], org_type='Non Profit')) == 'Philanthropy'
    assert fcalc.p_vs_venture(_row(['series_a'], stage='early_stage_venture')) == 'Venture'
    assert fcalc.p_vs_venture(_row([], stage='ipo')) == 'Post-Venture'
