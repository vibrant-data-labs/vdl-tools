from dataclasses import asdict
from io import BytesIO

import pandas as pd
from givingtuesday_datamart.client import BasicFieldsRow, NonprofitHit, GrantSummary, FunderIdentity
from vdl_tools.scrape_enrich.givingtuesday.query_prepare_givingtuesday import _assemble_cb_shape


def test_structured_granters_and_names_survive_assembly_and_parquet():
    hit = NonprofitHit('012345678', 'Legal', 'Second', 'City', 'CA', 1, 'Mission', dba_name='Trading')
    basic = [BasicFieldsRow('012345678', 'Legal', 'Second', year, None, None, 'City', 'CA', '12345',
                            'example.org', 1000, 500, 400, dba_name='Trading') for year in [2023, 2024]]
    granters = [FunderIdentity('111111111', 'Shared', None, None), FunderIdentity('222222222', 'Shared', 'Line 2', 'DBA')]
    summaries = [GrantSummary('012345678', year, 250, 3, granters) for year in [2023, 2024]]
    result = _assemble_cb_shape([hit], pd.DataFrame([asdict(r) for r in basic]),
                                pd.DataFrame([asdict(r) for r in summaries]), 'total_cash_contributions')
    row = result.iloc[0]
    assert [row[k] for k in ['businessname1', 'businessname2', 'dba_name']] == ['Legal', 'Second', 'Trading']
    assert row['granters'] == [asdict(g) for g in granters]
    assert row['total_grants_amount'] == 500
    assert 'Funders' not in result and 'Funder_Names' not in result
    buffer = BytesIO()
    result.to_parquet(buffer)
    buffer.seek(0)
    loaded = pd.read_parquet(buffer).iloc[0]['granters'].tolist()
    assert loaded == row['granters']
    empty = _assemble_cb_shape([hit], pd.DataFrame([asdict(r) for r in basic]), pd.DataFrame(), 'total_cash_contributions')
    assert empty.iloc[0]['granters'] == []


def test_summary_callback_reuses_query_and_excludes_removed_recipients():
    from unittest.mock import Mock
    from vdl_tools.scrape_enrich.givingtuesday.query_prepare_givingtuesday import query_process_givingtuesday_data

    client = Mock()
    eins = ['012345678', '111111111', '333333333']
    client.search_nonprofits.return_value = [
        NonprofitHit(ein, 'Legal', None, 'City', 'CA', 1, 'Mission') for ein in eins]
    # The third search hit has no basic fields and will not be returned.
    client.get_basic_fields.return_value = [
        BasicFieldsRow(ein, 'Legal', None, 2024, None, None, 'City', 'CA', '12345',
                       'example.org', 1000, 500, 400) for ein in eins[:2]]
    donor = FunderIdentity('111111111', 'Donor', 'Second', None)
    summaries = [GrantSummary(ein, year, 250, 3, [donor])
                 for ein in eins for year in [2023, 2024]]
    client.get_grant_summaries.return_value = summaries
    callback = Mock()
    result = query_process_givingtuesday_data(
        search_terms_list=['education'], client=client, on_grant_summaries=callback)
    client.get_grant_summaries.assert_called_once()
    callback.assert_called_once_with(summaries[:2])
    assert result['ein'].tolist() == ['01-2345678']
    assert result.iloc[0]['total_grants_amount'] == 500


def test_empty_search_calls_summary_callback_without_query():
    from unittest.mock import Mock
    from vdl_tools.scrape_enrich.givingtuesday.query_prepare_givingtuesday import query_process_givingtuesday_data

    client = Mock()
    client.search_nonprofits.return_value = []
    callback = Mock()
    result = query_process_givingtuesday_data(
        search_terms_list=['education'], client=client, on_grant_summaries=callback)
    callback.assert_called_once_with([])
    client.get_grant_summaries.assert_not_called()
    assert result.empty
