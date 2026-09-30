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
