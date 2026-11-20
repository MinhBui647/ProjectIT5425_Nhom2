"""All mock handoffs exercise unchanged P3 contracts, including deliberate faults."""
import json
import polars as pl
import pytest
from data_governance.transformers import transform_bronze
from data_governance.contracts import validate_table
from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore
from lakehouse_storage.ingestion_worker import raw_frame

SOURCES = ['market_prices', 'market_indices', 'weather', 'food_recalls', 'market_reports']


def test_normal_sources_validate_without_governance_in_mock(tmp_path):
    store = RunStore(tmp_path)
    run = store.create(RunRequest(sources=SOURCES, fao_variant='both'))
    for entry in run['batches']:
        env = json.loads(store.batch(run['run_id'], entry['sequence']))
        assert '_quality_status' not in env['data'][0]
        if entry['bronze_only']:
            assert env['data'][0]['published_date'] == '2026-01-15'
            continue
        frame = transform_bronze(raw_frame(env), entry['table_name'])
        quality = validate_table(frame, entry['table_name'])
        assert quality.invalid_row_count == 0 and not quality.hard_failures, quality
        if entry['table_name'] == 'weather':
            assert 'thi_index' in frame.columns
        if entry['table_name'] == 'market_prices':
            assert type(env['data'][0]['AveragePublishedPrice']) is str


@pytest.mark.parametrize('table', SOURCES[:-1])
@pytest.mark.parametrize('scenario', ['missing-field', 'invalid-time', 'invalid-value'])
def test_source_faults_do_not_silently_pass(tmp_path, table, scenario):
    run = RunStore(tmp_path).create(RunRequest(sources=[table], fault_table=table, scenario=scenario))
    env = json.loads(RunStore(tmp_path).batch(run['run_id'], 0))
    try:
        frame = transform_bronze(raw_frame(env), table)
        quality = validate_table(frame, table)
    except (ValueError, pl.exceptions.PolarsError):
        return
    assert quality.invalid_row_count > 0 or quality.hard_failures
