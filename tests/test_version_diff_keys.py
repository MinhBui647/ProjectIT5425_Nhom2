"""Version differences use Silver entity keys, including composite keys."""
import polars as pl
import pytest
from deltalake import write_deltalake
from lakehouse_storage.config import get_storage_options


@pytest.mark.parametrize(('columns', 'keys'), [
    ({'line_id': ['A', 'B'], 'timestamp': [1, 1]}, ['line_id', 'timestamp']),
    ({'farm_id': ['A', 'B'], 'observed_at': [1, 1]}, ['farm_id', 'observed_at']),
    ({'source': ['GDT', 'GDT'], 'product': ['WMP', 'SMP'], 'contract': ['EVENT_AVERAGE'] * 2,
      'observed_at': [1, 1], 'currency': ['USD'] * 2, 'unit': ['metric_ton'] * 2},
     ['source', 'product', 'contract', 'observed_at', 'currency', 'unit']),
    ({'source': ['FAO'] * 2, 'index_name': ['Dairy', 'Food'], 'period_start': [1, 1]},
     ['source', 'index_name', 'period_start']),
    ({'source': ['FDA', 'VFA'], 'recall_id': ['one', 'one']}, ['source', 'recall_id']),
])
def test_diff_uses_silver_composite_keys(tmp_path, columns, keys):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'diff')
    frame = pl.DataFrame(columns)
    write_deltalake(uri, frame.head(1).to_arrow(), storage_options=get_storage_options())
    write_deltalake(uri, frame.tail(1).to_arrow(), mode='append', storage_options=get_storage_options())
    result = diff_versions(uri, 0, 1)
    assert result['added'].equals(frame.tail(1))
    assert result['counts']['added_count'] == 1
    reverse = diff_versions(uri, 1, 0, id_column=keys)
    assert reverse['removed'].equals(frame.tail(1))
    assert reverse['id_column'] == keys


def test_diff_string_key_and_empty_snapshot(tmp_path):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'diff')
    frame = pl.DataFrame({'id': ['one'], 'value': [1]})
    write_deltalake(uri, frame.head(0).to_arrow(), storage_options=get_storage_options())
    write_deltalake(uri, frame.to_arrow(), mode='append', storage_options=get_storage_options())
    result = diff_versions(uri, 0, 1, 'id')
    assert result['id_column'] == 'id'
    assert result['counts']['added_count'] == 1
    assert diff_versions(uri, 0, 0)['counts']['added_count'] == 0


@pytest.mark.parametrize('keys', [['absent'], [], ['id', 'id']])
def test_diff_invalid_keys_raise(tmp_path, keys):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'diff')
    write_deltalake(uri, pl.DataFrame({'id': ['one']}).to_arrow(), storage_options=get_storage_options())
    with pytest.raises(ValueError, match='[Kk]ey|[Cc]olumn'):
        diff_versions(uri, 0, 0, keys)


@pytest.mark.parametrize('ids', [[None, 'one'], ['one', 'one']])
def test_diff_null_or_duplicate_keys_raise(tmp_path, ids):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'diff')
    write_deltalake(uri, pl.DataFrame({'id': ids}).to_arrow(), storage_options=get_storage_options())
    with pytest.raises(ValueError, match='[Kk]ey'):
        diff_versions(uri, 0, 0, 'id')


def test_diff_same_key_updates_are_not_additions(tmp_path):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'diff')
    for value in (1, 2):
        write_deltalake(uri, pl.DataFrame({'id': ['one'], 'value': [value]}).to_arrow(),
                        mode='overwrite', storage_options=get_storage_options())
    result = diff_versions(uri, 0, 1, 'id')
    assert result['counts']['added_count'] == result['counts']['removed_count'] == 0


def test_unknown_shape_keeps_temporal_fallback_with_null_batch_id(tmp_path):
    from lakehouse_storage.time_travel import diff_versions
    uri = str(tmp_path / 'unknown')
    frame = pl.DataFrame({'timestamp': [1, 2], 'batch_id': [None, None]},
                         schema_overrides={'batch_id': pl.String})
    write_deltalake(uri, frame.head(1).to_arrow(), storage_options=get_storage_options())
    write_deltalake(uri, frame.tail(1).to_arrow(), mode='append', storage_options=get_storage_options())
    result = diff_versions(uri, 0, 1)
    assert result['id_column'] == 'timestamp'
    assert result['counts']['added_count'] == 1
