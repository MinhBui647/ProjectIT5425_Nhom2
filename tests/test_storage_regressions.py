"""Regression cases from the November storage review, using isolated tables."""
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse_storage.config import get_storage_options
from lakehouse_storage.schemas import get_silver_schema
from lakehouse_storage.silver_manager import (
    _cast_to_schema, _to_silver_frame, append_to_delta, create_delta_table,
    ingest_bronze_file, read_silver_table,
)


def sensor():
    return pl.DataFrame({
        'timestamp': ['2026-01-15T10:00:00+00:00'],
        'line_id': ['LINE_UHT_1'], 'uht_temp': [140.0],
        '_source_system': ['TEST'], '_generated_at': ['2026-01-15T10:00:00Z'],
    })


@pytest.mark.parametrize('entry', ['append', 'ingest'])
@pytest.mark.parametrize('invalid', ['missing', 'null', 'timestamp'])
@pytest.mark.parametrize('existing', ['new', 'legacy_nullable'])
def test_strict_rejection_preserves_snapshot(tmp_path, entry, invalid, existing):
    from lakehouse_storage.bronze_writer import write_bronze_batch
    uri = str(tmp_path / 'silver')
    initial = sensor()
    if entry == 'ingest':
        initial = initial.with_columns(pl.lit('initial.parquet').alias('_bronze_file'),
                                       pl.lit('initial_hash').alias('_bronze_sha256'))
    if existing == 'new':
        append_to_delta(initial, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
    else:
        table = _cast_to_schema(_to_silver_frame(initial, 'sensor_telemetry').to_arrow(),
                                get_silver_schema('sensor_telemetry'), strict=True)
        nullable = pa.table({name: table[name] for name in table.column_names})
        write_deltalake(uri, nullable, partition_by=['line_id', 'ingestion_date'],
                        storage_options=get_storage_options())
    before = read_silver_table(uri)
    bad = sensor()
    if invalid == 'missing':
        bad = bad.drop('line_id')
    elif invalid == 'null':
        bad = bad.with_columns(pl.lit(None, dtype=pl.String).alias('line_id'))
    else:
        bad = pl.concat([bad, bad.with_columns(pl.lit('invalid').alias('timestamp'))])
    with pytest.raises((ValueError, pl.exceptions.PolarsError)):
        if entry == 'append':
            append_to_delta(bad, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
        else:
            path = write_bronze_batch(bad, 'sensor_telemetry', tmp_path / 'bronze', validate_schema=False)
            ingest_bronze_file(path, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
    assert DeltaTable(uri, storage_options=get_storage_options()).version() == 0
    assert before.equals(read_silver_table(uri))


def test_strict_nullable_cast_failure():
    schema = pa.schema([pa.field('optional', pa.int32())])
    with pytest.raises(ValueError, match='optional'):
        _cast_to_schema(pa.table({'optional': ['bad']}), schema, strict=True)


def test_cast_preserves_contract_and_operational_fields():
    schema = get_silver_schema('sensor_telemetry')
    table = _to_silver_frame(sensor(), 'sensor_telemetry').to_arrow()
    out = _cast_to_schema(table, schema, strict=True)
    assert out.schema.field('line_id').nullable is False
    assert out.schema.field('uht_temp').nullable is False
    assert out.schema.field('batch_id') == schema.field('batch_id')
    assert out['batch_id'].null_count == 1
    assert '_generated_at' in out.column_names


@pytest.mark.parametrize('initial', ['new', 'created', 'legacy'])
def test_valid_append_to_existing_schema(tmp_path, initial):
    uri = str(tmp_path / initial)
    frame = sensor().drop('_generated_at').with_columns(
        pl.lit('test.parquet').alias('_bronze_file'), pl.lit('abc').alias('_bronze_sha256'))
    if initial == 'created':
        create_delta_table(uri, get_silver_schema('sensor_telemetry'), ['line_id', 'ingestion_date'])
    if initial == 'legacy':
        legacy = _to_silver_frame(frame, 'sensor_telemetry').to_arrow()
        legacy = _cast_to_schema(legacy, get_silver_schema('sensor_telemetry'))
        legacy = pa.table({name: legacy[name] for name in legacy.column_names})
        write_deltalake(uri, legacy, partition_by=['line_id', 'ingestion_date'], storage_options=get_storage_options())
    append_to_delta(frame, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
    assert read_silver_table(uri).height == (2 if initial == 'legacy' else 1)
    schema = DeltaTable(uri, storage_options=get_storage_options()).schema().to_arrow()
    if initial != 'legacy':
        assert not schema.field('line_id').nullable



@pytest.mark.parametrize('entry', ['append', 'ingest'])
@pytest.mark.parametrize(('zone', 'text', 'utc_text'), [
    ('Asia/Ho_Chi_Minh', '2026-01-16T01:00:00+07:00', '2026-01-15T18:00:00+00:00'),
    ('America/New_York', '2026-01-14T23:00:00-05:00', '2026-01-15T04:00:00+00:00'),
])
def test_partition_date_matches_same_instant_across_input_types(tmp_path, entry, zone, text, utc_text):
    from datetime import date
    from lakehouse_storage.bronze_writer import write_bronze_batch
    frames = [sensor().with_columns(pl.lit(text).alias('timestamp'))]
    frames.append(frames[0].with_columns(pl.col('timestamp').str.to_datetime(time_zone=zone)))
    results = []
    for index, frame in enumerate(frames):
        uri = str(tmp_path / f'silver_{index}')
        if entry == 'append':
            append_to_delta(frame, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
        else:
            path = write_bronze_batch(frame, 'sensor_telemetry', tmp_path / 'bronze', validate_schema=False)
            ingest_bronze_file(path, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
        result = read_silver_table(uri, date_from='2026-01-15', date_to='2026-01-15')
        assert result.height == 1
        assert result['timestamp'][0] == datetime.fromisoformat(utc_text)
        assert result['ingestion_date'][0] == date(2026, 1, 15)
        results.append(result.select('timestamp', 'ingestion_date'))
    assert results[0].equals(results[1])


def test_partition_conversion_respects_factory_timezone_and_explicit_date(monkeypatch):
    from datetime import date
    from lakehouse_storage import silver_manager
    monkeypatch.setattr(silver_manager, 'TIMEZONE', 'Asia/Ho_Chi_Minh')
    frames = [sensor().with_columns(pl.lit('2026-01-15T18:00:00Z').alias('timestamp'))]
    frames.append(frames[0].with_columns(pl.col('timestamp').str.to_datetime(time_zone='UTC')))
    frames.append(sensor().with_columns(pl.lit(datetime(2026, 1, 16, 1)).alias('timestamp')))
    for frame in frames:
        transformed = _to_silver_frame(frame, 'sensor_telemetry')
        assert transformed['ingestion_date'][0] == date(2026, 1, 16)
        assert transformed.schema['timestamp'].time_zone == 'Asia/Ho_Chi_Minh'
        supplied = frame.with_columns(pl.lit(date(2025, 12, 31)).alias('ingestion_date'))
        assert _to_silver_frame(supplied, 'sensor_telemetry')['ingestion_date'][0] == date(2025, 12, 31)
    date_frame = pl.DataFrame({'period_start': [date(2026, 1, 1)]})
    assert _to_silver_frame(date_frame, 'market_indices')['ingestion_date'][0] == date(2026, 1, 1)
