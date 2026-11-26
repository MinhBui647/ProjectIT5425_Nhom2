from datetime import datetime, timezone
import polars as pl
import pytest
from lakehouse_pipeline.source_config import SourceConfig
from lakehouse_pipeline.sources import collect_live


def test_live_calls_collector_with_explicit_window(monkeypatch):
    calls = []
    monkeypatch.setattr('crawling_ingestion.gdt.get_gdt_marketprice_data',
                        lambda **kw: calls.append(kw) or pl.DataFrame({'raw': [1]}))
    config = SourceConfig(mode='live', sources=['market_prices'], start='2026-01-01T00:00:00Z', end='2026-02-01T00:00:00Z')
    assert list(collect_live(config))[0][0] == 'market_prices'
    assert calls == [{'start_date': '2026-01-01', 'end_date': '2026-02-01'}]


def test_invalid_mode_and_window_rejected():
    with pytest.raises(ValueError):
        SourceConfig(mode='production-scada', start='2026-01-01', end='2026-02-01')
    with pytest.raises(ValueError):
        SourceConfig(start='2026-02-01T00:00:00Z', end='2026-01-01T00:00:00Z')


@pytest.mark.parametrize('start,end', [
    ('2026-01-15T00:00:00Z', '2026-01-16T00:00:00Z'),
    ('2026-01-15T08:00:00Z', '2026-01-15T18:00:00Z'),
    ('2026-01-31T08:00:00Z', '2026-02-01T18:00:00Z'),
    ('2026-01-15T07:00:00+07:00', '2026-01-16T07:00:00+07:00'),
])
def test_live_gdt_actual_collector_preserves_last_day_and_half_open_window(monkeypatch, start, end):
    from datetime import timedelta
    from crawling_ingestion import gdt

    config = SourceConfig(mode='live', sources=['market_prices'], start=start, end=end)
    first, last = config.utc_window()
    times = [first - timedelta(days=1), first, last - timedelta(seconds=1), last]
    events = [dict(EventDate=instant.strftime(gdt.GDT_PRICE_DATE_FORMAT),
                   EventNumber=str(390 + i), AveragePublishedPrice='3100')
              for i, instant in enumerate(times)]
    monkeypatch.setattr(gdt, '_get_gdt_latest_event_guid', lambda: 'guid')
    monkeypatch.setattr(gdt, '_get_gdt_product_summaries', lambda _: [{'ProductGroupCode': 'WMP'}])
    monkeypatch.setattr(gdt, '_get_gdt_latest_winning_price', lambda *_: [e.copy() for e in events])
    batches = list(collect_live(config))
    assert len(batches) == 1 and batches[0][0] == 'market_prices'
    assert {row['EventNumber'] for row in batches[0][1]} == {'391', '392'}


def test_live_weather_keeps_inclusive_date_request(monkeypatch):
    calls = []
    monkeypatch.setattr('crawling_ingestion.openmeteo.get_openmeteo_weather_data',
                        lambda **kw: calls.append(kw) or pl.DataFrame())
    config = SourceConfig(mode='live', sources=['weather'], farms=[('MOC_CHAU', 20.8, 104.6)],
                          start='2026-01-01T00:00:00Z', end='2026-02-01T00:00:00Z')
    assert list(collect_live(config)) == [('weather', [])]
    assert calls[0]['start_date'] == '2026-01-01' and calls[0]['end_date'] == '2026-01-31'
