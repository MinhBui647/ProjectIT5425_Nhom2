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
    assert calls == [{'start_date': '2026-01-01', 'end_date': '2026-01-31'}]


def test_invalid_mode_and_window_rejected():
    with pytest.raises(ValueError):
        SourceConfig(mode='production-scada', start='2026-01-01', end='2026-02-01')
    with pytest.raises(ValueError):
        SourceConfig(start='2026-02-01T00:00:00Z', end='2026-01-01T00:00:00Z')
