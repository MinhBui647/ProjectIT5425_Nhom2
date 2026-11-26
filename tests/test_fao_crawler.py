"""Tests for the FAO crawler (no network: the Excel download is faked)."""
import polars as pl

from crawling_ingestion import fao


class _FakeResponse:
    status_code = 200
    content = b"excel"


def test_fao_month_is_plain_date(monkeypatch):
    # Shape of sheet 3: a notes row, a header row, then data; column 0 is the year.
    sheet = pl.DataFrame({
        "c0": ["notes", "Year", "2025", "2025"],
        "c1": ["", "Month", "2025-01-01 00:00:00", "2025-02-01 00:00:00"],
        "c2": ["", "Food Price Index", "107.4", "109.1"],
    })
    monkeypatch.setattr(fao.requests, "get", lambda *args, **kwargs: _FakeResponse())
    monkeypatch.setattr(fao.pl, "read_excel", lambda *args, **kwargs: sheet)

    df = fao._get_and_process_fao_sheet_data("https://example.invalid/fao.xlsx")

    assert df["Month"].to_list() == ["2025-01-01", "2025-02-01"]
    assert df["Food Price Index"].to_list() == ["107.4", "109.1"]