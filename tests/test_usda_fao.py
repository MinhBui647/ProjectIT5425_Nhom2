"""Offline contracts for USDA/FAO, including the existing Bronze writer."""
import json
from unittest.mock import Mock

import polars as pl
import pytest
import requests

from crawling_ingestion import fao, usda, _http
from crawling_ingestion.run_crawlers import run_crawlers
from lakehouse_storage import write_bronze_batch
from lakehouse_storage.bronze_writer import verify_checksum
from lakehouse_storage.schemas import get_bronze_schema


CSV = "FAO Food Price Index\n2014-2016=100\nDate,Food Price Index,Meat,Dairy,Cereals,Oils,Sugar,,\n2024-12,1,2,140,4,5,6,,\n2025-01,1,2,143.1,4,5,6,,\n2025-02,1,2,147.7,4,5,6,,\n"


def response(payload=None, text="", status=200, headers=None):
    obj = Mock(status_code=status, headers=headers or {}, text=text, content=text.encode())
    obj.json.return_value = payload
    if status >= 400:
        obj.raise_for_status.side_effect = requests.HTTPError(response=obj)
    return obj


def usda_payload(rows):
    return {"reportSection": usda.USDA_PRODUCT_SECTIONS["BUTTER"],
            "stats": {"totalRows:": len(rows)}, "results": rows}


def usda_row(day="01/04/2025", price="2.50"):
    return {"week_ending_date": day, "Butter_Price": price, "Butter_Sales": "1,000"}


def mock_fao(monkeypatch, csv=CSV):
    replies = iter([response(text='<a href="/dataset/food_price_indices_data.csv?v=1&amp;x=2">CSV</a>'), response(text=csv)])
    monkeypatch.setattr(fao, "get_response", lambda *args: next(replies))


def test_usda_range_case_sensitive_source_fields_and_raw(monkeypatch):
    payload = usda_payload([usda_row("12/28/2024"), usda_row(), usda_row("01/11/2025", None)])
    monkeypatch.setattr(usda, "get_response", lambda *args: response(payload))
    df = usda.get_usda_marketprice_data("2025-01-04", "2025-01-11", ["BUTTER"])
    assert df["price"].to_list() == [2.5, None]
    assert df["unit"].to_list() == ["lb", "lb"]
    assert df["_price_status"].to_list() == ["OK", "MISSING_OR_INVALID"]
    assert json.loads(df["_raw_record"][1])["Butter_Price"] is None


@pytest.mark.parametrize("invalid", ["NaN", "Infinity", "-1", "0", "(D)", True])
def test_usda_invalid_values_remain_null(monkeypatch, invalid):
    monkeypatch.setattr(usda, "get_response", lambda *args: response(usda_payload([usda_row(price=invalid)])))
    assert usda.get_usda_marketprice_data("2025-01-01", "2025-01-31", ["BUTTER"])["price"][0] is None


@pytest.mark.parametrize("kind", ["duplicate", "truncated", "wrong_section", "ambiguous"])
def test_usda_bad_source_fails(monkeypatch, kind):
    payload = usda_payload([usda_row()])
    if kind == "duplicate":
        payload = usda_payload([usda_row(), usda_row()])
    elif kind == "truncated":
        payload["stats"]["totalRows:"] = 2
    elif kind == "wrong_section":
        payload["reportSection"] = "Summary"
    else:
        payload["results"][0]["other_price"] = "3"
    monkeypatch.setattr(usda, "get_response", lambda *args: response(payload))
    with pytest.raises(ValueError):
        usda.get_usda_marketprice_data("2025-01-01", "2025-01-31", ["BUTTER"])


def test_fao_values_base_and_month_start(monkeypatch):
    mock_fao(monkeypatch)
    df = fao.get_fao_dairyindex_data("2025-01-01", "2025-02-01")
    assert df["index_value"].to_list() == [143.1, 147.7]
    assert set(df["base_period"]) == {"2014-2016=100"}
    assert df["_source_url"][0].endswith("?v=1&x=2")
    mock_fao(monkeypatch)
    assert fao.get_fao_dairyindex_data("2025-01-15", "2025-02-28")["period_start"].to_list() == ["2025-02-01"]


@pytest.mark.parametrize("csv", [CSV.replace("2014-2016", "2020-2022"), CSV.replace("Dairy", "Other"), CSV + "2025-01,1,2,150,4,5,6\n"])
def test_fao_rejects_changed_contract_or_duplicates(monkeypatch, csv):
    mock_fao(monkeypatch, csv)
    with pytest.raises(ValueError):
        fao.get_fao_dairyindex_data("2025-01-01", "2025-02-28")


def test_fao_missing_and_empty(monkeypatch):
    mock_fao(monkeypatch, CSV.replace("143.1", "NaN"))
    assert fao.get_fao_dairyindex_data("2025-01-01", "2025-01-31")["index_value"][0] is None
    mock_fao(monkeypatch)
    empty = fao.get_fao_dairyindex_data("2000-01-01", "2000-01-31")
    assert empty.is_empty() and empty.schema == fao.FAO_SCHEMA


def test_no_http_for_bad_input(monkeypatch):
    no_http = Mock(side_effect=AssertionError("must validate before HTTP"))
    monkeypatch.setattr(usda, "get_response", no_http)
    monkeypatch.setattr(fao, "get_response", no_http)
    for products in [[], ["INVALID"], ["BUTTER", "BUTTER"]]:
        with pytest.raises(ValueError):
            usda.get_usda_marketprice_data("2025-01-01", "2025-02-01", products)
    with pytest.raises(ValueError):
        fao.get_fao_dairyindex_data("2025-02-01", "2025-01-01")


def test_both_crawlers_write_bronze_and_keep_gdt_schema(monkeypatch, tmp_path):
    monkeypatch.setattr(usda, "get_response", lambda url, _: response({
        **usda_payload([usda_row()]), "reportSection": __import__("urllib.parse", fromlist=["unquote"]).unquote(url.rsplit("/", 1)[1])}))
    mock_fao(monkeypatch)
    paths = run_crawlers("2025-01-01", "2025-02-01", ["usda", "fao"], tmp_path)
    assert len(paths) == 3
    assert all(verify_checksum(p) for p in paths)
    assert "EventNumber" in get_bronze_schema("market_prices").names
    assert (tmp_path / "usda_market_prices" / "2025-01-04").is_dir()
    frames = [pl.read_parquet(p) for p in paths]
    assert sum(len(df) for df in frames) == 7


def test_http_retry_after_and_permanent_failure(monkeypatch):
    replies = iter([response(status=429, headers={"Retry-After": "3"}), response(payload={"ok": True})])
    monkeypatch.setattr(_http.requests, "get", lambda *a, **k: next(replies))
    sleeps = []
    monkeypatch.setattr(_http.time, "sleep", sleeps.append)
    monkeypatch.setattr(_http.random, "uniform", lambda *a: 0)
    _http._LAST_REQUEST.clear()
    assert _http.get_response("https://example.test", "test").json() == {"ok": True}
    assert 3 in sleeps
    bad = Mock(return_value=response(status=404))
    monkeypatch.setattr(_http.requests, "get", bad)
    with pytest.raises(requests.HTTPError):
        _http.get_response("https://example.test", "test")
    assert bad.call_count == 1


def test_http_retry_exhaustion_and_bad_json_not_retried(monkeypatch):
    monkeypatch.setattr(_http.time, "sleep", lambda _: None)
    bad = Mock(return_value=response(status=503))
    monkeypatch.setattr(_http.requests, "get", bad)
    with pytest.raises(requests.HTTPError):
        _http.get_response("https://example.test", "test")
    assert bad.call_count == 5
    broken = response()
    broken.json.side_effect = ValueError("invalid JSON")
    bad = Mock(return_value=broken)
    monkeypatch.setattr(usda, "get_response", bad)
    with pytest.raises(ValueError):
        usda.get_usda_marketprice_data("2025-01-01", "2025-01-31", ["BUTTER"])
    assert bad.call_count == 1
