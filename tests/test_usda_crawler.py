import socket
import subprocess
from unittest.mock import Mock

import pytest
import requests

from crawling_ingestion import usda


def test_dns_failure_uses_fallback(monkeypatch):
    failure = requests.ConnectionError(socket.gaierror(11002, "DNS failed"))
    monkeypatch.setattr(usda.requests, "get", Mock(side_effect=failure))
    fallback = Mock(return_value=[{"slug_id": "2993"}])
    monkeypatch.setattr(usda, "_get_json_with_public_dns", fallback)
    assert usda._get_usda_reports() == [{"slug_id": "2993"}]
    fallback.assert_called_once_with(usda.USDA_BASE_API)


@pytest.mark.parametrize("error", [requests.exceptions.SSLError("certificate"),
                                    requests.ConnectionError("connection refused")])
def test_other_connection_errors_do_not_use_dns_fallback(monkeypatch, error):
    monkeypatch.setattr(usda.requests, "get", Mock(side_effect=error))
    fallback = Mock()
    monkeypatch.setattr(usda, "_get_json_with_public_dns", fallback)
    with pytest.raises(type(error)):
        usda._get_usda_reports()
    fallback.assert_not_called()


def test_http_error_is_not_silently_ignored(monkeypatch):
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("503")
    monkeypatch.setattr(usda.requests, "get", Mock(return_value=response))
    with pytest.raises(requests.HTTPError):
        usda._get_usda_reports()


def test_dns_fallback_keeps_hostname_and_tls(monkeypatch):
    run = Mock(side_effect=[
        subprocess.CompletedProcess([], 0, stdout="192.0.2.1\n"),
        subprocess.CompletedProcess([], 0, stdout='{"results": []}'),
    ])
    monkeypatch.setattr(usda.subprocess, "run", run)
    assert usda._get_json_with_public_dns(usda.USDA_BASE_API) == {"results": []}
    args = run.call_args_list[1].args[0]
    assert args[-1] == usda.USDA_BASE_API
    assert "mpr.datamart.ams.usda.gov:443:192.0.2.1" in args
    assert "--insecure" not in args and "-k" not in args
    assert run.call_args_list[1].kwargs["check"] is True


def test_includes_last_day_and_skips_empty_sections(monkeypatch):
    monkeypatch.setattr(usda, "_find_usda_report_by_keyword", lambda _: [
        {"slug_id": "2993", "sectionNames": ["Prices", "Empty"]}
    ])
    def section(slug, name):
        rows = [] if name == "Empty" else [
            {"published_date": "06/29/2025 23:59:59", "price": "2"},
            {"published_date": "06/30/2025 00:00:00", "price": "3"},
        ]
        return {"results": rows, "stats": {"totalRows:": len(rows), "returnedRows:": len(rows)}}
    monkeypatch.setattr(usda, "_get_usda_report_section_detail", section)
    frames = usda.get_usda_marketreports_data("2025-01-01", "2025-06-29")
    assert len(frames) == 1
    assert frames[0].height == 1
    assert frames[0]["published_date"][0] == "06/29/2025 23:59:59"
