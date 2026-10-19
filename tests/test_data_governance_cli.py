import json
from pathlib import Path

import pytest

from data_governance import run_governance as cli


def test_parser_supports_table_aliases_dates_and_lineage_flags():
    namespace = cli.build_parser().parse_args(
        [
            "--table",
            "S1",
            "--tables",
            "weather,food_recalls",
            "--date-from",
            "2026-01-01",
            "--date-to",
            "2026-01-31",
            "--fail-threshold",
            "0.01",
            "--dry-run",
            "--build-data-docs",
            "--no-lineage",
        ]
    )
    assert cli.selected_tables(namespace) == (
        "sensor_telemetry",
        "weather",
        "food_recalls",
    )
    assert namespace.date_from.isoformat() == "2026-01-01"
    assert namespace.fail_threshold == 0.01
    assert namespace.dry_run and namespace.build_data_docs
    assert namespace.lineage is False


def test_discovery_order_is_stable_by_table_then_path(tmp_path):
    for relative in (
        "weather/z.parquet",
        "weather/a.parquet",
        "sensor_telemetry/b.parquet",
        "sensor_telemetry/a.parquet",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    found = cli.discover_bronze_files(
        tmp_path, ("sensor_telemetry", "weather")
    )
    assert [path.relative_to(tmp_path).as_posix() for path in found] == [
        "sensor_telemetry/a.parquet",
        "sensor_telemetry/b.parquet",
        "weather/a.parquet",
        "weather/z.parquet",
    ]


@pytest.mark.parametrize(
    ("statuses", "categories", "expected"),
    [
        (("PASS", "PASS_WITH_REJECTIONS"), (None, None), 0),
        (("PASS", "FAIL"), (None, "quality"), 2),
        (("FAIL", "PASS"), ("infrastructure", None), 3),
    ],
)
def test_exit_codes_continue_after_each_file(
    tmp_path, monkeypatch, capsys, statuses, categories, expected
):
    files = [tmp_path / "sensor_telemetry" / f"{index}.parquet" for index in range(2)]
    monkeypatch.setattr(cli, "discover_bronze_files", lambda *_args: files)
    seen = []

    def fake_process(path, **kwargs):
        index = int(path.stem)
        seen.append((path.name, kwargs["dry_run"]))
        return {
            "status": statuses[index],
            "error_category": categories[index],
            "rows_written": 0,
        }

    monkeypatch.setattr(cli, "process_bronze_file", fake_process)
    exit_code = cli.main(
        [
            "--table",
            "sensor_telemetry",
            "--bronze-root",
            str(tmp_path),
            "--silver-root",
            str(tmp_path / "silver"),
            "--dry-run",
            "--no-lineage",
        ]
    )

    summary = json.loads(capsys.readouterr().out)
    assert exit_code == expected
    assert summary["files_processed"] == 2
    assert seen == [("0.parquet", True), ("1.parquet", True)]


def test_no_input_is_infrastructure_exit_3(tmp_path, capsys):
    assert cli.main(["--bronze-root", str(tmp_path), "--no-lineage"]) == 3
    summary = json.loads(capsys.readouterr().out)
    assert summary["exit_code"] == 3
    assert "No Bronze Parquet files" in summary["error"]


def test_data_docs_failure_happens_before_silver_commit(tmp_path, monkeypatch):
    fixture_root = Path(__file__).parent / "fixtures" / "bronze"
    bronze_path = next((fixture_root / "sensor_telemetry").glob("*.parquet"))
    silver_was_called = False

    def fail_docs(*_args, **_kwargs):
        raise OSError("Data Docs unavailable")

    def track_silver(*_args, **_kwargs):
        nonlocal silver_was_called
        silver_was_called = True
        return {}

    monkeypatch.setattr(cli, "run_gx_checkpoint", fail_docs)
    monkeypatch.setattr(cli, "write_governed_silver", track_silver)
    result = cli.process_bronze_file(
        bronze_path,
        bronze_root=fixture_root,
        table_name="sensor_telemetry",
        silver_root=str(tmp_path / "silver"),
        fail_threshold=0.005,
        date_from=None,
        date_to=None,
        dry_run=False,
        emitter=None,
        reports_root=tmp_path / "reports",
        quarantine_root=tmp_path / "quarantine",
        figures_root=tmp_path / "figures",
        gx_context=object(),
    )

    assert result["status"] == "FAIL"
    assert result["error_category"] == "infrastructure"
    assert silver_was_called is False
    assert not (tmp_path / "silver").exists()


def test_loader_failure_persists_auditable_summary(tmp_path, monkeypatch):
    bronze_root = tmp_path / "bronze"
    bronze_path = bronze_root / "sensor_telemetry" / "bad.parquet"

    def fail_loader(*_args, **_kwargs):
        raise ValueError("Checksum mismatch for Bronze file")

    monkeypatch.setattr(cli, "load_bronze", fail_loader)
    result = cli.process_bronze_file(
        bronze_path,
        bronze_root=bronze_root,
        table_name="sensor_telemetry",
        silver_root=str(tmp_path / "silver"),
        fail_threshold=0.005,
        date_from=None,
        date_to=None,
        dry_run=False,
        emitter=None,
        reports_root=tmp_path / "reports",
        quarantine_root=tmp_path / "quarantine",
        figures_root=tmp_path / "figures",
        gx_context=None,
    )

    summary_path = Path(result["reports"]["summary"])
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["error_category"] == "quality"
    assert result["rows_written"] == 0
    assert summary["failure_stage"] == "load_bronze"
    assert summary["decision"]["status"] == "FAIL"
    assert summary["counts"]["total"] is None
    assert not (tmp_path / "silver").exists()
