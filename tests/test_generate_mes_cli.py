"""Generator CLI publishes only to the selected roots and preserves lineage."""

import json
import sys
from datetime import datetime, timedelta

import polars as pl
import pytest

import lakehouse_storage.generate_mes_data as cli
from lakehouse_storage.bronze_writer import verify_checksum
from olap_engineering.build_marts import load_batch_farm_map


def run_cli(monkeypatch, root, *arguments):
    monkeypatch.setattr(sys, "argv", [
        "generate_mes_data", "--date", "2025-01-01", "--days", "1",
        "--output-dir", str(root / "synthetic"), *arguments,
    ])
    return cli.main()


def read_mapping(root):
    return load_batch_farm_map(root / "synthetic" / "batch_farm_map.json")


def test_bronze_contains_two_lines_checksums_and_matching_batches(tmp_path, monkeypatch):
    bronze = tmp_path / "bronze"
    assert run_cli(monkeypatch, tmp_path, "--write-bronze", "--bronze-root", str(bronze)) == 0
    frames = {}
    for table, rows in [("sensor_telemetry", 28800), ("mes_lims", 4)]:
        files = sorted((bronze / table).rglob("*.parquet"))
        assert len(files) == 2
        assert all(verify_checksum(path) for path in files)
        frames[table] = pl.concat([pl.read_parquet(path) for path in files])
        assert frames[table].height == 2 * rows
        assert set(frames[table]["line_id"]) == {"LINE_UHT_1", "LINE_UHT_2"}
    batches = set(frames["mes_lims"]["batch_id"])
    assert len(batches) == 8
    assert set(frames["sensor_telemetry"]["batch_id"]) == batches == set(read_mapping(tmp_path))


def test_explicit_line_and_flag_root_override_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE_GENERATOR_BRONZE_ROOT", str(tmp_path / "unused"))
    bronze = tmp_path / "selected"
    assert run_cli(monkeypatch, tmp_path, "--write-bronze", "--bronze-root", str(bronze),
                   "--line-id", "LINE_UHT_2") == 0
    files = list((bronze / "mes_lims").rglob("*.parquet"))
    assert len(files) == 1
    assert pl.read_parquet(files[0])["line_id"].unique().to_list() == ["LINE_UHT_2"]
    assert not (tmp_path / "unused").exists()
    assert len(read_mapping(tmp_path)) == 4


def test_raw_export_defaults_to_two_lines(tmp_path, monkeypatch):
    assert run_cli(monkeypatch, tmp_path) == 0
    output = tmp_path / "synthetic"
    assert pl.read_parquet(output / "sensors_2025-01.parquet").height == 57600
    assert pl.read_parquet(output / "mes_2025-01.parquet").height == 8
    assert len(read_mapping(tmp_path)) == 8


def test_seed_zero_and_next_day_are_reproducible(tmp_path, monkeypatch):
    assert run_cli(monkeypatch, tmp_path, "--seed", "0", "--days", "2",
                   "--line-id", "LINE_UHT_1") == 0
    expected = pl.concat([
        cli.generate_daily_production(datetime(2025, 1, 1) + timedelta(days=d), seed=d)[1]
        for d in range(2)
    ])
    observed = pl.read_parquet(tmp_path / "synthetic" / "mes_2025-01.parquet")
    assert observed.drop("_generated_at").equals(expected.drop("_generated_at"))


@pytest.mark.parametrize("arguments", [
    ["--write-bronze", "--write-delta"], ["--write-delta"],
    ["--days", "0"], ["--days", "-1"], ["--seed", "-1"],
    ["--date", "2025-02-30"], ["--defect-rate", "nan"],
    ["--defect-rate", "inf"], ["--defect-rate", "-0.1"], ["--defect-rate", "1.1"],
    ["--line-id", ""], ["--line-id", "UNKNOWN"],
    ["--line-id", "LINE_UHT_1,LINE_UHT_1"],
])
def test_invalid_arguments_do_not_publish(tmp_path, monkeypatch, arguments):
    monkeypatch.delenv("STORAGE_GENERATOR_SILVER_ROOT", raising=False)
    with pytest.raises(SystemExit) as error:
        run_cli(monkeypatch, tmp_path, *arguments)
    assert error.value.code == 2
    assert not list(tmp_path.rglob("*.parquet"))


def test_environment_roots_reach_governance(tmp_path, monkeypatch):
    import lakehouse_storage.governance_adapter as adapter

    roots = {name: str(tmp_path / name) for name in ["bronze", "silver", "reports"]}
    for name, value in roots.items():
        monkeypatch.setenv(f"STORAGE_GENERATOR_{name.upper()}_ROOT", value)
    calls = []

    def accept(path, table, **options):
        assert verify_checksum(path)
        calls.append((table, options))
        return {"status": "PASS", "rows_written": 1}

    monkeypatch.setattr(adapter, "ingest_governed_bronze_file", accept)
    assert run_cli(monkeypatch, tmp_path, "--write-delta") == 0
    assert [table for table, _ in calls] == ["sensor_telemetry", "mes_lims"] * 2
    for _, options in calls:
        assert options == {f"{key}_root": value for key, value in roots.items()}
    assert len(read_mapping(tmp_path)) == 8


@pytest.mark.parametrize("status", ["FAIL", "NEEDS_REVIEW"])
def test_rejected_p3_retains_bronze_and_mapping(tmp_path, monkeypatch, status):
    import lakehouse_storage.governance_adapter as adapter

    monkeypatch.setattr(adapter, "ingest_governed_bronze_file", lambda *args, **kwargs: {"status": status})
    assert run_cli(monkeypatch, tmp_path, "--write-delta", "--bronze-root", str(tmp_path / "bronze"),
                   "--silver-root", str(tmp_path / "silver"), "--reports-root", str(tmp_path / "reports")) == 1
    assert list((tmp_path / "bronze").rglob("*.parquet"))
    assert len(read_mapping(tmp_path)) == 4
    assert not (tmp_path / "silver").exists()


def test_incremental_mapping_preserves_old_batches(tmp_path, monkeypatch):
    arguments = ["--write-bronze", "--bronze-root", str(tmp_path / "bronze")]
    assert run_cli(monkeypatch, tmp_path, *arguments) == 0
    original = read_mapping(tmp_path)
    assert run_cli(monkeypatch, tmp_path, *arguments, "--date", "2025-01-02", "--seed", "5426") == 0
    combined = read_mapping(tmp_path)
    assert len(combined) == 16
    assert all(combined[batch] == farm for batch, farm in original.items())


@pytest.mark.parametrize("contents", [
    "not JSON", '{}', '["bad"]', '[{"B": null}]', '[{"B": {"farm_id": 123}}]',
    '[{"B": {"farm_id": "MOC_CHAU"}}, {"B": {"farm_id": "BA_VI"}}]',
])
def test_bad_existing_mapping_is_preserved_before_publish(tmp_path, monkeypatch, contents):
    output = tmp_path / "synthetic"
    output.mkdir()
    mapping = output / "batch_farm_map.json"
    mapping.write_text(contents)
    with pytest.raises(SystemExit) as error:
        run_cli(monkeypatch, tmp_path, "--write-bronze", "--bronze-root", str(tmp_path / "bronze"))
    assert error.value.code == 2
    assert mapping.read_text() == contents
    assert not list(tmp_path.rglob("*.parquet"))


def test_new_mapping_conflict_does_not_publish(tmp_path, monkeypatch):
    output = tmp_path / "synthetic"
    output.mkdir()
    (output / "batch_farm_map.json").write_text(json.dumps([
        {"BATCH_20250101_01_LINE_UHT_1": {"farm_id": "DIFFERENT"}},
    ]))
    with pytest.raises(SystemExit) as error:
        run_cli(monkeypatch, tmp_path, "--write-bronze", "--bronze-root", str(tmp_path / "bronze"))
    assert error.value.code == 2
    assert not list(tmp_path.rglob("*.parquet"))


def test_atomic_mapping_failure_preserves_old_json_and_cleans_temp(tmp_path, monkeypatch):
    arguments = ["--write-bronze", "--bronze-root", str(tmp_path / "bronze")]
    assert run_cli(monkeypatch, tmp_path, *arguments) == 0
    mapping = tmp_path / "synthetic" / "batch_farm_map.json"
    original = mapping.read_bytes()

    def fail_replace(*args):
        raise OSError("injected mapping failure")

    monkeypatch.setattr(cli.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected mapping failure"):
        run_cli(monkeypatch, tmp_path, *arguments, "--date", "2025-01-02", "--seed", "5426")
    assert mapping.read_bytes() == original
    assert not list((tmp_path / "synthetic").glob(".batch_farm_map-*.tmp"))


def test_split_runs_match_a_continuous_run_with_day_seed_offsets(tmp_path, monkeypatch):
    continuous, split = tmp_path / "continuous", tmp_path / "split"
    for root, date_value, days, seed in [
        (continuous, "2025-01-01", "2", "0"),
        (split, "2025-01-01", "1", "0"),
        (split, "2025-01-02", "1", "1"),
    ]:
        assert run_cli(monkeypatch, root, "--write-bronze", "--bronze-root", str(root / "bronze"),
                       "--date", date_value, "--days", days, "--seed", seed, "--line-id", "LINE_UHT_1") == 0
    frames = []
    for root in (continuous, split):
        files = (root / "bronze" / "mes_lims").rglob("*.parquet")
        frames.append(pl.concat([pl.read_parquet(path) for path in files]).drop("_generated_at").sort("batch_id"))
    assert frames[0].equals(frames[1])
    assert read_mapping(continuous) == read_mapping(split)
