"""Tests for the persisted Marquez lineage evidence bundle."""

import json

import pytest

from data_governance.marquez_evidence import (
    collect_marquez_evidence,
    write_marquez_evidence,
)


def _responses():
    input_id = "dataset:file://bronze:sensor_telemetry/input.parquet"
    job_id = "job:smart-dairy-factory:bronze_to_silver.sensor_telemetry"
    output_id = (
        "dataset:s3://smart-dairy-lakehouse:"
        "02_silver_curated/sensor_telemetry"
    )
    return {
        "job": {
            "name": "bronze_to_silver.sensor_telemetry",
            "inputs": [
                {
                    "namespace": "file://bronze",
                    "name": "sensor_telemetry/input.parquet",
                }
            ],
            "outputs": [
                {
                    "namespace": "s3://smart-dairy-lakehouse",
                    "name": "02_silver_curated/sensor_telemetry",
                }
            ],
            "latestRun": {"state": "COMPLETED"},
        },
        "runs": {"runs": [{"state": "COMPLETED"}]},
        "lineage": {
            "graph": [
                {
                    "id": input_id,
                    "inEdges": [],
                    "outEdges": [
                        {"origin": input_id, "destination": job_id}
                    ],
                },
                {
                    "id": job_id,
                    "inEdges": [
                        {"origin": input_id, "destination": job_id}
                    ],
                    "outEdges": [
                        {"origin": job_id, "destination": output_id}
                    ],
                },
                {
                    "id": output_id,
                    "inEdges": [
                        {"origin": job_id, "destination": output_id}
                    ],
                    "outEdges": [],
                },
            ]
        },
    }


def test_collects_job_run_and_dataset_lineage_evidence():
    responses = _responses()

    def fetcher(url):
        if "/lineage?" in url:
            return responses["lineage"]
        if "/runs?" in url:
            return responses["runs"]
        return responses["job"]

    payload = collect_marquez_evidence(
        endpoint="http://localhost:5000/",
        fetcher=fetcher,
    )

    assert payload["subject"]["latest_run_state"] == "COMPLETED"
    assert payload["subject"]["input_datasets"] == [
        "file://bronze/sensor_telemetry/input.parquet"
    ]
    assert payload["subject"]["output_datasets"] == [
        "s3://smart-dairy-lakehouse/02_silver_curated/sensor_telemetry"
    ]
    assert len(payload["subject"]["verified_edges"]) == 2
    assert set(payload["api_responses"]) == {"job", "runs", "lineage"}


def test_rejects_evidence_without_both_dataset_edges():
    responses = _responses()
    responses["job"]["outputs"] = []

    def fetcher(url):
        if "/lineage?" in url:
            return responses["lineage"]
        if "/runs?" in url:
            return responses["runs"]
        return responses["job"]

    with pytest.raises(ValueError, match="no output dataset"):
        collect_marquez_evidence(
            endpoint="http://localhost:5000",
            fetcher=fetcher,
        )


@pytest.mark.parametrize("missing_side", ["input", "output"])
def test_rejects_lineage_graph_with_missing_expected_edge(missing_side):
    responses = _responses()
    graph = responses["lineage"]["graph"]
    if missing_side == "input":
        graph[0]["outEdges"] = []
        graph[1]["inEdges"] = []
    else:
        graph[1]["outEdges"] = []
        graph[2]["inEdges"] = []

    def fetcher(url):
        if "/lineage?" in url:
            return responses["lineage"]
        if "/runs?" in url:
            return responses["runs"]
        return responses["job"]

    with pytest.raises(ValueError, match="missing expected edge"):
        collect_marquez_evidence(
            endpoint="http://localhost:5000",
            fetcher=fetcher,
        )


def test_writes_valid_json_atomically(tmp_path):
    target = write_marquez_evidence(
        {"subject": {"job_name": "bronze_to_silver.sensor_telemetry"}},
        tmp_path / "lineage" / "sensor.json",
    )

    assert json.loads(target.read_text(encoding="utf-8"))["subject"]["job_name"] == (
        "bronze_to_silver.sensor_telemetry"
    )
    assert not list(target.parent.glob("*.tmp-*"))
