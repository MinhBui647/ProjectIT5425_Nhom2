"""Export auditable Marquez API evidence for one Bronze-to-Silver job."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import urlopen


DEFAULT_NAMESPACE = "smart-dairy-factory"
DEFAULT_JOB = "bronze_to_silver.sensor_telemetry"


def _fetch_json(url: str, timeout: float = 15) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as response:  # noqa: S310 - configured API URL
        return json.load(response)


def _dataset_names(items: list[dict[str, Any]]) -> list[str]:
    return [
        f"{item.get('namespace', '')}/{item.get('name', '')}".rstrip("/")
        for item in items
    ]


def _dataset_node_id(item: dict[str, Any]) -> str:
    return f"dataset:{item.get('namespace', '')}:{item.get('name', '')}"


def validate_lineage_graph(
    lineage: dict[str, Any],
    *,
    namespace: str,
    job_name: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Require every declared input/job/output edge to exist in the graph."""
    graph = lineage.get("graph")
    if not isinstance(graph, list) or not graph:
        raise ValueError(f"Marquez job {job_name} has no lineage graph nodes")

    observed: set[tuple[str, str]] = set()
    for node in graph:
        if not isinstance(node, dict):
            continue
        for edge_group in (node.get("inEdges") or [], node.get("outEdges") or []):
            for edge in edge_group:
                if not isinstance(edge, dict):
                    continue
                origin = edge.get("origin")
                destination = edge.get("destination")
                if isinstance(origin, str) and isinstance(destination, str):
                    observed.add((origin, destination))

    job_id = f"job:{namespace}:{job_name}"
    expected = {
        *{(_dataset_node_id(item), job_id) for item in inputs},
        *{(job_id, _dataset_node_id(item)) for item in outputs},
    }
    missing = sorted(expected - observed)
    if missing:
        formatted = ", ".join(f"{origin} -> {destination}" for origin, destination in missing)
        raise ValueError(
            f"Marquez lineage graph for {job_name} is missing expected edge(s): "
            f"{formatted}"
        )
    return [
        {"origin": origin, "destination": destination}
        for origin, destination in sorted(expected)
    ]


def collect_marquez_evidence(
    *,
    endpoint: str,
    namespace: str = DEFAULT_NAMESPACE,
    job_name: str = DEFAULT_JOB,
    fetcher: Callable[[str], dict[str, Any]] = _fetch_json,
) -> dict[str, Any]:
    """Fetch job, run, and lineage responses and verify both dataset edges."""
    base = endpoint.rstrip("/")
    namespace_path = quote(namespace, safe="")
    job_path = quote(job_name, safe="")
    job_url = f"{base}/api/v1/namespaces/{namespace_path}/jobs/{job_path}"
    runs_url = f"{job_url}/runs?{urlencode({'limit': 20})}"
    node_id = f"job:{namespace}:{job_name}"
    lineage_url = f"{base}/api/v1/lineage?{urlencode({'nodeId': node_id, 'depth': 1})}"

    job = fetcher(job_url)
    runs = fetcher(runs_url)
    lineage = fetcher(lineage_url)
    inputs = list(job.get("inputs") or [])
    outputs = list(job.get("outputs") or [])
    if not inputs:
        raise ValueError(f"Marquez job {job_name} has no input dataset")
    if not outputs:
        raise ValueError(f"Marquez job {job_name} has no output dataset")
    verified_edges = validate_lineage_graph(
        lineage,
        namespace=namespace,
        job_name=job_name,
        inputs=inputs,
        outputs=outputs,
    )

    latest_run = job.get("latestRun") or {}
    return {
        "evidence_version": 1,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "marquez_endpoint": base,
        "subject": {
            "namespace": namespace,
            "job_name": job_name,
            "latest_run_state": latest_run.get("state"),
            "input_datasets": _dataset_names(inputs),
            "output_datasets": _dataset_names(outputs),
            "verified_edges": verified_edges,
        },
        "api_responses": {
            "job": job,
            "runs": runs,
            "lineage": lineage,
        },
    }


def write_marquez_evidence(payload: dict[str, Any], output: str | Path) -> Path:
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export one Marquez Bronze-to-Silver lineage evidence bundle."
    )
    parser.add_argument("--endpoint", default="http://localhost:5000")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--job", default=DEFAULT_JOB)
    parser.add_argument(
        "--output",
        default="reports/lineage/sensor_telemetry_marquez.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    payload = collect_marquez_evidence(
        endpoint=args.endpoint,
        namespace=args.namespace,
        job_name=args.job,
    )
    target = write_marquez_evidence(payload, args.output)
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
