"""OpenLineage event construction, reliable emission, and local spooling."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde

from data_governance.config import LINEAGE_SPOOL_PATH


LOGGER = logging.getLogger(__name__)
PRODUCER = "https://github.com/smart-dairy-factory/data-governance"
JOB_NAMESPACE = "smart-dairy-factory"
SILVER_NAMESPACE = "s3://smart-dairy-lakehouse"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _bronze_dataset(bronze_path: str | Path, bronze_root: str | Path) -> Dataset:
    path_text = str(bronze_path).replace("\\", "/")
    root_text = str(bronze_root).replace("\\", "/").rstrip("/")
    if path_text.startswith("s3://"):
        marker = "/01_bronze_vault/"
        namespace, separator, relative = path_text.partition(marker)
        if separator:
            return Dataset(f"{namespace}/01_bronze_vault", relative)
        return Dataset(path_text.rsplit("/", 1)[0], path_text.rsplit("/", 1)[-1])

    try:
        relative = Path(bronze_path).resolve().relative_to(Path(bronze_root).resolve())
        name = PurePosixPath(relative.as_posix()).as_posix()
    except (OSError, ValueError):
        name = PurePosixPath(path_text).name
    return Dataset("file://bronze", name)


def build_run_event(
    event_type: str | RunState,
    *,
    run_id: str,
    table_name: str,
    bronze_path: str | Path,
    bronze_root: str | Path,
    event_time: str | None = None,
) -> RunEvent:
    """Build one event matching the project's Bronze-to-Silver contract."""
    state = event_type if isinstance(event_type, RunState) else RunState(event_type.upper())
    return RunEvent(
        eventType=state,
        eventTime=event_time or _utc_now(),
        run=Run(runId=run_id),
        job=Job(namespace=JOB_NAMESPACE, name=f"bronze_to_silver.{table_name}"),
        producer=PRODUCER,
        inputs=[_bronze_dataset(bronze_path, bronze_root)],
        outputs=[Dataset(SILVER_NAMESPACE, f"02_silver_curated/{table_name}")],
    )


def event_to_dict(event: RunEvent) -> dict[str, Any]:
    """Serialize through the official client so spool files are API-ready JSON."""
    return json.loads(Serde.to_json(event))


def event_from_dict(payload: Mapping[str, Any]) -> RunEvent:
    """Rehydrate the stable core fields of a spooled RunEvent."""
    return RunEvent(
        eventType=RunState(str(payload["eventType"])),
        eventTime=str(payload["eventTime"]),
        run=Run(runId=str(payload["run"]["runId"])),
        job=Job(
            namespace=str(payload["job"]["namespace"]),
            name=str(payload["job"]["name"]),
        ),
        producer=str(payload["producer"]),
        inputs=[
            Dataset(namespace=str(item["namespace"]), name=str(item["name"]))
            for item in payload.get("inputs", [])
        ],
        outputs=[
            Dataset(namespace=str(item["namespace"]), name=str(item["name"]))
            for item in payload.get("outputs", [])
        ],
    )


class LineageEmitter:
    """Emit lineage without letting a temporarily unavailable API lose events."""

    def __init__(
        self,
        endpoint: str = "http://localhost:5000",
        *,
        spool_path: str | Path = LINEAGE_SPOOL_PATH,
        emit_func: Callable[[RunEvent], Any] | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.spool_path = Path(spool_path)
        self._emit_func = emit_func

    def _emit(self, event: RunEvent) -> None:
        if self._emit_func is not None:
            self._emit_func(event)
            return
        client = OpenLineageClient(url=self.endpoint)
        try:
            client.emit(event)
        finally:
            client.close(timeout=10)

    def _spool_file(self, event: RunEvent) -> Path:
        payload = event_to_dict(event)
        event_type = str(payload["eventType"]).lower()
        event_time = str(payload["eventTime"]).replace(":", "-")
        run_id = str(payload["run"]["runId"])
        return self.spool_path / run_id / f"{event_time}_{event_type}.json"

    def spool(self, event: RunEvent) -> Path:
        target = self._spool_file(event)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(event_to_dict(event), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, target)
        return target

    def emit(self, event: RunEvent) -> str:
        """Return EMITTED or SPOOLED; never alter the pipeline quality status."""
        try:
            self._emit(event)
            return "EMITTED"
        except Exception as exc:  # transport implementations raise several types
            target = self.spool(event)
            LOGGER.warning("OpenLineage unavailable; event spooled at %s: %s", target, exc)
            return "SPOOLED"

    def replay(self) -> dict[str, int]:
        """Replay in stable order and delete a spool file only after successful emit."""
        result = {"emitted": 0, "failed": 0, "remaining": 0}
        if not self.spool_path.exists():
            return result
        for path in sorted(self.spool_path.rglob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                event = event_from_dict(payload)
                self._emit(event)
            except Exception as exc:
                result["failed"] += 1
                LOGGER.warning("Could not replay %s: %s", path, exc)
            else:
                path.unlink()
                result["emitted"] += 1
        for directory in sorted(self.spool_path.glob("*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
        result["remaining"] = sum(1 for _ in self.spool_path.rglob("*.json"))
        return result


def replay_spool(
    endpoint: str = "http://localhost:5000",
    *,
    spool_path: str | Path = LINEAGE_SPOOL_PATH,
) -> Mapping[str, int]:
    """Convenience entry point used by the CLI."""
    return LineageEmitter(endpoint, spool_path=spool_path).replay()
